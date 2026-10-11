package aquifer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

const (
	defaultWebSocketStreamPrefix    = "aqueduct:ws:"
	defaultWebSocketStreamMaxEvents = 10000
	defaultWebSocketReadBatch       = 100
)

type WebSocketStreamEvent struct {
	StreamID   string
	Direction  string
	Envelope   WebSocketEnvelope
	RecordedAt int64
}

type WebSocketStreamStore interface {
	Append(ctx context.Context, sessionID, direction string, message WebSocketEnvelope) (string, error)
	ReadAfter(ctx context.Context, sessionID, after string, count int64, block time.Duration) ([]WebSocketStreamEvent, error)
	// Follow signals when sessionID's transcript may have entries after
	// cursor, so a live session reads only when there is something to read.
	Follow(ctx context.Context, sessionID, cursor string) (WebSocketFollow, error)
	CheckCursor(ctx context.Context, sessionID, after string) error
	Ping(ctx context.Context) error
	Close() error
}

type RedisWebSocketStreamStore struct {
	client    *redis.Client
	follower  *streamFollower
	prefix    string
	maxEvents int64
	ttl       time.Duration
}

// NewRedisWebSocketStreamStore connects the transcript store. Live sessions
// learn about new entries through readerShards shared pub/sub connections
// (see streamFollower), so Valkey connections don't grow with sessions.
func NewRedisWebSocketStreamStore(rawURL, prefix string, maxEvents int64, ttl time.Duration, readerShards int) (*RedisWebSocketStreamStore, error) {
	if rawURL == "" {
		return nil, errors.New("AQUIFER_VALKEY_URL is required when WebSockets are enabled")
	}
	parsed, err := normalizeWebSocketRedisURL(rawURL)
	if err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(parsed)
	if err != nil {
		return nil, fmt.Errorf("parse websocket Redis URL: %w", err)
	}
	if prefix == "" {
		prefix = defaultWebSocketStreamPrefix
	}
	if maxEvents <= 0 {
		maxEvents = defaultWebSocketStreamMaxEvents
	}
	if ttl <= 0 {
		ttl = defaultWebSocketStreamTTL
	}
	// Valkey doesn't support go-redis's maintenance-notification handshake,
	// and in auto mode go-redis rewrites shared options when it fails, which
	// races when the follower opens its dedicated connections concurrently.
	opts.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	client := redis.NewClient(opts)
	return &RedisWebSocketStreamStore{
		client:    client,
		follower:  newStreamFollower(client, readerShards),
		prefix:    prefix,
		maxEvents: maxEvents,
		ttl:       ttl,
	}, nil
}

func normalizeWebSocketRedisURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse websocket Redis URL: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "redis", "rediss":
	case "valkey":
		u.Scheme = "redis"
	case "valkeys":
		u.Scheme = "rediss"
	default:
		return "", fmt.Errorf("unsupported websocket Redis URL scheme %q", u.Scheme)
	}
	return u.String(), nil
}

func (s *RedisWebSocketStreamStore) Append(ctx context.Context, sessionID, direction string, message WebSocketEnvelope) (string, error) {
	if sessionID == "" {
		return "", errors.New("websocket session_id is required")
	}
	if direction != "client" && direction != "backend" {
		return "", fmt.Errorf("invalid websocket event direction %q", direction)
	}
	payload := ""
	if len(message.Payload) > 0 {
		payload = string(message.Payload)
	}
	values := []any{
		"direction", direction,
		"type", message.Type,
		"message_id", message.MessageID,
		"caused_by", message.CausedBy,
		"payload", payload,
		"generation", strconv.FormatInt(message.Generation, 10),
		"recorded_at", strconv.FormatInt(time.Now().UnixMilli(), 10),
	}
	pipe := s.client.TxPipeline()
	add := pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: s.streamKey(sessionID),
		MaxLen: s.maxEvents,
		Approx: true,
		Values: values,
	})
	pipe.Expire(ctx, s.streamKey(sessionID), s.ttl)
	if direction == "backend" {
		// Sessions deliver only backend entries; client entries are read
		// past on the next wake, so they don't need one of their own.
		pipe.Publish(ctx, s.notifyChannel(sessionID), "")
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("append websocket event: %w", err)
	}
	id, err := add.Result()
	if err != nil {
		return "", fmt.Errorf("read appended websocket event id: %w", err)
	}
	return id, nil
}

func (s *RedisWebSocketStreamStore) ReadAfter(ctx context.Context, sessionID, after string, count int64, block time.Duration) ([]WebSocketStreamEvent, error) {
	if count <= 0 {
		count = defaultWebSocketReadBatch
	}
	if after == "" {
		after = "0-0"
	}
	streams, err := s.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{s.streamKey(sessionID), after},
		Count:   count,
		Block:   block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read websocket events: %w", err)
	}

	var events []WebSocketStreamEvent
	for _, stream := range streams {
		for _, raw := range stream.Messages {
			event, err := decodeWebSocketStreamEvent(raw)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}
	return events, nil
}

func (s *RedisWebSocketStreamStore) CheckCursor(ctx context.Context, sessionID, after string) error {
	if after == "" || after == "0-0" {
		return nil
	}
	oldest, err := s.client.XRangeN(ctx, s.streamKey(sessionID), "-", "+", 1).Result()
	if err != nil {
		return fmt.Errorf("read websocket stream cursor: %w", err)
	}
	if len(oldest) == 0 {
		return ErrWebSocketReplayGap
	}
	cmp, err := compareRedisStreamIDs(after, oldest[0].ID)
	if err != nil {
		return fmt.Errorf("invalid websocket replay cursor %q: %w", after, err)
	}
	if cmp < 0 {
		return ErrWebSocketReplayGap
	}
	return nil
}

func (s *RedisWebSocketStreamStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *RedisWebSocketStreamStore) Follow(ctx context.Context, sessionID, _ string) (WebSocketFollow, error) {
	if sessionID == "" {
		return nil, errors.New("websocket session_id is required")
	}
	return s.follower.follow(ctx, s.notifyChannel(sessionID))
}

func (s *RedisWebSocketStreamStore) notifyChannel(sessionID string) string {
	return s.streamKey(sessionID) + ":notify"
}

func (s *RedisWebSocketStreamStore) Close() error {
	s.follower.close()
	return s.client.Close()
}

func (s *RedisWebSocketStreamStore) streamKey(sessionID string) string {
	return s.prefix + hashKey(sessionID)
}

func decodeWebSocketStreamEvent(raw redis.XMessage) (WebSocketStreamEvent, error) {
	value := func(name string) string {
		v, ok := raw.Values[name]
		if !ok || v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}

	generation, err := strconv.ParseInt(defaultString(value("generation"), "0"), 10, 64)
	if err != nil {
		return WebSocketStreamEvent{}, fmt.Errorf("decode websocket event %s generation: %w", raw.ID, err)
	}
	recordedAt, err := strconv.ParseInt(defaultString(value("recorded_at"), "0"), 10, 64)
	if err != nil {
		return WebSocketStreamEvent{}, fmt.Errorf("decode websocket event %s recorded_at: %w", raw.ID, err)
	}

	event := WebSocketStreamEvent{
		StreamID:   raw.ID,
		Direction:  value("direction"),
		RecordedAt: recordedAt,
		Envelope: WebSocketEnvelope{
			Type:       value("type"),
			MessageID:  value("message_id"),
			CausedBy:   value("caused_by"),
			Generation: generation,
		},
	}
	if payload := value("payload"); payload != "" {
		if !json.Valid([]byte(payload)) {
			return WebSocketStreamEvent{}, fmt.Errorf("decode websocket event %s payload: invalid json", raw.ID)
		}
		event.Envelope.Payload = json.RawMessage(payload)
	}
	return event, nil
}

func compareRedisStreamIDs(left, right string) (int, error) {
	leftMS, leftSequence, err := parseRedisStreamID(left)
	if err != nil {
		return 0, err
	}
	rightMS, rightSequence, err := parseRedisStreamID(right)
	if err != nil {
		return 0, err
	}
	if leftMS < rightMS || leftMS == rightMS && leftSequence < rightSequence {
		return -1, nil
	}
	if leftMS == rightMS && leftSequence == rightSequence {
		return 0, nil
	}
	return 1, nil
}

func parseRedisStreamID(id string) (int64, int64, error) {
	ms, sequence, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, errors.New("expected milliseconds-sequence")
	}
	parsedMS, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	parsedSequence, err := strconv.ParseInt(sequence, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return parsedMS, parsedSequence, nil
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
