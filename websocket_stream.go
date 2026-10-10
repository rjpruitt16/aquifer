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
	CheckCursor(ctx context.Context, sessionID, after string) error
	Ping(ctx context.Context) error
	Close() error
}

type RedisWebSocketStreamStore struct {
	client    *redis.Client // appends, ranges and replay reads
	readers   *redis.Client // live sessions' blocking reads
	prefix    string
	maxEvents int64
	ttl       time.Duration
}

// NewRedisWebSocketStreamStore connects the transcript store. liveReaders is
// how many sessions can follow their streams at once (the client-connection
// ceiling): each live session holds one connection in a blocking XREAD, so
// those reads get their own pool of that size. Sharing one default-sized
// pool (10 per CPU) with appends made every append wait for some session's
// blocking read to time out once sessions outnumbered the pool: command
// confirmations took the full read-block interval (1s) with 200 sessions.
func NewRedisWebSocketStreamStore(rawURL, prefix string, maxEvents int64, ttl time.Duration, liveReaders int) (*RedisWebSocketStreamStore, error) {
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
	readerOpts := *opts
	readerOpts.PoolSize = max(liveReaders, 1) + 8
	// Sized for many sessions, not throughput: a blocking XREAD is a tiny
	// command and its replies are small batches (larger ones still stream
	// through), so small buffers save ~50KB per session over go-redis's 32KB
	// defaults. Connections a burst of sessions opened are closed once idle
	// instead of being kept for go-redis's default 30 minutes.
	readerOpts.ReadBufferSize = 8 * 1024
	readerOpts.WriteBufferSize = 4 * 1024
	readerOpts.MaxIdleConns = 64
	readerOpts.ConnMaxIdleTime = time.Minute
	return &RedisWebSocketStreamStore{
		client:    redis.NewClient(opts),
		readers:   redis.NewClient(&readerOpts),
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
	client := s.client
	if block >= 0 {
		client = s.readers // blocking reads must not hold connections appends need
	}
	streams, err := client.XRead(ctx, &redis.XReadArgs{
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

func (s *RedisWebSocketStreamStore) Close() error {
	return errors.Join(s.client.Close(), s.readers.Close())
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
