// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// WebSocket transcript export.
//
// Session transcripts live in Valkey for a bounded replay window (see
// AQUIFER_WS_STREAM_TTL_SECONDS). Export copies every recorded message to a
// receiver you run, such as Inlet, which can keep it for as long as you
// like, for example in a Kafka topic.
//
// Each Append also adds the message to one shared export stream, in the
// same MULTI as the transcript write, so a recorded message is never missed.
// The exporter reads that stream through a consumer group shared by every
// Aquifer node, POSTs batches to AQUIFER_WS_EXPORT_URL, and acknowledges and
// deletes each batch only after a 2xx. Delivery is at least once: a receiver
// can drop repeats by (session_id, stream_id).

const (
	defaultWebSocketExportBatch      = 500
	defaultWebSocketExportMaxPending = 1_000_000
	webSocketExportGroup             = "aquifer-export"
	webSocketExportClaimIdle         = time.Minute
	webSocketExportRequestTimeout    = 10 * time.Second
	webSocketExportMaxBackoff        = 30 * time.Second
	webSocketExportSchema            = "aquifer.websocket_batch.v1"
)

type WebSocketExportConfig struct {
	Enabled bool
	URL     string
	// Token is sent as "Authorization: Bearer <token>" when set.
	Token     string
	BatchSize int64
	// MaxPending caps the export stream (approximately). While the receiver is
	// unreachable, messages past the cap are dropped from export; they stay in
	// their session transcripts for the replay window.
	MaxPending      int64
	IncludePayloads bool
}

// LoadWebSocketExportConfig reads AQUIFER_WS_EXPORT_*. Export is off unless
// AQUIFER_WS_EXPORT_ENABLED=true, and stays off with a warning when it is
// enabled without a URL.
func LoadWebSocketExportConfig() WebSocketExportConfig {
	cfg := WebSocketExportConfig{
		Enabled:         envBool("AQUIFER_WS_EXPORT_ENABLED", false),
		URL:             strings.TrimSpace(os.Getenv("AQUIFER_WS_EXPORT_URL")),
		Token:           os.Getenv("AQUIFER_WS_EXPORT_TOKEN"),
		BatchSize:       positiveEnvInt64("AQUIFER_WS_EXPORT_BATCH", defaultWebSocketExportBatch),
		MaxPending:      positiveEnvInt64("AQUIFER_WS_EXPORT_MAX_PENDING", defaultWebSocketExportMaxPending),
		IncludePayloads: envBool("AQUIFER_WS_EXPORT_PAYLOADS", true),
	}
	if cfg.Enabled && cfg.URL == "" {
		log.Printf("websocket export: AQUIFER_WS_EXPORT_ENABLED is set but AQUIFER_WS_EXPORT_URL is empty; export stays off")
		cfg.Enabled = false
	}
	return cfg
}

// WebSocketExportEvent is one exported transcript message.
type WebSocketExportEvent struct {
	SessionID  string          `json:"session_id"`
	StreamID   string          `json:"stream_id"`
	Direction  string          `json:"direction"`
	Type       string          `json:"type"`
	MessageID  string          `json:"message_id,omitempty"`
	CausedBy   string          `json:"caused_by,omitempty"`
	Generation int64           `json:"generation,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	RecordedAt int64           `json:"recorded_at"`
}

// WebSocketExportBatch is the body POSTed to AQUIFER_WS_EXPORT_URL.
type WebSocketExportBatch struct {
	Schema string                 `json:"schema"`
	NodeID string                 `json:"node_id"`
	Events []WebSocketExportEvent `json:"events"`
}

// webSocketExportAppendScript adds a transcript entry, refreshes the
// transcript's TTL, and adds the export copy with the entry's stream ID.
// KEYS: transcript, export stream. ARGV: transcript max length, TTL seconds,
// export max length, session ID, then the entry's field/value pairs.
var webSocketExportAppendScript = redis.NewScript(`
local fields = {}
for i = 5, #ARGV do fields[#fields + 1] = ARGV[i] end
local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(fields))
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*', 'session_id', ARGV[4], 'stream_id', id, unpack(fields))
return id
`)

func (s *RedisWebSocketStreamStore) exportKey() string {
	return s.prefix + "export"
}

// EnableExport makes every Append also write to the export stream.
func (s *RedisWebSocketStreamStore) EnableExport(maxPending int64) {
	s.exportMaxLen = maxPending
}

// WebSocketExporter moves the export stream to the receiver.
type WebSocketExporter struct {
	client   *redis.Client
	key      string
	consumer string
	cfg      WebSocketExportConfig
	http     *http.Client

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// StartWebSocketExport turns export on for store and starts the exporter.
func StartWebSocketExport(store *RedisWebSocketStreamStore, cfg WebSocketExportConfig) (*WebSocketExporter, error) {
	store.EnableExport(cfg.MaxPending)
	exporter := &WebSocketExporter{
		client:   store.client,
		key:      store.exportKey(),
		consumer: NodeID(),
		cfg:      cfg,
		http:     &http.Client{Timeout: webSocketExportRequestTimeout},
		done:     make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	exporter.cancel = cancel
	if err := exporter.ensureGroup(ctx); err != nil {
		cancel()
		return nil, err
	}
	go exporter.run(ctx)
	log.Printf("websocket export: sending transcripts to %s as %s", cfg.URL, exporter.consumer)
	return exporter, nil
}

// Stop ends the exporter. Messages not yet acknowledged stay in the export
// stream and are sent after the next start.
func (e *WebSocketExporter) Stop() {
	if e == nil {
		return
	}
	e.once.Do(func() {
		e.cancel()
		<-e.done
	})
}

func (e *WebSocketExporter) ensureGroup(ctx context.Context) error {
	// "0": a new group starts with whatever is already in the stream.
	err := e.client.XGroupCreateMkStream(ctx, e.key, webSocketExportGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("websocket export: create consumer group: %w", err)
	}
	return nil
}

func (e *WebSocketExporter) run(ctx context.Context) {
	defer close(e.done)
	backoff := time.Second
	for ctx.Err() == nil {
		// exportOnce waits up to a second for new messages, so an idle
		// exporter isn't a busy loop.
		if _, err := e.exportOnce(ctx); err == nil {
			backoff = time.Second
			continue
		} else if ctx.Err() == nil {
			log.Printf("websocket export: %v; retrying in %s", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, webSocketExportMaxBackoff)
	}
}

// exportOnce sends one batch: this node's unacknowledged messages first,
// then messages another node left unacknowledged for over a minute, then
// new messages (waiting up to a second for some). It returns how many it
// sent.
func (e *WebSocketExporter) exportOnce(ctx context.Context) (int, error) {
	messages, err := e.readGroup(ctx, "0", 0)
	if err != nil {
		return 0, err
	}
	if len(messages) == 0 {
		messages, err = e.claimStale(ctx)
		if err != nil {
			return 0, err
		}
	}
	if len(messages) == 0 {
		messages, err = e.readGroup(ctx, ">", time.Second)
		if err != nil {
			return 0, err
		}
	}
	if len(messages) == 0 {
		return 0, nil
	}

	batch := WebSocketExportBatch{Schema: webSocketExportSchema, NodeID: e.consumer}
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
		event, ok := e.event(message)
		if ok {
			batch.Events = append(batch.Events, event)
		}
	}
	if len(batch.Events) > 0 {
		if err := e.post(ctx, batch); err != nil {
			return 0, err
		}
	}
	pipe := e.client.TxPipeline()
	pipe.XAck(ctx, e.key, webSocketExportGroup, ids...)
	pipe.XDel(ctx, e.key, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("acknowledge exported batch: %w", err)
	}
	return len(batch.Events), nil
}

func (e *WebSocketExporter) readGroup(ctx context.Context, id string, block time.Duration) ([]redis.XMessage, error) {
	args := &redis.XReadGroupArgs{
		Group:    webSocketExportGroup,
		Consumer: e.consumer,
		Streams:  []string{e.key, id},
		Count:    e.cfg.BatchSize,
		Block:    block,
	}
	if block == 0 {
		args.Block = -1 // don't block when reading this node's pending messages
	}
	streams, err := e.client.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		if strings.Contains(err.Error(), "NOGROUP") {
			// The stream was deleted (for example by a flush); start over.
			if groupErr := e.ensureGroup(ctx); groupErr != nil {
				return nil, groupErr
			}
			return nil, nil
		}
		return nil, fmt.Errorf("read export stream: %w", err)
	}
	var messages []redis.XMessage
	for _, stream := range streams {
		messages = append(messages, stream.Messages...)
	}
	return messages, nil
}

func (e *WebSocketExporter) claimStale(ctx context.Context) ([]redis.XMessage, error) {
	messages, _, err := e.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   e.key,
		Group:    webSocketExportGroup,
		Consumer: e.consumer,
		MinIdle:  webSocketExportClaimIdle,
		Start:    "0-0",
		Count:    e.cfg.BatchSize,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim stale export messages: %w", err)
	}
	return messages, nil
}

// event decodes one export stream entry. Entries a newer or older version
// wrote in another shape are skipped (and still acknowledged).
func (e *WebSocketExporter) event(message redis.XMessage) (WebSocketExportEvent, bool) {
	field := func(name string) string {
		value, _ := message.Values[name].(string)
		return value
	}
	sessionID := field("session_id")
	if sessionID == "" {
		return WebSocketExportEvent{}, false
	}
	event := WebSocketExportEvent{
		SessionID: sessionID,
		StreamID:  field("stream_id"),
		Direction: field("direction"),
		Type:      field("type"),
		MessageID: field("message_id"),
		CausedBy:  field("caused_by"),
	}
	event.Generation, _ = strconv.ParseInt(field("generation"), 10, 64)
	event.RecordedAt, _ = strconv.ParseInt(field("recorded_at"), 10, 64)
	if payload := field("payload"); e.cfg.IncludePayloads && payload != "" {
		if json.Valid([]byte(payload)) {
			event.Payload = json.RawMessage(payload)
		} else {
			encoded, _ := json.Marshal(payload)
			event.Payload = encoded
		}
	}
	return event, true
}

func (e *WebSocketExporter) post(ctx context.Context, batch WebSocketExportBatch) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if e.cfg.Token != "" {
		request.Header.Set("Authorization", "Bearer "+e.cfg.Token)
	}
	response, err := e.http.Do(request)
	if err != nil {
		return fmt.Errorf("send batch of %d: %w", len(batch.Events), err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("receiver answered %d to a batch of %d", response.StatusCode, len(batch.Events))
	}
	return nil
}
