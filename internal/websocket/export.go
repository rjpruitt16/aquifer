// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/nodeid"
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
	defaultWebSocketExportBatchBytes = 4 << 20 // under Lambda's 6 MB request limit
	defaultWebSocketExportMaxPending = 1_000_000
	webSocketExportGroup             = "aquifer-export"
	webSocketExportClaimIdle         = time.Minute
	webSocketExportRequestTimeout    = 10 * time.Second
	webSocketExportMaxBackoff        = 30 * time.Second
	webSocketExportSchema            = "aquifer.websocket_batch.v1"
	// Consumers that left the group (a node that was replaced or renamed)
	// are removed once they have nothing pending and have been idle this
	// long, so the group's consumer list doesn't grow with every deploy.
	webSocketExportStaleConsumer = time.Hour
	webSocketExportSweepEvery    = 10 * time.Minute
	webSocketExportGrowAfter     = 10
)

type ExportConfig struct {
	Enabled bool
	URL     string
	// Token is sent as "Authorization: Bearer <token>" when set.
	Token     string
	BatchSize int64
	// MaxBatchBytes caps one request's encoded size; a single message
	// larger than this is still sent alone.
	MaxBatchBytes int
	// MaxPending caps the export stream (approximately). While the receiver is
	// unreachable, messages past the cap are dropped from export; they stay in
	// their session transcripts for the replay window.
	MaxPending      int64
	IncludePayloads bool
}

// LoadExportConfig reads AQUIFER_WS_EXPORT_*. Export is off unless
// AQUIFER_WS_EXPORT_ENABLED=true, and stays off with a warning when it is
// enabled without a URL.
func LoadExportConfig() ExportConfig {
	cfg := ExportConfig{
		Enabled:         env.Bool("AQUIFER_WS_EXPORT_ENABLED", false),
		URL:             strings.TrimSpace(os.Getenv("AQUIFER_WS_EXPORT_URL")),
		Token:           os.Getenv("AQUIFER_WS_EXPORT_TOKEN"),
		BatchSize:       positiveEnvInt64("AQUIFER_WS_EXPORT_BATCH", defaultWebSocketExportBatch),
		MaxBatchBytes:   int(positiveEnvInt64("AQUIFER_WS_EXPORT_MAX_BATCH_BYTES", defaultWebSocketExportBatchBytes)),
		MaxPending:      positiveEnvInt64("AQUIFER_WS_EXPORT_MAX_PENDING", defaultWebSocketExportMaxPending),
		IncludePayloads: env.Bool("AQUIFER_WS_EXPORT_PAYLOADS", true),
	}
	if cfg.Enabled && cfg.URL == "" {
		log.Printf("websocket export: AQUIFER_WS_EXPORT_ENABLED is set but AQUIFER_WS_EXPORT_URL is empty; export stays off")
		cfg.Enabled = false
	}
	if cfg.Enabled {
		if err := checkWebSocketExportURL(cfg.URL, env.Bool("AQUIFER_WS_EXPORT_ALLOW_INSECURE", false)); err != nil {
			log.Printf("websocket export: %v; export stays off", err)
			cfg.Enabled = false
		}
	}
	return cfg
}

// checkWebSocketExportURL requires https, so the bearer token and the
// transcripts aren't sent in cleartext. http is allowed for loopback, or
// anywhere when AQUIFER_WS_EXPORT_ALLOW_INSECURE=true (a private network
// you trust).
func checkWebSocketExportURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("AQUIFER_WS_EXPORT_URL %q isn't an absolute URL", raw)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if allowInsecure || host == "localhost" {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("AQUIFER_WS_EXPORT_URL must use https (http is allowed for loopback, or with AQUIFER_WS_EXPORT_ALLOW_INSECURE=true)")
	default:
		return fmt.Errorf("AQUIFER_WS_EXPORT_URL must be an http(s) URL")
	}
}

// ExportEvent is one exported transcript message.
type ExportEvent struct {
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

// ExportBatch is the body POSTed to AQUIFER_WS_EXPORT_URL.
type ExportBatch struct {
	Schema string        `json:"schema"`
	NodeID string        `json:"node_id"`
	Events []ExportEvent `json:"events"`
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

func (s *RedisStreamStore) exportKey() string {
	return s.prefix + "export"
}

// EnableExport makes every Append also write to the export stream.
func (s *RedisStreamStore) EnableExport(maxPending int64) {
	s.exportMaxLen = maxPending
}

// Exporter moves the export stream to the receiver.
type Exporter struct {
	client   *redis.Client
	key      string
	consumer string
	cfg      ExportConfig
	http     *http.Client
	// limit is the batch size for the next read. A 413 halves it; it
	// doubles back toward BatchSize after webSocketExportGrowAfter
	// successful batches in a row, so a receiver with a smaller body limit
	// settles near its limit instead of getting a 413 on every batch.
	limit     int64
	successes int
	lastSweep time.Time

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// StartExport turns export on for store and starts the exporter.
func StartExport(store *RedisStreamStore, cfg ExportConfig) (*Exporter, error) {
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = defaultWebSocketExportBatchBytes
	}
	exporter := &Exporter{
		client:   store.client,
		key:      store.exportKey(),
		consumer: nodeid.Get(),
		cfg:      cfg,
		http: &http.Client{
			Timeout: webSocketExportRequestTimeout,
			// Never follow a redirect: it could send the token elsewhere, or
			// downgrade to http. A 3xx counts as a failed batch.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		limit: cfg.BatchSize,
		done:  make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	exporter.cancel = cancel
	if err := exporter.ensureGroup(ctx); err != nil {
		cancel()
		return nil, err
	}
	// Only now: without the group, Appends would fill the export stream
	// with nothing reading it.
	store.EnableExport(cfg.MaxPending)
	go exporter.run(ctx)
	log.Printf("websocket export: sending transcripts to %s as %s", cfg.URL, exporter.consumer)
	return exporter, nil
}

// Stop ends the exporter. Messages not yet acknowledged stay in the export
// stream and are sent after the next start. With nothing pending, this
// node's consumer is removed from the group.
func (e *Exporter) Stop() {
	if e == nil {
		return
	}
	e.once.Do(func() {
		e.cancel()
		<-e.done
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if pending, err := e.consumerPending(ctx, e.consumer); err == nil && pending == 0 {
			e.client.XGroupDelConsumer(ctx, e.key, webSocketExportGroup, e.consumer)
		}
	})
}

func (e *Exporter) consumerPending(ctx context.Context, name string) (int64, error) {
	consumers, err := e.client.XInfoConsumers(ctx, e.key, webSocketExportGroup).Result()
	if err != nil {
		return 0, err
	}
	for _, consumer := range consumers {
		if consumer.Name == name {
			return consumer.Pending, nil
		}
	}
	return 0, nil
}

// sweepConsumers removes other nodes' consumers that have nothing pending
// and have been idle for an hour. Consumers with pending messages are kept:
// claimStale moves those messages to a live node first.
func (e *Exporter) sweepConsumers(ctx context.Context) {
	consumers, err := e.client.XInfoConsumers(ctx, e.key, webSocketExportGroup).Result()
	if err != nil {
		return
	}
	for _, consumer := range consumers {
		if consumer.Name != e.consumer && consumer.Pending == 0 && consumer.Idle > webSocketExportStaleConsumer {
			e.client.XGroupDelConsumer(ctx, e.key, webSocketExportGroup, consumer.Name)
		}
	}
}

func (e *Exporter) ensureGroup(ctx context.Context) error {
	// "0": a new group starts with whatever is already in the stream.
	err := e.client.XGroupCreateMkStream(ctx, e.key, webSocketExportGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("websocket export: create consumer group: %w", err)
	}
	return nil
}

func (e *Exporter) run(ctx context.Context) {
	defer close(e.done)
	backoff := time.Second
	for ctx.Err() == nil {
		if time.Since(e.lastSweep) > webSocketExportSweepEvery {
			e.sweepConsumers(ctx)
			e.lastSweep = time.Now()
		}
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
func (e *Exporter) exportOnce(ctx context.Context) (int, error) {
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

	// Fill the batch up to MaxBatchBytes. Messages that don't fit stay
	// pending and lead the next batch. Entries that can't be decoded are
	// acknowledged with the batch and dropped.
	batch := ExportBatch{Schema: webSocketExportSchema, NodeID: e.consumer}
	ids := make([]string, 0, len(messages))
	size := 0
	for _, message := range messages {
		event, ok := e.event(message)
		if !ok {
			ids = append(ids, message.ID)
			continue
		}
		encoded, _ := json.Marshal(event)
		if len(batch.Events) > 0 && size+len(encoded) > e.cfg.MaxBatchBytes {
			break
		}
		batch.Events = append(batch.Events, event)
		ids = append(ids, message.ID)
		size += len(encoded)
	}
	if len(batch.Events) > 0 {
		err := e.post(ctx, batch)
		var tooLarge errBatchTooLarge
		switch {
		case errors.As(err, &tooLarge) && len(batch.Events) > 1:
			// The receiver can't take this much at once: halve and retry
			// right away (no backoff; the receiver answered fine).
			e.limit = max(1, int64(len(batch.Events)/2))
			e.successes = 0
			return 0, nil
		case errors.As(err, &tooLarge):
			// One message the receiver will never accept. Dropping it is
			// better than blocking every message behind it; it stays in its
			// session transcript for the replay window.
			log.Printf("websocket export: receiver refused one message as too large (session %s, stream %s); skipping it",
				batch.Events[0].SessionID, batch.Events[0].StreamID)
		case err != nil:
			return 0, err
		}
	}
	pipe := e.client.TxPipeline()
	pipe.XAck(ctx, e.key, webSocketExportGroup, ids...)
	pipe.XDel(ctx, e.key, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("acknowledge exported batch: %w", err)
	}
	if e.limit < e.cfg.BatchSize {
		e.successes++
		if e.successes >= webSocketExportGrowAfter {
			e.limit = min(e.cfg.BatchSize, e.limit*2)
			e.successes = 0
		}
	}
	return len(batch.Events), nil
}

type errBatchTooLarge struct{ events int }

func (err errBatchTooLarge) Error() string {
	return fmt.Sprintf("receiver answered 413 to a batch of %d", err.events)
}

func (e *Exporter) readGroup(ctx context.Context, id string, block time.Duration) ([]redis.XMessage, error) {
	args := &redis.XReadGroupArgs{
		Group:    webSocketExportGroup,
		Consumer: e.consumer,
		Streams:  []string{e.key, id},
		Count:    e.limit,
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

func (e *Exporter) claimStale(ctx context.Context) ([]redis.XMessage, error) {
	messages, _, err := e.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   e.key,
		Group:    webSocketExportGroup,
		Consumer: e.consumer,
		MinIdle:  webSocketExportClaimIdle,
		Start:    "0-0",
		Count:    e.limit,
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
func (e *Exporter) event(message redis.XMessage) (ExportEvent, bool) {
	field := func(name string) string {
		value, _ := message.Values[name].(string)
		return value
	}
	sessionID := field("session_id")
	if sessionID == "" {
		return ExportEvent{}, false
	}
	event := ExportEvent{
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

func (e *Exporter) post(ctx context.Context, batch ExportBatch) error {
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
	if response.StatusCode == http.StatusRequestEntityTooLarge {
		return errBatchTooLarge{events: len(batch.Events)}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("receiver answered %d to a batch of %d", response.StatusCode, len(batch.Events))
	}
	return nil
}
