// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebSocketExportConfigIsOffByDefault(t *testing.T) {
	t.Setenv("AQUIFER_WS_EXPORT_ENABLED", "")
	t.Setenv("AQUIFER_WS_EXPORT_URL", "https://receiver.example/batches")
	if LoadWebSocketExportConfig().Enabled {
		t.Fatal("export must be off unless AQUIFER_WS_EXPORT_ENABLED=true")
	}

	t.Setenv("AQUIFER_WS_EXPORT_ENABLED", "true")
	t.Setenv("AQUIFER_WS_EXPORT_URL", "")
	if LoadWebSocketExportConfig().Enabled {
		t.Fatal("export without a URL must stay off")
	}

	t.Setenv("AQUIFER_WS_EXPORT_URL", "https://receiver.example/batches")
	cfg := LoadWebSocketExportConfig()
	if !cfg.Enabled || cfg.BatchSize != defaultWebSocketExportBatch || !cfg.IncludePayloads {
		t.Fatalf("unexpected config %+v", cfg)
	}
}

// exportReceiver records batches, and answers 503 to the first failFirst
// requests so the test can check retries.
type exportReceiver struct {
	srv       *httptest.Server
	mu        sync.Mutex
	events    []WebSocketExportEvent
	requests  atomic.Int64
	failFirst int64
	auth      string
}

func newExportReceiver(t *testing.T, failFirst int64) *exportReceiver {
	t.Helper()
	r := &exportReceiver{failFirst: failFirst}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := r.requests.Add(1)
		r.mu.Lock()
		r.auth = req.Header.Get("Authorization")
		r.mu.Unlock()
		if n <= r.failFirst {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch WebSocketExportBatch
		if err := json.NewDecoder(req.Body).Decode(&batch); err != nil || batch.Schema != webSocketExportSchema {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.events = append(r.events, batch.Events...)
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *exportReceiver) received() []WebSocketExportEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]WebSocketExportEvent(nil), r.events...)
}

func exportTestStore(t *testing.T) *RedisWebSocketStreamStore {
	t.Helper()
	rawURL := os.Getenv("AQUIFER_TEST_VALKEY_URL")
	if rawURL == "" {
		t.Skip("set AQUIFER_TEST_VALKEY_URL to run the real Valkey integration test")
	}
	prefix := fmt.Sprintf("aquifer-test:%d:", time.Now().UnixNano())
	store, err := NewRedisWebSocketStreamStore(rawURL, prefix, 100, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		keys, _ := store.client.Keys(ctx, prefix+"*").Result()
		if len(keys) > 0 {
			store.client.Del(ctx, keys...)
		}
		store.Close()
	})
	return store
}

func waitForExport(t *testing.T, receiver *exportReceiver, want int) []WebSocketExportEvent {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if events := receiver.received(); len(events) >= want {
			return events
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("receiver got %d of %d events", len(receiver.received()), want)
	return nil
}

func TestWebSocketExportDeliversEveryMessageAndCleansUp(t *testing.T) {
	store := exportTestStore(t)
	receiver := newExportReceiver(t, 2) // the first two batches fail
	cfg := WebSocketExportConfig{Enabled: true, URL: receiver.srv.URL, Token: "s3cret", BatchSize: 2, MaxPending: 1000, IncludePayloads: true}
	exporter, err := StartWebSocketExport(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Stop()

	ctx := context.Background()
	ids := map[string]string{}
	for i, session := range []string{"session-a", "session-b", "session-a"} {
		message := WebSocketEnvelope{Type: "command", MessageID: fmt.Sprintf("m%d", i), Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))}
		id, err := store.Append(ctx, session, "client", message)
		if err != nil {
			t.Fatal(err)
		}
		ids[message.MessageID] = id
	}

	events := waitForExport(t, receiver, 3)
	for _, event := range events {
		if event.StreamID != ids[event.MessageID] {
			t.Fatalf("event %s exported with stream_id %q, the transcript entry is %q", event.MessageID, event.StreamID, ids[event.MessageID])
		}
		if event.Direction != "client" || event.Type != "command" || len(event.Payload) == 0 || event.RecordedAt == 0 {
			t.Fatalf("incomplete event %+v", event)
		}
	}
	if receiver.requests.Load() < 3 {
		t.Fatalf("expected the failed batches to be retried, got %d requests", receiver.requests.Load())
	}
	receiver.mu.Lock()
	auth := receiver.auth
	receiver.mu.Unlock()
	if auth != "Bearer s3cret" {
		t.Fatalf("expected the bearer token, got %q", auth)
	}

	// Acknowledged messages are deleted from the export stream, but the
	// session transcripts keep them for the replay window.
	if !waitUntil(15*time.Second, func() bool { return store.client.XLen(ctx, store.exportKey()).Val() == 0 }) {
		t.Fatal("acknowledged messages should be deleted from the export stream")
	}
	transcript, err := store.ReadAfter(ctx, "session-a", "", 10, 0)
	if err != nil || len(transcript) != 2 {
		t.Fatalf("expected session-a's transcript to keep 2 entries, got %d (%v)", len(transcript), err)
	}
}

func TestWebSocketExportResumesAfterRestart(t *testing.T) {
	store := exportTestStore(t)
	down := newExportReceiver(t, 1<<30) // never accepts
	cfg := WebSocketExportConfig{Enabled: true, URL: down.srv.URL, BatchSize: 10, MaxPending: 1000, IncludePayloads: false}
	exporter, err := StartWebSocketExport(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := store.Append(ctx, "session-r", "backend", WebSocketEnvelope{Type: "event", MessageID: fmt.Sprintf("r%d", i), Payload: json.RawMessage(`{"secret":true}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if !waitUntil(15*time.Second, func() bool { return down.requests.Load() > 0 }) {
		t.Fatal("the exporter never tried the receiver")
	}
	exporter.Stop()

	// A restarted exporter (same node) sends what the first one never got
	// acknowledged.
	up := newExportReceiver(t, 0)
	cfg.URL = up.srv.URL
	restarted, err := StartWebSocketExport(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	events := waitForExport(t, up, 3)
	for _, event := range events {
		if len(event.Payload) != 0 {
			t.Fatalf("AQUIFER_WS_EXPORT_PAYLOADS=false must leave payloads out, got %s", event.Payload)
		}
	}
}

func TestWebSocketAppendWithoutExportWritesNoExportStream(t *testing.T) {
	store := exportTestStore(t)
	ctx := context.Background()
	if _, err := store.Append(ctx, "session-x", "client", WebSocketEnvelope{Type: "command", MessageID: "x1"}); err != nil {
		t.Fatal(err)
	}
	if n := store.client.Exists(ctx, store.exportKey()).Val(); n != 0 {
		t.Fatal("export is off, so Append must not write the export stream")
	}
}

func TestWebSocketExportURLMustBeHTTPS(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://inlet.example/_inlet/websocket-events": true,
		"http://localhost:9000/batches":                 true,
		"http://127.0.0.1:9000/batches":                 true,
		"http://[::1]:9000/batches":                     true,
		"http://inlet.example/batches":                  false,
		"ftp://inlet.example/batches":                   false,
		"/relative":                                     false,
	} {
		if err := checkWebSocketExportURL(raw, false); (err == nil) != ok {
			t.Errorf("%s: expected ok=%v, got %v", raw, ok, err)
		}
	}
	if err := checkWebSocketExportURL("http://inlet.internal/batches", true); err != nil {
		t.Fatalf("AQUIFER_WS_EXPORT_ALLOW_INSECURE should allow http: %v", err)
	}
}

func TestWebSocketExportSplitsBatchesBySizeAndHandles413(t *testing.T) {
	store := exportTestStore(t)
	var mu sync.Mutex
	var batchSizes []int
	var delivered []string
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var batch WebSocketExportBatch
		json.NewDecoder(req.Body).Decode(&batch)
		mu.Lock()
		defer mu.Unlock()
		batchSizes = append(batchSizes, len(batch.Events))
		// Takes at most two messages at a time, and never "huge".
		for _, event := range batch.Events {
			if event.MessageID == "huge" {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
		}
		if len(batch.Events) > 2 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		for _, event := range batch.Events {
			delivered = append(delivered, event.MessageID)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// Preload the export stream before the exporter starts, so its first
	// read holds all six messages and the receiver's 413 forces a split.
	ctx := context.Background()
	if err := store.client.XGroupCreateMkStream(ctx, store.exportKey(), webSocketExportGroup, "0").Err(); err != nil {
		t.Fatal(err)
	}
	store.EnableExport(1000)
	for _, id := range []string{"a", "b", "huge", "c", "d", "e"} {
		if _, err := store.Append(ctx, "session-big", "client", WebSocketEnvelope{Type: "command", MessageID: id, Payload: json.RawMessage(`{"x":1}`)}); err != nil {
			t.Fatal(err)
		}
	}
	exporter, err := StartWebSocketExport(store, WebSocketExportConfig{Enabled: true, URL: receiver.URL, BatchSize: 6, MaxPending: 1000, MaxBatchBytes: 1 << 20, IncludePayloads: true})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Stop()

	snapshot := func() ([]string, []int) {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), delivered...), append([]int(nil), batchSizes...)
	}
	// Retrying a 413 needs no backoff, so this finishes quickly.
	if !waitUntil(10*time.Second, func() bool { got, _ := snapshot(); return len(got) == 5 }) {
		got, sizes := snapshot()
		t.Fatalf("expected every message but huge to arrive; got %v (batches %v)", got, sizes)
	}
	if !waitUntil(10*time.Second, func() bool { return store.client.XLen(ctx, store.exportKey()).Val() == 0 }) {
		t.Fatal("the skipped message should be acknowledged too, so it doesn't block the stream")
	}
	got, _ := snapshot()
	sort.Strings(got)
	if strings.Join(got, ",") != "a,b,c,d,e" {
		t.Fatalf("expected exactly a, b, c, d and e once each, got %v", got)
	}
	_, sizes := snapshot()
	if len(sizes) < 2 || sizes[0] != 6 || sizes[1] >= sizes[0] {
		t.Fatalf("the first batch should hold all six and the 413 should shrink the next one, got %v", sizes)
	}
}

func TestWebSocketExportCapsBatchBytes(t *testing.T) {
	store := exportTestStore(t)
	var mu sync.Mutex
	var sizes []int
	delivered := 0
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var batch WebSocketExportBatch
		json.NewDecoder(req.Body).Decode(&batch)
		mu.Lock()
		sizes = append(sizes, len(batch.Events))
		delivered += len(batch.Events)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer counting.Close()
	// Preload the export stream so the exporter's first read has all four.
	ctx := context.Background()
	if err := store.client.XGroupCreateMkStream(ctx, store.exportKey(), webSocketExportGroup, "0").Err(); err != nil {
		t.Fatal(err)
	}
	store.EnableExport(1000)
	// About 550 encoded bytes per event: two fit under the 1500-byte cap,
	// three don't.
	payload := json.RawMessage(fmt.Sprintf(`{"blob":%q}`, strings.Repeat("x", 400)))
	for i := 0; i < 4; i++ {
		if _, err := store.Append(ctx, "session-bytes", "client", WebSocketEnvelope{Type: "command", MessageID: fmt.Sprintf("m%d", i), Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	exporter, err := StartWebSocketExport(store, WebSocketExportConfig{Enabled: true, URL: counting.URL, BatchSize: 10, MaxPending: 1000, MaxBatchBytes: 1500, IncludePayloads: true})
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Stop()
	if !waitUntil(15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return delivered == 4
	}) {
		t.Fatal("not every message was exported")
	}
	mu.Lock()
	defer mu.Unlock()
	sawPair := false
	for _, n := range sizes {
		if n > 2 {
			t.Fatalf("a 1500-byte cap should fit at most two ~550-byte messages per batch, got batches %v", sizes)
		}
		sawPair = sawPair || n == 2
	}
	if !sawPair {
		t.Fatalf("the cap should still pack two messages into a batch, got batches %v", sizes)
	}
}

func TestWebSocketExportStopRemovesItsIdleConsumer(t *testing.T) {
	store := exportTestStore(t)
	receiver := newExportReceiver(t, 0)
	exporter, err := StartWebSocketExport(store, WebSocketExportConfig{Enabled: true, URL: receiver.srv.URL, BatchSize: 10, MaxPending: 1000, IncludePayloads: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Append(ctx, "session-c", "client", WebSocketEnvelope{Type: "command", MessageID: "c1"}); err != nil {
		t.Fatal(err)
	}
	waitForExport(t, receiver, 1)
	// The receiver has the message; wait for the acknowledgement too, so
	// nothing is pending when Stop runs.
	if !waitUntil(10*time.Second, func() bool { return store.client.XLen(ctx, store.exportKey()).Val() == 0 }) {
		t.Fatal("the exported message was never acknowledged")
	}
	exporter.Stop()
	consumers, err := store.client.XInfoConsumers(ctx, store.exportKey(), webSocketExportGroup).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(consumers) != 0 {
		t.Fatalf("a stopped exporter with nothing pending should leave no consumer behind, got %+v", consumers)
	}
}
