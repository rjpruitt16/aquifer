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
