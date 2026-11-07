// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

// New sessions wait while the process is over its memory limit, the way new
// HTTP jobs do; under the limit they're accepted as before (issue #35).
func TestWebSocketHandshakeRespectsTheMemoryLimit(t *testing.T) {
	store := newMemoryWebSocketStreamStore()
	manager := NewManager(testWebSocketConfig(), store)
	var usedMB atomic.Int64
	usedMB.Store(900)
	manager.SetMemoryCheck(func() (int64, int64) { return usedMB.Load(), 800 })
	server := serveManager(manager)
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{"ws://backend.invalid/socket"}}
	dialer := websocket.Dialer{Subprotocols: []string{webSocketSubprotocol}}
	url := toWebSocketURL(server.URL) + "/websocket?session_id=mem&after=0-0"

	_, response, err := dialer.Dial(url, headers)
	if err == nil || response == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("over the memory limit: expected 429, got response=%v err=%v", response, err)
	}
	if response.Header.Get("Retry-After") == "" {
		t.Fatal("expected Retry-After on a memory refusal")
	}
	var body map[string]any
	json.NewDecoder(response.Body).Decode(&body)
	response.Body.Close()
	if body["limit_reason"] != "memory" || body["limit"] != float64(800) || body["current"] != float64(900) {
		t.Fatalf("refusal body = %v, want limit_reason memory, limit 800, current 900", body)
	}

	usedMB.Store(700)
	conn, response, err := dialer.Dial(url, headers)
	if err != nil {
		t.Fatalf("under the memory limit the handshake should succeed: response=%v err=%v", response, err)
	}
	conn.Close()
}
