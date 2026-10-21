// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/websocket"
)

// unavailableStreamStore is enough to build a manager; these tests never
// open a session.
type unavailableStreamStore struct{}

func (unavailableStreamStore) Append(context.Context, string, string, WebSocketEnvelope) (string, error) {
	return "", context.Canceled
}
func (unavailableStreamStore) ReadAfter(context.Context, string, string, int64, time.Duration) ([]WebSocketStreamEvent, error) {
	return nil, context.Canceled
}
func (unavailableStreamStore) CheckCursor(context.Context, string, string) error { return nil }
func (unavailableStreamStore) Ping(context.Context) error                        { return context.Canceled }
func (unavailableStreamStore) Follow(context.Context, string, string) (websocket.Follow, error) {
	return nil, context.Canceled
}
func (unavailableStreamStore) Close() error { return nil }

func TestWebSocketRouteIsNotFoundWhenTheProxyIsOff(t *testing.T) {
	server := httptest.NewServer(NewServer(&Aquifer{}).Routes())
	defer server.Close()

	resp, err := http.Get(server.URL + "/websocket?session_id=s&after=0-0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 without a WebSocket manager, got %d", resp.StatusCode)
	}
}

// The route hands requests to the manager (a plain GET gets its 426), and
// draining the node drains the manager's sessions.
func TestWebSocketRouteReachesTheManagerAndFollowsDrain(t *testing.T) {
	cfg := LoadWebSocketConfig()
	cfg.Enabled = true
	manager := NewWebSocketManager(cfg, unavailableStreamStore{})
	defer manager.Close()
	app := &Aquifer{}
	app.SetWebSocketManager(manager)
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()

	resp, err := http.Get(server.URL + "/websocket?session_id=s&after=0-0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("expected the manager's 426 for a plain GET, got %d", resp.StatusCode)
	}

	app.BeginDrain(10 * time.Millisecond)
	if !manager.IsDraining() {
		t.Fatal("expected Aquifer.BeginDrain to drain the WebSocket manager")
	}
}
