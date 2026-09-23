package aquifer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type memoryWebSocketStreamStore struct {
	mu        sync.Mutex
	events    map[string][]WebSocketStreamEvent
	next      int64
	notify    chan struct{}
	available bool
	closed    bool
}

func newMemoryWebSocketStreamStore() *memoryWebSocketStreamStore {
	return &memoryWebSocketStreamStore{
		events:    make(map[string][]WebSocketStreamEvent),
		notify:    make(chan struct{}),
		available: true,
	}
}

func (s *memoryWebSocketStreamStore) Append(_ context.Context, sessionID, direction string, message WebSocketEnvelope) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available || s.closed {
		return "", errors.New("stream unavailable")
	}
	s.next++
	id := fmt.Sprintf("%d-0", s.next)
	copyMessage := message
	copyMessage.Payload = append(json.RawMessage(nil), message.Payload...)
	s.events[sessionID] = append(s.events[sessionID], WebSocketStreamEvent{
		StreamID:   id,
		Direction:  direction,
		Envelope:   copyMessage,
		RecordedAt: time.Now().UnixMilli(),
	})
	close(s.notify)
	s.notify = make(chan struct{})
	return id, nil
}

func (s *memoryWebSocketStreamStore) ReadAfter(ctx context.Context, sessionID, after string, count int64, block time.Duration) ([]WebSocketStreamEvent, error) {
	for {
		s.mu.Lock()
		if !s.available || s.closed {
			s.mu.Unlock()
			return nil, errors.New("stream unavailable")
		}
		var result []WebSocketStreamEvent
		for _, event := range s.events[sessionID] {
			cmp, err := compareRedisStreamIDs(event.StreamID, after)
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			if cmp > 0 {
				result = append(result, event)
				if count > 0 && int64(len(result)) >= count {
					break
				}
			}
		}
		notify := s.notify
		s.mu.Unlock()
		if len(result) > 0 || block < 0 {
			return result, nil
		}

		timer := time.NewTimer(block)
		select {
		case <-notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			return nil, nil
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		}
	}
}

func (s *memoryWebSocketStreamStore) CheckCursor(_ context.Context, sessionID, after string) error {
	if after == "" || after == "0-0" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available || s.closed {
		return errors.New("stream unavailable")
	}
	if len(s.events[sessionID]) == 0 {
		return ErrWebSocketReplayGap
	}
	cmp, err := compareRedisStreamIDs(after, s.events[sessionID][0].StreamID)
	if err != nil {
		return err
	}
	if cmp < 0 {
		return ErrWebSocketReplayGap
	}
	return nil
}

func (s *memoryWebSocketStreamStore) Ping(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available || s.closed {
		return errors.New("stream unavailable")
	}
	return nil
}

func (s *memoryWebSocketStreamStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.notify)
	}
	return nil
}

func testWebSocketConfig() WebSocketConfig {
	return WebSocketConfig{
		Enabled:          true,
		ReadBatch:        100,
		ReadBlock:        10 * time.Millisecond,
		MaxMessageBytes:  1024 * 1024,
		HandshakeTimeout: time.Second,
		ReconnectMax:     20 * time.Millisecond,
		Scheduler: WebSocketSchedulerConfig{
			MaxClients:   10,
			MaxUpstreams: 10,
			MaxWaiting:   10,
			ConnectRPS:   1000,
			SlowStartRPS: 1000,
		},
	}
}

func TestWebSocketProxyRecordsCorrelatesAndDeliversThroughStream(t *testing.T) {
	backendUpgrader := websocket.Upgrader{
		Subprotocols: []string{webSocketSubprotocol},
		CheckOrigin:  func(*http.Request) bool { return true },
	}
	backendAuthorization := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		responseHeaders := http.Header{}
		responseHeaders.Set(webSocketCapacityMaxHeader, "3")
		responseHeaders.Set(webSocketCapacityRPSHeader, "5")
		conn, err := backendUpgrader.Upgrade(w, r, responseHeaders)
		if err != nil {
			return
		}
		defer conn.Close()
		backendAuthorization <- r.Header.Get("Authorization")

		var command WebSocketEnvelope
		if err := conn.ReadJSON(&command); err != nil {
			return
		}
		_ = conn.WriteJSON(WebSocketEnvelope{Type: "ack", MessageID: command.MessageID})
		_ = conn.WriteJSON(WebSocketEnvelope{Type: "event", MessageID: "backend-1", CausedBy: command.MessageID, Payload: json.RawMessage(`{"part":1}`)})
		_ = conn.WriteJSON(WebSocketEnvelope{Type: "event", MessageID: "backend-2", CausedBy: command.MessageID, Payload: json.RawMessage(`{"part":2}`)})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	store := newMemoryWebSocketStreamStore()
	manager := NewWebSocketManager(testWebSocketConfig(), store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{}
	headers.Set(webSocketUpstreamURLHeader, toWebSocketURL(backend.URL))
	headers.Set("Authorization", "Bearer gateway-token")
	client := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=session-1&after=0-0", headers)
	defer client.Close()

	readWebSocketUntil(t, client, func(message WebSocketEnvelope) bool {
		return message.Type == "status" && message.State == "connected"
	})
	if err := client.WriteJSON(WebSocketEnvelope{
		Type:      "command",
		MessageID: "client-1",
		Payload:   json.RawMessage(`{"action":"start"}`),
	}); err != nil {
		t.Fatalf("write client command: %v", err)
	}

	seen := make(map[string]WebSocketEnvelope)
	deadline := time.Now().Add(2 * time.Second)
	for len(seen) < 4 && time.Now().Before(deadline) {
		message := readWebSocketEnvelope(t, client, time.Until(deadline))
		if message.Type == "command_recorded" || message.Type == "ack" || message.Type == "event" {
			seen[message.Type+":"+message.MessageID] = message
		}
	}
	if len(seen) != 4 {
		t.Fatalf("expected command record, ack, and two events, got %+v", seen)
	}
	for _, id := range []string{"backend-1", "backend-2"} {
		message := seen["event:"+id]
		if message.CausedBy != "client-1" || message.StreamID == "" {
			t.Fatalf("expected correlated durable event %s, got %+v", id, message)
		}
	}

	select {
	case authorization := <-backendAuthorization:
		if authorization != "Bearer gateway-token" {
			t.Fatalf("expected gateway authorization to reach upstream, got %q", authorization)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not observe forwarded headers")
	}

	snapshot := manager.queue.Snapshot()
	if snapshot.EffectiveMax != 3 || snapshot.EffectiveConnectRPS != 5 {
		t.Fatalf("expected successful handshake capacity headers to lower local limits, got %+v", snapshot)
	}

	store.mu.Lock()
	events := append([]WebSocketStreamEvent(nil), store.events["session-1"]...)
	store.mu.Unlock()
	if len(events) != 4 || events[0].Direction != "client" {
		t.Fatalf("expected one ordered client command and three backend entries, got %+v", events)
	}
}

func TestWebSocketProxyReconnectsUpstreamAndKeepsClientConnected(t *testing.T) {
	var connections atomic.Int32
	backendUpgrader := websocket.Upgrader{Subprotocols: []string{webSocketSubprotocol}, CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := backendUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if connections.Add(1) == 1 {
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseServiceRestart, "restart"), time.Now().Add(time.Second))
			return
		}
		_ = conn.WriteJSON(WebSocketEnvelope{Type: "event", MessageID: "after-reconnect", Payload: json.RawMessage(`{"ok":true}`)})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	store := newMemoryWebSocketStreamStore()
	cfg := testWebSocketConfig()
	cfg.Scheduler.ConnectRPS = 8
	cfg.Scheduler.SlowStartRPS = 1
	manager := NewWebSocketManager(cfg, store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{toWebSocketURL(backend.URL)}}
	client := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=reconnect&after=0-0", headers)
	defer client.Close()

	var sawReconnect, sawSecondConnection, sawEvent bool
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !sawEvent {
		message := readWebSocketEnvelope(t, client, time.Until(deadline))
		if message.Type == "status" && message.State == "reconnecting" {
			sawReconnect = true
		}
		if message.Type == "status" && message.State == "connected" && message.Generation >= 2 {
			sawSecondConnection = true
		}
		if message.Type == "event" && message.MessageID == "after-reconnect" {
			sawEvent = true
		}
	}
	if !sawReconnect || !sawSecondConnection || !sawEvent {
		t.Fatalf("expected reconnect lifecycle and durable event, reconnect=%v second=%v event=%v", sawReconnect, sawSecondConnection, sawEvent)
	}
	if snapshot := manager.queue.Snapshot(); snapshot.CurrentRampRPS != 4 {
		t.Fatalf("ordinary established-socket loss should preserve the ramp; expected 4 RPS after two successful handshakes, got %+v", snapshot)
	}
}

func TestWebSocketDrainNotifiesClosesAndRejectsNewHandshakes(t *testing.T) {
	backendUpgrader := websocket.Upgrader{Subprotocols: []string{webSocketSubprotocol}, CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := backendUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	store := newMemoryWebSocketStreamStore()
	manager := NewWebSocketManager(testWebSocketConfig(), store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{toWebSocketURL(backend.URL)}}
	client := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=drain-existing&after=0-0", headers)
	readWebSocketUntil(t, client, func(message WebSocketEnvelope) bool {
		return message.Type == "status" && message.State == "connected"
	})

	app.BeginDrain(50 * time.Millisecond)
	message := readWebSocketEnvelope(t, client, time.Second)
	for message.Type != "status" || message.State != "server_draining" {
		message = readWebSocketEnvelope(t, client, time.Second)
	}
	if message.RetryAfterMS != 50 {
		t.Fatalf("expected 50ms reconnect grace, got %+v", message)
	}

	client.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := client.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseServiceRestart {
		t.Fatalf("expected service-restart close code %d, got %v", websocket.CloseServiceRestart, err)
	}

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/websocket?session_id=drain-new&after=0-0"
	_, response, err := websocket.DefaultDialer.Dial(endpoint, headers)
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected draining handshake rejection, response=%v err=%v", response, err)
	}
	defer response.Body.Close()
	if response.Header.Get(nodeStateHeader) != "draining" {
		t.Fatalf("expected draining node-state header, got %v", response.Header)
	}
}

func TestWebSocketProxyRejectsHandshakeWhenStreamUnavailable(t *testing.T) {
	store := newMemoryWebSocketStreamStore()
	store.available = false
	manager := NewWebSocketManager(testWebSocketConfig(), store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{"ws://backend.invalid/socket"}}
	dialer := websocket.Dialer{Subprotocols: []string{webSocketSubprotocol}}
	_, response, err := dialer.Dial(toWebSocketURL(server.URL)+"/websocket?session_id=offline&after=0-0", headers)
	if err == nil {
		t.Fatal("expected WebSocket handshake to fail while stream is unavailable")
	}
	if response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got response=%v err=%v", response, err)
	}
	response.Body.Close()
}

func TestWebSocketProxyReplaysOnlyBackendEventsAfterCursor(t *testing.T) {
	store := newMemoryWebSocketStreamStore()
	firstID, err := store.Append(context.Background(), "replay", "backend", WebSocketEnvelope{
		Type:      "event",
		MessageID: "already-processed",
		Payload:   json.RawMessage(`{"part":1}`),
	})
	if err != nil {
		t.Fatalf("append first replay event: %v", err)
	}
	if _, err := store.Append(context.Background(), "replay", "client", WebSocketEnvelope{
		Type:      "command",
		MessageID: "client-between",
		Payload:   json.RawMessage(`{"action":"next"}`),
	}); err != nil {
		t.Fatalf("append client transcript event: %v", err)
	}
	if _, err := store.Append(context.Background(), "replay", "backend", WebSocketEnvelope{
		Type:      "event",
		MessageID: "needs-replay",
		Payload:   json.RawMessage(`{"part":2}`),
	}); err != nil {
		t.Fatalf("append second replay event: %v", err)
	}

	backendUpgrader := websocket.Upgrader{Subprotocols: []string{webSocketSubprotocol}, CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, upgradeErr := backendUpgrader.Upgrade(w, r, nil)
		if upgradeErr != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				return
			}
		}
	}))
	defer backend.Close()

	manager := NewWebSocketManager(testWebSocketConfig(), store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{toWebSocketURL(backend.URL)}}
	client := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=replay&after="+firstID, headers)
	defer client.Close()

	var replayed []string
	for {
		message := readWebSocketEnvelope(t, client, 2*time.Second)
		if message.Type == "event" {
			replayed = append(replayed, message.MessageID)
		}
		if message.Type == "status" && message.State == "replay_complete" {
			break
		}
	}
	if len(replayed) != 1 || replayed[0] != "needs-replay" {
		t.Fatalf("expected only the unseen backend event, got %v", replayed)
	}
}

func TestWebSocketProxySharesOnePerNodeUpstreamLimit(t *testing.T) {
	backendUpgrader := websocket.Upgrader{Subprotocols: []string{webSocketSubprotocol}, CheckOrigin: func(*http.Request) bool { return true }}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := backendUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	cfg := testWebSocketConfig()
	cfg.Scheduler.MaxUpstreams = 1
	store := newMemoryWebSocketStreamStore()
	manager := NewWebSocketManager(cfg, store)
	app := &Aquifer{webSockets: manager}
	server := httptest.NewServer(NewServer(app).Routes())
	defer server.Close()
	defer manager.Close()

	headers := http.Header{webSocketUpstreamURLHeader: []string{toWebSocketURL(backend.URL)}}
	first := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=limit-1&after=0-0", headers)
	readWebSocketUntil(t, first, func(message WebSocketEnvelope) bool {
		return message.Type == "status" && message.State == "connected"
	})

	second := dialAqueductWebSocket(t, server.URL+"/websocket?session_id=limit-2&after=0-0", headers)
	defer second.Close()
	messages := make(chan WebSocketEnvelope, 16)
	readErrors := make(chan error, 1)
	go func() {
		for {
			var message WebSocketEnvelope
			if err := second.ReadJSON(&message); err != nil {
				readErrors <- err
				return
			}
			messages <- message
		}
	}()

	waitingDeadline := time.After(time.Second)
	for {
		select {
		case message := <-messages:
			if message.Type == "status" && message.State == "connected" {
				t.Fatal("second client connected before the per-node upstream slot was released")
			}
			if message.Type == "status" && message.State == "waiting" {
				goto waiting
			}
		case err := <-readErrors:
			t.Fatalf("read second client while waiting: %v", err)
		case <-waitingDeadline:
			t.Fatal("second client did not report waiting")
		}
	}

waiting:
	select {
	case message := <-messages:
		if message.Type == "status" && message.State == "connected" {
			t.Fatal("second client connected while first still owned the local slot")
		}
	case <-time.After(100 * time.Millisecond):
	}

	_ = first.Close()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case message := <-messages:
			if message.Type == "status" && message.State == "connected" {
				return
			}
		case err := <-readErrors:
			t.Fatalf("read second client after release: %v", err)
		case <-deadline:
			t.Fatal("second client did not receive released per-node upstream slot")
		}
	}
}

func dialAqueductWebSocket(t *testing.T, rawURL string, headers http.Header) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{Subprotocols: []string{webSocketSubprotocol}, HandshakeTimeout: time.Second}
	conn, response, err := dialer.Dial(toWebSocketURL(rawURL), headers)
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		t.Fatalf("dial Aqueduct WebSocket: %v", err)
	}
	return conn
}

func readWebSocketUntil(t *testing.T, conn *websocket.Conn, match func(WebSocketEnvelope) bool) WebSocketEnvelope {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		message := readWebSocketEnvelope(t, conn, time.Until(deadline))
		if match(message) {
			return message
		}
	}
	t.Fatal("timed out waiting for matching WebSocket message")
	return WebSocketEnvelope{}
}

func readWebSocketEnvelope(t *testing.T, conn *websocket.Conn, timeout time.Duration) WebSocketEnvelope {
	t.Helper()
	if timeout <= 0 {
		t.Fatal("timed out waiting for WebSocket message")
	}
	conn.SetReadDeadline(time.Now().Add(timeout))
	var message WebSocketEnvelope
	if err := conn.ReadJSON(&message); err != nil {
		t.Fatalf("read WebSocket message: %v", err)
	}
	return message
}

func toWebSocketURL(raw string) string {
	return strings.NewReplacer("https://", "wss://", "http://", "ws://").Replace(raw)
}
