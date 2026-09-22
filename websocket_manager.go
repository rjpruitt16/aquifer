package aquifer

import (
	"context"
	"errors"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	webSocketCapacityMaxHeader = "X-Aqueduct-WS-Max-Connections"
	webSocketCapacityRPSHeader = "X-Aqueduct-WS-Connect-Rps"
)

type webSocketWriter struct {
	conn      *websocket.Conn
	mu        sync.Mutex
	closeOnce sync.Once
}

func (w *webSocketWriter) WriteJSON(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return w.conn.WriteJSON(value)
}

func (w *webSocketWriter) Close(code int, reason string) {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		deadline := time.Now().Add(time.Second)
		_ = w.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
		_ = w.conn.Close()
	})
}

type WebSocketManager struct {
	cfg      WebSocketConfig
	store    WebSocketStreamStore
	queue    *WebSocketScheduler
	dialer   *websocket.Dialer
	upgrader websocket.Upgrader

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closeOnce  sync.Once
	generation atomic.Int64
}

func NewWebSocketManager(cfg WebSocketConfig, store WebSocketStreamStore) *WebSocketManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &WebSocketManager{
		cfg:   cfg,
		store: store,
		queue: NewWebSocketScheduler(cfg.Scheduler),
		dialer: &websocket.Dialer{
			HandshakeTimeout: cfg.HandshakeTimeout,
			Subprotocols:     []string{webSocketSubprotocol},
			Proxy:            http.ProxyFromEnvironment,
		},
		upgrader: websocket.Upgrader{
			HandshakeTimeout: cfg.HandshakeTimeout,
			Subprotocols:     []string{webSocketSubprotocol},
			CheckOrigin: func(*http.Request) bool {
				// Authentication and origin policy belong to the trusted gateway.
				return true
			},
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

func NewWebSocketManagerFromConfig(cfg WebSocketConfig) (*WebSocketManager, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	store, err := NewRedisWebSocketStreamStore(cfg.RedisURL, cfg.StreamPrefix, cfg.StreamMaxEvents)
	if err != nil {
		return nil, err
	}
	return NewWebSocketManager(cfg, store), nil
}

func (m *WebSocketManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m == nil || !m.cfg.Enabled {
		jsonError(w, "websocket proxy is disabled", http.StatusNotFound)
		return
	}
	if !websocket.IsWebSocketUpgrade(r) {
		jsonError(w, "websocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	if !containsString(websocket.Subprotocols(r), webSocketSubprotocol) {
		jsonError(w, "Sec-WebSocket-Protocol must include aqueduct.v1", http.StatusBadRequest)
		return
	}

	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		jsonError(w, "session_id is required", http.StatusBadRequest)
		return
	}
	after := defaultString(r.URL.Query().Get("after"), "0-0")
	if _, _, err := parseRedisStreamID(after); err != nil {
		jsonError(w, "after must be a Redis stream id", http.StatusBadRequest)
		return
	}

	upstreamURL := r.Header.Get(webSocketUpstreamURLHeader)
	if err := validateWebSocketUpstreamURL(upstreamURL); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	checkCtx, cancel := context.WithTimeout(r.Context(), m.cfg.HandshakeTimeout)
	defer cancel()
	if err := m.store.Ping(checkCtx); err != nil {
		w.Header().Set("Retry-After", "1")
		jsonError(w, "websocket event store unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := m.store.CheckCursor(checkCtx, sessionID, after); err != nil {
		if errors.Is(err, ErrWebSocketReplayGap) {
			jsonError(w, "websocket replay cursor is older than retained history", http.StatusConflict)
		} else {
			w.Header().Set("Retry-After", "1")
			jsonError(w, "websocket event store unavailable", http.StatusServiceUnavailable)
		}
		return
	}

	if err := m.queue.AcquireClient(); err != nil {
		w.Header().Set("Retry-After", "1")
		jsonError(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	defer m.queue.ReleaseClient()

	admission, err := m.queue.EnqueueUpstream()
	if err != nil {
		w.Header().Set("Retry-After", "1")
		jsonError(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	client, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		admission.Cancel()
		return
	}
	client.SetReadLimit(m.cfg.MaxMessageBytes)

	sessionCtx, sessionCancel := context.WithCancel(m.ctx)
	session := &webSocketSession{
		manager:          m,
		ctx:              sessionCtx,
		cancel:           sessionCancel,
		sessionID:        sessionID,
		cursor:           after,
		upstreamURL:      upstreamURL,
		upstreamHeaders:  forwardedWebSocketHeaders(r.Header, sessionID),
		client:           &webSocketWriter{conn: client},
		commands:         make(chan WebSocketEnvelope, 128),
		initialAdmission: admission,
	}

	m.wg.Add(1)
	defer m.wg.Done()
	session.run()
}

func (m *WebSocketManager) Snapshot() map[string]any {
	if m == nil {
		return nil
	}
	snapshot := m.queue.Snapshot()
	return map[string]any{
		"enabled":                  true,
		"clients":                  snapshot.Clients,
		"active_upstreams":         snapshot.ActiveUpstreams,
		"waiting":                  snapshot.Waiting,
		"max_client_connections":   snapshot.MaxClients,
		"max_upstream_connections": snapshot.MaxUpstreams,
		"effective_max_upstreams":  snapshot.EffectiveMax,
		"connect_rps":              snapshot.ConnectRPS,
		"effective_connect_rps":    snapshot.EffectiveConnectRPS,
	}
}

func (m *WebSocketManager) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		m.cancel()
		m.queue.Close()
		m.wg.Wait()
		if m.store != nil {
			_ = m.store.Close()
		}
	})
}

type webSocketSession struct {
	manager          *WebSocketManager
	ctx              context.Context
	cancel           context.CancelFunc
	sessionID        string
	cursor           string
	upstreamURL      string
	upstreamHeaders  http.Header
	client           *webSocketWriter
	commands         chan WebSocketEnvelope
	initialAdmission *WebSocketAdmission
}

func (s *webSocketSession) run() {
	defer s.cancel()

	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	start := func(run func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- run()
		}()
	}
	start(s.readClient)
	start(s.deliverStream)
	start(s.connectUpstream)

	err := <-errCh
	s.cancel()
	s.client.Close(websocket.CloseNormalClosure, "session closed")
	wg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Printf("websocket session %s closed: %v", s.sessionID, err)
	}
}

func (s *webSocketSession) readClient() error {
	for {
		_, raw, err := s.client.conn.ReadMessage()
		if err != nil {
			return err
		}
		message, err := decodeClientWebSocketMessage(raw)
		if err != nil {
			_ = s.client.WriteJSON(WebSocketEnvelope{Type: "error", Reason: err.Error()})
			continue
		}
		streamID, err := s.manager.store.Append(s.ctx, s.sessionID, "client", message)
		if err != nil {
			return err
		}
		if err := s.client.WriteJSON(WebSocketEnvelope{
			Type:      "command_recorded",
			MessageID: message.MessageID,
			StreamID:  streamID,
		}); err != nil {
			return err
		}
		select {
		case s.commands <- message:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *webSocketSession) deliverStream() error {
	if err := s.client.WriteJSON(WebSocketEnvelope{Type: "status", State: "replaying"}); err != nil {
		return err
	}

	for {
		events, err := s.manager.store.ReadAfter(s.ctx, s.sessionID, s.cursor, s.manager.cfg.ReadBatch, -1)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			break
		}
		if err := s.deliverEvents(events); err != nil {
			return err
		}
		if int64(len(events)) < s.manager.cfg.ReadBatch {
			break
		}
	}
	if err := s.client.WriteJSON(WebSocketEnvelope{Type: "status", State: "replay_complete"}); err != nil {
		return err
	}

	for {
		events, err := s.manager.store.ReadAfter(s.ctx, s.sessionID, s.cursor, s.manager.cfg.ReadBatch, s.manager.cfg.ReadBlock)
		if err != nil {
			return err
		}
		if err := s.deliverEvents(events); err != nil {
			return err
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}
	}
}

func (s *webSocketSession) deliverEvents(events []WebSocketStreamEvent) error {
	for _, event := range events {
		s.cursor = event.StreamID
		if event.Direction != "backend" {
			continue
		}
		message := event.Envelope
		message.StreamID = event.StreamID
		if err := s.client.WriteJSON(message); err != nil {
			return err
		}
	}
	return nil
}

func (s *webSocketSession) connectUpstream() error {
	admission := s.initialAdmission
	attempt := 0
	for {
		if admission == nil {
			var err error
			admission, err = s.manager.queue.EnqueueUpstream()
			if err != nil {
				return err
			}
		}
		if err := s.client.WriteJSON(WebSocketEnvelope{Type: "status", State: "waiting", Position: admission.Position}); err != nil {
			admission.Cancel()
			return err
		}
		release, err := admission.Wait(s.ctx)
		admission = nil
		if err != nil {
			return err
		}

		if err := s.client.WriteJSON(WebSocketEnvelope{Type: "status", State: "connecting"}); err != nil {
			release()
			return err
		}
		upstream, response, err := s.manager.dialer.DialContext(s.ctx, s.upstreamURL, s.upstreamHeaders)
		retryAfter := s.manager.applyCapacityHeaders(response)
		if err != nil {
			release()
			attempt++
			backoff := reconnectBackoff(attempt, retryAfter, s.manager.cfg.ReconnectMax)
			if err := s.client.WriteJSON(WebSocketEnvelope{
				Type:         "status",
				State:        "reconnecting",
				Reason:       err.Error(),
				RetryAfterMS: backoff.Milliseconds(),
			}); err != nil {
				return err
			}
			if !sleepBeforeRetryContext(s.ctx, backoff) {
				return s.ctx.Err()
			}
			continue
		}
		if upstream.Subprotocol() != webSocketSubprotocol {
			release()
			_ = upstream.Close()
			return errors.New("upstream did not negotiate aqueduct.v1")
		}

		attempt = 0
		generation := s.manager.generation.Add(1)
		upstream.SetReadLimit(s.manager.cfg.MaxMessageBytes)
		if err := s.client.WriteJSON(WebSocketEnvelope{Type: "status", State: "connected", Generation: generation}); err != nil {
			release()
			_ = upstream.Close()
			return err
		}
		err = s.relayUpstream(upstream, generation)
		release()
		_ = upstream.Close()
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		attempt++
		backoff := reconnectBackoff(attempt, 0, s.manager.cfg.ReconnectMax)
		if writeErr := s.client.WriteJSON(WebSocketEnvelope{
			Type:         "status",
			State:        "reconnecting",
			Reason:       err.Error(),
			RetryAfterMS: backoff.Milliseconds(),
		}); writeErr != nil {
			return writeErr
		}
		if !sleepBeforeRetryContext(s.ctx, backoff) {
			return s.ctx.Err()
		}
	}
}

func (s *webSocketSession) relayUpstream(upstream *websocket.Conn, generation int64) error {
	relayCtx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, raw, err := upstream.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			message, err := decodeBackendWebSocketMessage(raw)
			if err != nil {
				errCh <- err
				return
			}
			if message.Type == "aqueduct.capacity" {
				s.manager.queue.UpdateCapacity(message.MaxConnections, message.ConnectRPS)
				continue
			}
			message.Generation = generation
			if _, err := s.manager.store.Append(relayCtx, s.sessionID, "backend", message); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case message := <-s.commands:
				upstream.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := upstream.WriteJSON(message); err != nil {
					errCh <- err
					return
				}
			case <-relayCtx.Done():
				errCh <- relayCtx.Err()
				return
			}
		}
	}()

	err := <-errCh
	cancel()
	_ = upstream.Close()
	wg.Wait()
	return err
}

func (m *WebSocketManager) applyCapacityHeaders(response *http.Response) time.Duration {
	if response == nil {
		return 0
	}
	if response.StatusCode != http.StatusSwitchingProtocols && response.Body != nil {
		defer response.Body.Close()
	}
	var maxConnections *int
	if raw := response.Header.Get(webSocketCapacityMaxHeader); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			maxConnections = &value
		}
	}
	var connectRPS *float64
	if raw := response.Header.Get(webSocketCapacityRPSHeader); raw != "" {
		if value, err := strconv.ParseFloat(raw, 64); err == nil && value > 0 {
			connectRPS = &value
		}
	}
	m.queue.UpdateCapacity(maxConnections, connectRPS)

	if raw := response.Header.Get("Retry-After"); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 0
}

func reconnectBackoff(attempt int, retryAfter, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
	if retryAfter > backoff {
		backoff = retryAfter
	}
	if maximum > 0 && backoff > maximum {
		backoff = maximum
	}
	return withJitter(backoff)
}

func validateWebSocketUpstreamURL(raw string) error {
	if raw == "" {
		return errors.New("X-Aqueduct-Upstream-URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return errors.New("X-Aqueduct-Upstream-URL must be an absolute ws:// or wss:// URL")
	}
	if !domainAllowed(raw) {
		return errors.New("websocket upstream domain is not in AQUIFER_ALLOWED_URL_DOMAINS")
	}
	return nil
}

func forwardedWebSocketHeaders(source http.Header, sessionID string) http.Header {
	destination := make(http.Header)
	for name, values := range source {
		canonical := http.CanonicalHeaderKey(name)
		if isWebSocketHopHeader(canonical) || strings.HasPrefix(canonical, "X-Aqueduct-") || strings.HasPrefix(canonical, "X-Aquifer-") {
			continue
		}
		for _, value := range values {
			destination.Add(canonical, value)
		}
	}
	destination.Set("X-Aqueduct-Session-ID", sessionID)
	return destination
}

func isWebSocketHopHeader(name string) bool {
	switch name {
	case "Connection", "Upgrade", "Sec-Websocket-Key", "Sec-Websocket-Version", "Sec-Websocket-Extensions", "Sec-Websocket-Protocol", "Host":
		return true
	default:
		return false
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
