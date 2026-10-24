// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package websocket

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
	"github.com/rjpruitt16/aquifer/internal/httpx"
	"github.com/rjpruitt16/aquifer/internal/retry"
	"github.com/rjpruitt16/aquifer/internal/urlpolicy"
)

const (
	storeRetryFirstDelay = 100 * time.Millisecond
	storeRetryMaxDelay   = 2 * time.Second

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

type Manager struct {
	cfg      Config
	store    StreamStore
	queue    *Scheduler
	dialer   *websocket.Dialer
	upgrader websocket.Upgrader

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closeOnce  sync.Once
	generation atomic.Int64
	// messages counts every message sessions read, from clients and
	// backends, for the instance's load (see Messages).
	messages   atomic.Int64
	sessionsMu sync.Mutex
	sessions   map[*webSocketSession]struct{}
	draining   bool
}

func NewManager(cfg Config, store StreamStore) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		cfg:   cfg,
		store: store,
		queue: NewScheduler(cfg.Scheduler),
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
		ctx:      ctx,
		cancel:   cancel,
		sessions: make(map[*webSocketSession]struct{}),
	}
}

func NewManagerFromConfig(cfg Config) (*Manager, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	store, err := NewRedisStreamStore(cfg.RedisURL, cfg.StreamPrefix, cfg.StreamMaxEvents, cfg.StreamTTL, cfg.ReaderShards, cfg.StorePoolSize)
	if err != nil {
		return nil, err
	}
	return NewManager(cfg, store), nil
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m == nil || !m.cfg.Enabled {
		httpx.JSONError(w, "websocket proxy is disabled", http.StatusNotFound)
		return
	}
	if !websocket.IsWebSocketUpgrade(r) {
		httpx.JSONError(w, "websocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	if m.IsDraining() {
		httpx.WriteDraining(w, 1)
		return
	}
	if !containsString(websocket.Subprotocols(r), webSocketSubprotocol) {
		httpx.JSONError(w, "Sec-WebSocket-Protocol must include aqueduct.v1", http.StatusBadRequest)
		return
	}

	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		httpx.JSONError(w, "session_id is required", http.StatusBadRequest)
		return
	}
	after := defaultString(r.URL.Query().Get("after"), "0-0")
	if _, _, err := parseRedisStreamID(after); err != nil {
		httpx.JSONError(w, "after must be a Redis stream id", http.StatusBadRequest)
		return
	}

	upstreamURL := r.Header.Get(webSocketUpstreamURLHeader)
	if err := validateWebSocketUpstreamURL(upstreamURL); err != nil {
		httpx.JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	checkCtx, cancel := context.WithTimeout(r.Context(), m.cfg.HandshakeTimeout)
	defer cancel()
	if err := m.store.Ping(checkCtx); err != nil {
		w.Header().Set("Retry-After", "1")
		httpx.JSONError(w, "websocket event store unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := m.store.CheckCursor(checkCtx, sessionID, after); err != nil {
		if errors.Is(err, ErrWebSocketReplayGap) {
			httpx.JSONError(w, "websocket replay cursor is older than retained history", http.StatusConflict)
		} else {
			w.Header().Set("Retry-After", "1")
			httpx.JSONError(w, "websocket event store unavailable", http.StatusServiceUnavailable)
		}
		return
	}

	if err := m.queue.AcquireClient(); err != nil {
		w.Header().Set("Retry-After", "1")
		httpx.JSONError(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	defer m.queue.ReleaseClient()

	admission, err := m.queue.EnqueueUpstream()
	if err != nil {
		w.Header().Set("Retry-After", "1")
		httpx.JSONError(w, err.Error(), http.StatusTooManyRequests)
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
		commands:         make(chan Envelope, 128),
		initialAdmission: admission,
	}

	if !m.registerSession(session) {
		admission.Cancel()
		_ = session.client.WriteJSON(Envelope{
			Type:         "status",
			State:        "server_draining",
			Reason:       httpx.ErrDraining.Error(),
			RetryAfterMS: 1000,
		})
		session.client.Close(websocket.CloseServiceRestart, "aquifer is draining; reconnect through the gateway")
		return
	}
	defer m.unregisterSession(session)
	session.run()
}

func (m *Manager) Snapshot() map[string]any {
	if m == nil {
		return nil
	}
	snapshot := m.queue.Snapshot()
	return map[string]any{
		"enabled":                  true,
		"draining":                 m.IsDraining(),
		"clients":                  snapshot.Clients,
		"active_upstreams":         snapshot.ActiveUpstreams,
		"waiting":                  snapshot.Waiting,
		"max_client_connections":   snapshot.MaxClients,
		"max_upstream_connections": snapshot.MaxUpstreams,
		"effective_max_upstreams":  snapshot.EffectiveMax,
		"connect_rps":              snapshot.ConnectRPS,
		"slow_start_rps":           snapshot.SlowStartRPS,
		"current_ramp_rps":         snapshot.CurrentRampRPS,
		"effective_connect_rps":    snapshot.EffectiveConnectRPS,
	}
}

// Messages is how many WebSocket messages sessions have read, from clients
// and backends, since start.
func (m *Manager) Messages() int64 {
	if m == nil {
		return 0
	}
	return m.messages.Load()
}

// Sessions is how many clients are connected, and the most this instance
// takes (AQUIFER_WS_MAX_CLIENT_CONNECTIONS).
func (m *Manager) Sessions() (open, limit int) {
	if m == nil {
		return 0, 0
	}
	snapshot := m.queue.Snapshot()
	return snapshot.Clients, snapshot.MaxClients
}

func (m *Manager) IsDraining() bool {
	if m == nil {
		return false
	}
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	return m.draining
}

func (m *Manager) BeginDrain(grace time.Duration) {
	if m == nil {
		return
	}
	if grace < 0 {
		grace = 0
	}
	m.sessionsMu.Lock()
	if m.draining {
		m.sessionsMu.Unlock()
		return
	}
	m.draining = true
	sessions := m.sessionSnapshotLocked()
	m.sessionsMu.Unlock()

	for _, session := range sessions {
		go func(s *webSocketSession) {
			_ = s.client.WriteJSON(Envelope{
				Type:         "status",
				State:        "server_draining",
				Reason:       "reconnect through the gateway",
				RetryAfterMS: grace.Milliseconds(),
			})
		}(session)
	}

	go func() {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-m.ctx.Done():
		}
		m.sessionsMu.Lock()
		current := m.sessionSnapshotLocked()
		m.sessionsMu.Unlock()
		for _, session := range current {
			session.client.Close(websocket.CloseServiceRestart, "aquifer is draining; reconnect through the gateway")
		}
	}()
}

func (m *Manager) WaitDrained(ctx context.Context) error {
	if m == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) registerSession(session *webSocketSession) bool {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	if m.draining {
		return false
	}
	m.sessions[session] = struct{}{}
	m.wg.Add(1)
	return true
}

func (m *Manager) unregisterSession(session *webSocketSession) {
	m.sessionsMu.Lock()
	delete(m.sessions, session)
	m.sessionsMu.Unlock()
	m.wg.Done()
}

func (m *Manager) sessionSnapshotLocked() []*webSocketSession {
	sessions := make([]*webSocketSession, 0, len(m.sessions))
	for session := range m.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (m *Manager) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		m.BeginDrain(0)
		m.cancel()
		m.queue.Close()
		m.wg.Wait()
		if m.store != nil {
			_ = m.store.Close()
		}
	})
}

type webSocketSession struct {
	manager          *Manager
	ctx              context.Context
	cancel           context.CancelFunc
	sessionID        string
	cursor           string
	upstreamURL      string
	upstreamHeaders  http.Header
	client           *webSocketWriter
	commands         chan Envelope
	initialAdmission *Admission
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
	if err != nil && !s.manager.IsDraining() && !errors.Is(err, context.Canceled) && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		log.Printf("websocket session %s closed: %v", s.sessionID, err)
	}
}

func (s *webSocketSession) readClient() error {
	for {
		_, raw, err := s.client.conn.ReadMessage()
		if err != nil {
			return err
		}
		s.manager.messages.Add(1)
		message, err := decodeClientWebSocketMessage(raw)
		if err != nil {
			_ = s.client.WriteJSON(Envelope{Type: "error", Reason: err.Error()})
			continue
		}
		if s.manager.IsDraining() {
			_ = s.client.WriteJSON(Envelope{Type: "error", Reason: httpx.ErrDraining.Error()})
			continue
		}
		var streamID string
		err = s.retryStore(s.ctx, "record a client command", func() (err error) {
			streamID, err = s.manager.store.Append(s.ctx, s.sessionID, "client", message)
			return err
		})
		if err != nil {
			return err
		}
		if err := s.client.WriteJSON(Envelope{
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
	if err := s.client.WriteJSON(Envelope{Type: "status", State: "replaying"}); err != nil {
		return err
	}

	for {
		events, err := s.readAfterCursor()
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
	if err := s.client.WriteJSON(Envelope{Type: "status", State: "replay_complete"}); err != nil {
		return err
	}

	// Follow before the catch-up read, so an entry recorded between that read
	// and the wait below still signals the follow.
	var follow Follow
	err := s.retryStore(s.ctx, "follow the transcript", func() (err error) {
		follow, err = s.manager.store.Follow(s.ctx, s.sessionID, s.cursor)
		return err
	})
	if err != nil {
		return err
	}
	defer follow.Close()
	for {
		for {
			events, err := s.readAfterCursor()
			if err != nil {
				return err
			}
			if err := s.deliverEvents(events); err != nil {
				return err
			}
			if int64(len(events)) < s.manager.cfg.ReadBatch {
				break
			}
		}
		follow.Advance(s.cursor)
		select {
		case <-follow.C():
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *webSocketSession) readAfterCursor() ([]StreamEvent, error) {
	var events []StreamEvent
	err := s.retryStore(s.ctx, "read the transcript", func() (err error) {
		events, err = s.manager.store.ReadAfter(s.ctx, s.sessionID, s.cursor, s.manager.cfg.ReadBatch, -1)
		return err
	})
	return events, err
}

// retryStore runs a transcript read or write until it succeeds, ctx ends,
// or the store has kept failing for cfg.StoreRetry. On a busy instance a
// Valkey call can time out waiting for a connection; ending the session on
// the first one sent every session through the reconnect queue at once,
// and commands recorded before it expired unanswered (#53). A retried write
// may be recorded twice, which aqueduct.v1 allows: message IDs repeat, and
// clients drop duplicates.
func (s *webSocketSession) retryStore(ctx context.Context, what string, op func() error) error {
	deadline := time.Now().Add(s.manager.cfg.StoreRetry)
	delay := storeRetryFirstDelay
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			if attempt > 1 {
				log.Printf("websocket session %s: %s succeeded after %d attempts", s.sessionID, what, attempt)
			}
			return nil
		}
		if ctx.Err() != nil || time.Now().Add(delay).After(deadline) {
			return err
		}
		if attempt == 1 {
			log.Printf("websocket session %s: %s failed, retrying for up to %s: %v", s.sessionID, what, s.manager.cfg.StoreRetry, err)
		}
		if !retry.Sleep(ctx, retry.Jitter(delay)) {
			return ctx.Err()
		}
		delay = min(delay*2, storeRetryMaxDelay)
	}
}

func (s *webSocketSession) deliverEvents(events []StreamEvent) error {
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
		release, err := admission.Wait(s.ctx, func(position int) error {
			return s.client.WriteJSON(Envelope{Type: "status", State: "waiting", Position: position})
		})
		admission = nil
		if err != nil {
			return err
		}

		if err := s.client.WriteJSON(Envelope{Type: "status", State: "connecting"}); err != nil {
			release()
			return err
		}
		upstream, response, err := s.manager.dialer.DialContext(s.ctx, s.upstreamURL, s.upstreamHeaders)
		retryAfter := s.manager.applyCapacityHeaders(response)
		if err != nil {
			release()
			s.manager.queue.ReportFailure()
			attempt++
			backoff := reconnectBackoff(attempt, retryAfter, s.manager.cfg.ReconnectMax)
			if err := s.client.WriteJSON(Envelope{
				Type:         "status",
				State:        "reconnecting",
				Reason:       err.Error(),
				RetryAfterMS: backoff.Milliseconds(),
			}); err != nil {
				return err
			}
			if !retry.Sleep(s.ctx, backoff) {
				return s.ctx.Err()
			}
			continue
		}
		if upstream.Subprotocol() != webSocketSubprotocol {
			release()
			s.manager.queue.ReportFailure()
			_ = upstream.Close()
			return errors.New("upstream did not negotiate aqueduct.v1")
		}

		s.manager.queue.ReportSuccess()
		attempt = 0
		generation := s.manager.generation.Add(1)
		upstream.SetReadLimit(s.manager.cfg.MaxMessageBytes)
		if err := s.client.WriteJSON(Envelope{Type: "status", State: "connected", Generation: generation}); err != nil {
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
		if writeErr := s.client.WriteJSON(Envelope{
			Type:         "status",
			State:        "reconnecting",
			Reason:       err.Error(),
			RetryAfterMS: backoff.Milliseconds(),
		}); writeErr != nil {
			return writeErr
		}
		if !retry.Sleep(s.ctx, backoff) {
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
			s.manager.messages.Add(1)
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
			if err := s.retryStore(relayCtx, "record a backend message", func() error {
				_, err := s.manager.store.Append(relayCtx, s.sessionID, "backend", message)
				return err
			}); err != nil {
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

func (m *Manager) applyCapacityHeaders(response *http.Response) time.Duration {
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
	return retry.Jitter(backoff)
}

func validateWebSocketUpstreamURL(raw string) error {
	if raw == "" {
		return errors.New("X-Aqueduct-Upstream-URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return errors.New("X-Aqueduct-Upstream-URL must be an absolute ws:// or wss:// URL")
	}
	if !urlpolicy.Allowed(raw) {
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
