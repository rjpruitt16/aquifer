package aquifer

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrWebSocketClientLimit     = errors.New("websocket client connection limit reached")
	ErrWebSocketWaitingLimit    = errors.New("websocket waiting connection limit reached")
	ErrWebSocketSchedulerClosed = errors.New("websocket scheduler closed")
)

type WebSocketSchedulerConfig struct {
	MaxClients   int
	MaxUpstreams int
	MaxWaiting   int
	ConnectRPS   float64
}

type WebSocketSchedulerSnapshot struct {
	Clients             int     `json:"clients"`
	ActiveUpstreams     int     `json:"active_upstreams"`
	Waiting             int     `json:"waiting"`
	MaxClients          int     `json:"max_clients"`
	MaxUpstreams        int     `json:"max_upstreams"`
	EffectiveMax        int     `json:"effective_max_upstreams"`
	ConnectRPS          float64 `json:"connect_rps"`
	EffectiveConnectRPS float64 `json:"effective_connect_rps"`
}

type websocketWaiter struct {
	ready    chan struct{}
	granted  bool
	canceled bool
	err      error
}

type WebSocketAdmission struct {
	scheduler *WebSocketScheduler
	waiter    *websocketWaiter
	Position  int
}

type WebSocketScheduler struct {
	mu sync.Mutex

	maxClients      int
	maxUpstreams    int
	maxWaiting      int
	configuredRPS   float64
	advertisedMax   int
	advertisedRPS   float64
	clients         int
	activeUpstreams int
	waiters         []*websocketWaiter
	nextGrant       time.Time

	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	jitter    func(time.Duration) time.Duration
}

func NewWebSocketScheduler(cfg WebSocketSchedulerConfig) *WebSocketScheduler {
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 1000
	}
	if cfg.MaxUpstreams <= 0 {
		cfg.MaxUpstreams = 1000
	}
	if cfg.MaxWaiting <= 0 {
		cfg.MaxWaiting = 1000
	}
	if cfg.ConnectRPS <= 0 {
		cfg.ConnectRPS = 20
	}
	s := &WebSocketScheduler{
		maxClients:    cfg.MaxClients,
		maxUpstreams:  cfg.MaxUpstreams,
		maxWaiting:    cfg.MaxWaiting,
		configuredRPS: cfg.ConnectRPS,
		wake:          make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		jitter:        withJitter,
	}
	go s.run()
	return s
}

func (s *WebSocketScheduler) AcquireClient() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stop:
		return ErrWebSocketSchedulerClosed
	default:
	}
	if s.clients >= s.maxClients {
		return ErrWebSocketClientLimit
	}
	s.clients++
	return nil
}

func (s *WebSocketScheduler) ReleaseClient() {
	s.mu.Lock()
	if s.clients > 0 {
		s.clients--
	}
	s.mu.Unlock()
}

func (s *WebSocketScheduler) EnqueueUpstream() (*WebSocketAdmission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stop:
		return nil, ErrWebSocketSchedulerClosed
	default:
	}
	if len(s.waiters) >= s.maxWaiting {
		return nil, ErrWebSocketWaitingLimit
	}
	waiter := &websocketWaiter{ready: make(chan struct{})}
	s.waiters = append(s.waiters, waiter)
	admission := &WebSocketAdmission{scheduler: s, waiter: waiter, Position: len(s.waiters)}
	s.notify()
	return admission, nil
}

func (a *WebSocketAdmission) Wait(ctx context.Context) (func(), error) {
	if a == nil || a.scheduler == nil || a.waiter == nil {
		return nil, ErrWebSocketSchedulerClosed
	}
	select {
	case <-a.waiter.ready:
		if a.waiter.err != nil {
			return nil, a.waiter.err
		}
		return a.scheduler.releaseFunc(), nil
	case <-ctx.Done():
		s := a.scheduler
		s.mu.Lock()
		if a.waiter.granted {
			s.activeUpstreams--
			s.mu.Unlock()
			s.notify()
			return nil, ctx.Err()
		}
		a.waiter.canceled = true
		s.removeWaiterLocked(a.waiter)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *WebSocketScheduler) UpdateCapacity(maxConnections *int, connectRPS *float64) {
	s.mu.Lock()
	if maxConnections != nil && *maxConnections > 0 {
		s.advertisedMax = *maxConnections
	}
	if connectRPS != nil && *connectRPS > 0 {
		s.advertisedRPS = *connectRPS
	}
	s.mu.Unlock()
	s.notify()
}

func (s *WebSocketScheduler) Snapshot() WebSocketSchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return WebSocketSchedulerSnapshot{
		Clients:             s.clients,
		ActiveUpstreams:     s.activeUpstreams,
		Waiting:             len(s.waiters),
		MaxClients:          s.maxClients,
		MaxUpstreams:        s.maxUpstreams,
		EffectiveMax:        s.effectiveMaxLocked(),
		ConnectRPS:          s.configuredRPS,
		EffectiveConnectRPS: s.effectiveRPSLocked(),
	}
}

func (s *WebSocketScheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.notify()
		<-s.done
	})
}

func (s *WebSocketScheduler) run() {
	defer close(s.done)
	for {
		delay, hasWork := s.grantReady()
		if !hasWork {
			select {
			case <-s.wake:
			case <-s.stop:
				s.failWaiters()
				return
			}
			continue
		}

		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-s.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-s.stop:
			if !timer.Stop() {
				<-timer.C
			}
			s.failWaiters()
			return
		}
	}
}

// grantReady grants at most one connection per call so the configured
// connection-open rate applies even when many upstream slots are free.
func (s *WebSocketScheduler) grantReady() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.waiters) > 0 && s.waiters[0].canceled {
		s.waiters = s.waiters[1:]
	}
	if len(s.waiters) == 0 || s.activeUpstreams >= s.effectiveMaxLocked() {
		return 0, false
	}
	if delay := time.Until(s.nextGrant); delay > 0 {
		return delay, true
	}

	waiter := s.waiters[0]
	s.waiters = s.waiters[1:]
	waiter.granted = true
	s.activeUpstreams++
	interval := time.Duration(float64(time.Second) / s.effectiveRPSLocked())
	s.nextGrant = time.Now().Add(s.jitter(interval))
	close(waiter.ready)
	return 0, len(s.waiters) > 0
}

func (s *WebSocketScheduler) effectiveMaxLocked() int {
	if s.advertisedMax > 0 && s.advertisedMax < s.maxUpstreams {
		return s.advertisedMax
	}
	return s.maxUpstreams
}

func (s *WebSocketScheduler) effectiveRPSLocked() float64 {
	if s.advertisedRPS > 0 && s.advertisedRPS < s.configuredRPS {
		return s.advertisedRPS
	}
	return s.configuredRPS
}

func (s *WebSocketScheduler) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if s.activeUpstreams > 0 {
				s.activeUpstreams--
			}
			s.mu.Unlock()
			s.notify()
		})
	}
}

func (s *WebSocketScheduler) removeWaiterLocked(target *websocketWaiter) {
	for i, waiter := range s.waiters {
		if waiter == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

func (s *WebSocketScheduler) failWaiters() {
	s.mu.Lock()
	waiters := s.waiters
	s.waiters = nil
	for _, waiter := range waiters {
		waiter.err = ErrWebSocketSchedulerClosed
		close(waiter.ready)
	}
	s.mu.Unlock()
}

func (s *WebSocketScheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
