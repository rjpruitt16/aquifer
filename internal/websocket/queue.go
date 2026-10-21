// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rjpruitt16/aquifer/internal/retry"
)

var (
	ErrWebSocketClientLimit     = errors.New("websocket client connection limit reached")
	ErrWebSocketWaitingLimit    = errors.New("websocket waiting connection limit reached")
	ErrWebSocketSchedulerClosed = errors.New("websocket scheduler closed")
)

type SchedulerConfig struct {
	MaxClients   int
	MaxUpstreams int
	MaxWaiting   int
	ConnectRPS   float64
	SlowStartRPS float64
}

type SchedulerSnapshot struct {
	Clients             int     `json:"clients"`
	ActiveUpstreams     int     `json:"active_upstreams"`
	Waiting             int     `json:"waiting"`
	MaxClients          int     `json:"max_clients"`
	MaxUpstreams        int     `json:"max_upstreams"`
	EffectiveMax        int     `json:"effective_max_upstreams"`
	ConnectRPS          float64 `json:"connect_rps"`
	SlowStartRPS        float64 `json:"slow_start_rps"`
	CurrentRampRPS      float64 `json:"current_ramp_rps"`
	EffectiveConnectRPS float64 `json:"effective_connect_rps"`
}

type websocketWaiter struct {
	ready     chan struct{}
	positions chan int
	position  int
	granted   bool
	canceled  bool
	err       error
}

type Admission struct {
	scheduler *Scheduler
	waiter    *websocketWaiter
	Position  int
}

// Scheduler is the per-Aquifer connection queue. It admits client
// sockets, holds target API connection attempts in FIFO order, and dispatches
// those attempts at the configured, jittered connection-open rate.
type Scheduler struct {
	mu sync.Mutex

	maxClients      int
	maxUpstreams    int
	maxWaiting      int
	configuredRPS   float64
	slowStartRPS    float64
	rampRPS         float64
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

func NewScheduler(cfg SchedulerConfig) *Scheduler {
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
	if cfg.SlowStartRPS <= 0 {
		cfg.SlowStartRPS = 1
	}
	if cfg.SlowStartRPS > cfg.ConnectRPS {
		cfg.SlowStartRPS = cfg.ConnectRPS
	}
	s := &Scheduler{
		maxClients:    cfg.MaxClients,
		maxUpstreams:  cfg.MaxUpstreams,
		maxWaiting:    cfg.MaxWaiting,
		configuredRPS: cfg.ConnectRPS,
		slowStartRPS:  cfg.SlowStartRPS,
		rampRPS:       cfg.SlowStartRPS,
		wake:          make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		jitter:        retry.Jitter,
	}
	go s.run()
	return s
}

func (s *Scheduler) AcquireClient() error {
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

func (s *Scheduler) ReleaseClient() {
	s.mu.Lock()
	if s.clients > 0 {
		s.clients--
	}
	s.mu.Unlock()
}

func (s *Scheduler) EnqueueUpstream() (*Admission, error) {
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
	position := len(s.waiters) + 1
	waiter := &websocketWaiter{
		ready:     make(chan struct{}),
		positions: make(chan int, 1),
		position:  position,
	}
	s.waiters = append(s.waiters, waiter)
	admission := &Admission{scheduler: s, waiter: waiter, Position: position}
	s.notify()
	return admission, nil
}

func (a *Admission) Wait(ctx context.Context, onPosition func(int) error) (func(), error) {
	if a == nil || a.scheduler == nil || a.waiter == nil {
		return nil, ErrWebSocketSchedulerClosed
	}
	lastPosition := a.Position
	if onPosition != nil {
		if err := onPosition(lastPosition); err != nil {
			a.Cancel()
			return nil, err
		}
	}
	for {
		select {
		case <-a.waiter.ready:
			if a.waiter.err != nil {
				return nil, a.waiter.err
			}
			return a.scheduler.releaseFunc(), nil
		case position := <-a.waiter.positions:
			if position == lastPosition {
				continue
			}
			lastPosition = position
			if onPosition != nil {
				if err := onPosition(position); err != nil {
					a.Cancel()
					return nil, err
				}
			}
		case <-ctx.Done():
			s := a.scheduler
			s.mu.Lock()
			if a.waiter.granted {
				s.activeUpstreams--
				a.waiter.granted = false
				s.mu.Unlock()
				s.notify()
				return nil, ctx.Err()
			}
			a.waiter.canceled = true
			s.removeWaiterLocked(a.waiter)
			s.publishPositionsLocked()
			s.mu.Unlock()
			s.notify()
			return nil, ctx.Err()
		}
	}
}

func (a *Admission) Cancel() {
	if a == nil || a.scheduler == nil || a.waiter == nil {
		return
	}
	s := a.scheduler
	s.mu.Lock()
	if a.waiter.granted {
		s.activeUpstreams--
		a.waiter.granted = false
	} else if !a.waiter.canceled {
		a.waiter.canceled = true
		s.removeWaiterLocked(a.waiter)
		s.publishPositionsLocked()
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Scheduler) ReportSuccess() {
	s.mu.Lock()
	s.rampRPS *= 2
	if s.rampRPS > s.configuredRPS {
		s.rampRPS = s.configuredRPS
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Scheduler) ReportFailure() {
	s.mu.Lock()
	s.rampRPS = s.slowStartRPS
	if len(s.waiters) > 0 {
		interval := time.Duration(float64(time.Second) / s.effectiveRPSLocked())
		earliest := time.Now().Add(s.jitter(interval))
		if s.nextGrant.Before(earliest) {
			s.nextGrant = earliest
		}
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Scheduler) UpdateCapacity(maxConnections *int, connectRPS *float64) {
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

func (s *Scheduler) Snapshot() SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SchedulerSnapshot{
		Clients:             s.clients,
		ActiveUpstreams:     s.activeUpstreams,
		Waiting:             len(s.waiters),
		MaxClients:          s.maxClients,
		MaxUpstreams:        s.maxUpstreams,
		EffectiveMax:        s.effectiveMaxLocked(),
		ConnectRPS:          s.configuredRPS,
		SlowStartRPS:        s.slowStartRPS,
		CurrentRampRPS:      s.rampRPS,
		EffectiveConnectRPS: s.effectiveRPSLocked(),
	}
}

func (s *Scheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.notify()
		<-s.done
	})
}

func (s *Scheduler) run() {
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
				select {
				case <-timer.C:
				default:
				}
			}
		case <-s.stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.failWaiters()
			return
		}
	}
}

// grantReady grants at most one connection per call so the configured
// connection-open rate applies even when many target API slots are free.
func (s *Scheduler) grantReady() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.waiters) > 0 && s.waiters[0].canceled {
		s.waiters = s.waiters[1:]
		s.publishPositionsLocked()
	}
	if len(s.waiters) == 0 || s.activeUpstreams >= s.effectiveMaxLocked() {
		return 0, false
	}
	if delay := time.Until(s.nextGrant); delay > 0 {
		return delay, true
	}

	waiter := s.waiters[0]
	s.waiters = s.waiters[1:]
	s.publishPositionsLocked()
	waiter.granted = true
	s.activeUpstreams++
	interval := time.Duration(float64(time.Second) / s.effectiveRPSLocked())
	s.nextGrant = time.Now().Add(s.jitter(interval))
	close(waiter.ready)
	return 0, len(s.waiters) > 0
}

func (s *Scheduler) effectiveMaxLocked() int {
	if s.advertisedMax > 0 && s.advertisedMax < s.maxUpstreams {
		return s.advertisedMax
	}
	return s.maxUpstreams
}

func (s *Scheduler) effectiveRPSLocked() float64 {
	effective := s.configuredRPS
	if s.rampRPS > 0 && s.rampRPS < effective {
		effective = s.rampRPS
	}
	if s.advertisedRPS > 0 && s.advertisedRPS < effective {
		effective = s.advertisedRPS
	}
	return effective
}

func (s *Scheduler) publishPositionsLocked() {
	for i, waiter := range s.waiters {
		position := i + 1
		if waiter.position == position {
			continue
		}
		waiter.position = position
		select {
		case <-waiter.positions:
		default:
		}
		select {
		case waiter.positions <- position:
		default:
		}
	}
}

func (s *Scheduler) releaseFunc() func() {
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

func (s *Scheduler) removeWaiterLocked(target *websocketWaiter) {
	for i, waiter := range s.waiters {
		if waiter == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

func (s *Scheduler) failWaiters() {
	s.mu.Lock()
	waiters := s.waiters
	s.waiters = nil
	for _, waiter := range waiters {
		waiter.err = ErrWebSocketSchedulerClosed
		close(waiter.ready)
	}
	s.mu.Unlock()
}

func (s *Scheduler) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
