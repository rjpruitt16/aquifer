// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"sync"
	"time"
)

// sharedPace holds a target API's total rate across all of its account
// queues. Each queue still paces itself, but before it dispatches it must
// also take the next slot here, so the target sees at most the configured
// ceiling no matter how many tenants are active. Without it the ceiling was
// enforced only by enforceAggregateBudget every few seconds, and each queue's
// per-success recovery climbed back to the full per-queue rate in between
// (issue #146).
type sharedPace struct {
	mu   sync.Mutex
	next time.Time
	rate func() float64
}

func newSharedPace(rate func() float64) *sharedPace {
	return &sharedPace{rate: rate}
}

// reserve takes the next dispatch slot if one is free at now and returns 0.
// Otherwise it takes nothing and returns how long until the next slot. A nil
// sharedPace, or a non-positive rate, never limits.
func (s *sharedPace) reserve(now time.Time) time.Duration {
	if s == nil {
		return 0
	}
	r := s.rate()
	if r <= 0 {
		return 0
	}
	interval := time.Duration(float64(time.Second) / r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Before(s.next) {
		return s.next.Sub(now)
	}
	// Carry at most one interval of unused credit, so a timer that fires a
	// little late doesn't lose throughput, but idle time doesn't bank a burst.
	start := s.next
	if floor := now.Add(-interval); start.Before(floor) {
		start = floor
	}
	s.next = start.Add(interval)
	return 0
}
