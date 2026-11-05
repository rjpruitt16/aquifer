// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"math"
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
//
// Slots go to queues in the order they started waiting, not to whichever
// asks first: a queue that wakes often (a steady stream of new jobs) would
// otherwise take nearly every slot from one with a static backlog that only
// wakes on its timer.
type sharedPace struct {
	mu sync.Mutex
	// last is when the most recent slot started. The next slot is computed
	// from it with the current rate on every call, so a rate change (a pool
	// gaining capacity) takes effect at once instead of after the old
	// interval.
	last    time.Time
	granted bool
	rate    func() float64
	// waiting maps each queue that was turned away to when it first asked
	// and when it last asked. A queue that stops asking (it went idle, or
	// stopped) is dropped after staleWaiter.
	waiting map[any]waiter
}

type waiter struct{ since, asked time.Time }

func newSharedPace(rate func() float64) *sharedPace {
	return &sharedPace{rate: rate, waiting: make(map[any]waiter)}
}

// reserve takes the next dispatch slot for who if one is free at now and
// it's who's turn, and returns 0. Otherwise it takes nothing and returns how
// long to wait before asking again. A nil sharedPace, or a non-positive rate,
// never limits.
func (s *sharedPace) reserve(who any, now time.Time) time.Duration {
	if s == nil {
		return 0
	}
	r := s.rate()
	if r <= 0 {
		return 0
	}
	// Same floor each queue already has: a pool whose members' reputations
	// collapsed must still get enough dispatches to recover.
	r = math.Max(r, MinRPS)
	interval := time.Duration(float64(time.Second) / r)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Forget queues that stopped asking. A waiting queue asks again at least
	// once per interval (its timer is set to the wait returned here).
	stale := 2*interval + 100*time.Millisecond
	for k, w := range s.waiting {
		if now.Sub(w.asked) > stale {
			delete(s.waiting, k)
		}
	}

	wait := func() time.Duration {
		w, ok := s.waiting[who]
		if !ok {
			w.since = now
		}
		w.asked = now
		s.waiting[who] = w
		if s.granted {
			if next := s.last.Add(interval); now.Before(next) {
				return next.Sub(now)
			}
		}
		// The slot is free but belongs to a queue that has waited longer; ask
		// again shortly, after it has had the chance to take it.
		return min(interval, 5*time.Millisecond)
	}

	// Carry at most one interval of unused credit, so a timer that fires a
	// little late doesn't lose throughput, but idle time doesn't bank a burst.
	start := now.Add(-interval)
	if s.granted {
		next := s.last.Add(interval)
		if now.Before(next) {
			return wait()
		}
		if next.After(start) {
			start = next
		}
	}
	mine, waitingNow := s.waiting[who]
	for k, w := range s.waiting {
		if k != who && (!waitingNow || w.since.Before(mine.since)) {
			return wait()
		}
	}
	delete(s.waiting, who)
	s.last, s.granted = start, true
	return 0
}
