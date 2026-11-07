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
// also take the next slot here, so the target sees at most its total no
// matter how many tenants are active.
//
// The total is set by the target at runtime: the highest X-Aqueduct-Rps it
// is advertising to any queue that asked in the last activeWindow. Only when
// it advertises nothing does the configured rate (or a pool's live
// capacity, which its members advertise) apply. A target that stamps one
// number on every response therefore gets that number in total, not that
// number per tenant, and a target that advertises 1000 to one tenant raises
// its total to 1000 for as long as it keeps doing so. Without it the ceiling was
// enforced only by an aggregate throttle every few seconds, and each queue's
// per-success recovery climbed back to the full per-queue rate in between
// (issue #146).
//
// When queues contend, slots are shared in proportion to each queue's
// weight: the rate the target last advertised for that queue
// (X-Aqueduct-Rps), or its own pace when it advertised nothing. That keeps
// the target in charge of priority: advertising 20 to one tenant and 10 to
// another gives the first two slots for every one of the second's, while the
// total stays at the ceiling. Equal weights get equal turns.
//
// This is stride scheduling. Every queue has a pass; taking a slot adds
// 1/weight to it, and a free slot goes to the contending queue with the
// lowest pass, ties to the one that has waited longest. A queue that wakes
// often (a steady stream of new jobs) therefore can't take slots from one
// with a static backlog that only wakes on its timer, and a queue that was
// idle rejoins at the current pass instead of with banked credit.
type sharedPace struct {
	mu sync.Mutex
	// last is when the most recent slot started. The next slot is computed
	// from it with the current rate on every call, so a rate change (a pool
	// gaining capacity) takes effect at once instead of after the old
	// interval.
	last    time.Time
	granted bool
	rate    func() float64
	// followAdvertised lets advertised rates set the total. Off for a pool,
	// whose capacity already sums what its members advertise.
	followAdvertised bool
	// vtime is the pass of the most recent slot. A queue's pass is never
	// below it when it asks, so idle time banks nothing.
	vtime float64
	// queues holds every queue that asked recently: its pass, and the window
	// in which it contends for slots ([readyAt, until]). A queue turned away
	// contends until it stops asking. A queue that took a slot contends again
	// from when its own pace lets it ask, for about its timer's jitter: a
	// backlogged queue keeps its turn while it waits out its own interval,
	// instead of the slot going to whoever asks just before its timer fires.
	// That hold is at most one interval, which the slot schedule's one
	// interval of credit gives back. A queue that stops asking altogether is
	// forgotten after forgetQueue.
	queues map[any]*paceQueue
	// contenders is the subset of queues whose window hasn't closed, the
	// only ones a reservation looks at: with thousands of recently drained
	// queues, scanning all of them under the lock on every reservation would
	// cost real CPU. Idle queues are swept out of queues at most once a
	// second.
	contenders map[any]*paceQueue
	swept      time.Time
	// top is the highest rate advertised to a queue active in the last
	// activeWindow, and topWho that queue; kept up to date as queues ask, and
	// recomputed only when that queue lowers its rate or goes quiet.
	top    float64
	topWho any
}

type paceQueue struct {
	pass       float64
	stride     float64 // 1/weight at the queue's last slot
	advertised float64
	waiting    bool // turned away at since, and asking since
	since      time.Time
	asked      time.Time
	readyAt    time.Time
	until      time.Time
}

// forgetQueue is how long a queue that stopped asking keeps its pass. Its
// pass only matters while it is above vtime, which lasts about one of its
// own intervals, so this just bounds the map.
const forgetQueue = time.Minute

// activeWindow is how long a queue's advertised rate keeps counting toward
// the total after it last asked for a slot.
const activeWindow = 3 * time.Second

func newSharedPace(rate func() float64) *sharedPace {
	return &sharedPace{rate: rate, queues: make(map[any]*paceQueue), contenders: make(map[any]*paceQueue)}
}

// finiteRate is r if it is a usable rate, otherwise 0: a target answering
// X-Aqueduct-Rps: +Inf must not get a zero stride (every slot) or an
// unlimited total.
func finiteRate(r float64) float64 {
	if math.IsNaN(r) || math.IsInf(r, 0) || r <= 0 {
		return 0
	}
	return r
}

// reserve takes the next dispatch slot for who if one is free at now and
// it's who's turn, and returns 0. Otherwise it takes nothing and returns how
// long to wait before asking again. weight is the queue's share when queues
// contend: the rate the target advertised to it, or its own rate.
// advertised is that advertised rate, or 0 if it sent none; own is the
// queue's own interval between dispatches. A nil sharedPace, or a
// non-positive total, never limits.
func (s *sharedPace) reserve(who any, weight, advertised float64, own time.Duration, now time.Time) time.Duration {
	wait, _ := s.reserveSlot(who, weight, advertised, own, now)
	return wait
}

// reserveSlot is reserve that also returns, when it grants a slot, the time
// the slot was scheduled for, which is at most one interval before now. A
// queue counts its own pace from it rather than from now: counted from now,
// the timer jitter in every wait would add up, and a queue paced at the
// ceiling would fall further behind the slot schedule each turn until it
// missed its turns. With no limit the slot is now.
func (s *sharedPace) reserveSlot(who any, weight, advertised float64, own time.Duration, now time.Time) (time.Duration, time.Time) {
	if s == nil {
		return 0, now
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sweepLocked(now)
	me, ok := s.queues[who]
	if !ok {
		me = &paceQueue{}
		s.queues[who] = me
	}
	if now.After(me.until) {
		me.waiting = false
	}
	me.asked, me.advertised = now, finiteRate(advertised)
	s.noteAdvertisedLocked(who, me, now)

	r := s.totalLocked(now)
	if r <= 0 {
		return 0, now
	}
	// Same floor each queue already has: a pool whose members' reputations
	// collapsed must still get enough dispatches to recover.
	r = math.Max(r, MinRPS)
	interval := time.Duration(float64(time.Second) / r)
	weight = math.Max(finiteRate(weight), MinRPS)

	// A queue turned away asks again at least once per interval (its timer
	// is set to the wait returned here); one that doesn't has stopped.
	stale := 2*interval + 100*time.Millisecond

	// The step this queue's last slot added to its pass was for its weight
	// then. If the target has since changed its rate, redo that step at the
	// new weight: a tenant promoted from 0.5 to 600 would otherwise wait out
	// a 2-unit step, about 1,200 slots of a queue at 600, before its first
	// turn at the new rate.
	stride := 1 / weight
	if me.stride > 0 && me.stride != stride {
		me.pass += stride - me.stride
		me.stride = stride
	}
	me.pass = math.Max(me.pass, s.vtime)

	wait := func() time.Duration {
		if !me.waiting {
			me.waiting, me.since = true, now
		}
		me.readyAt, me.until = now, now.Add(stale)
		s.contenders[who] = me
		if s.granted {
			if next := s.last.Add(interval); now.Before(next) {
				return next.Sub(now)
			}
		}
		// The slot is free but belongs to a queue whose turn comes first;
		// ask again shortly, after it has had the chance to take it.
		return min(interval, 5*time.Millisecond)
	}

	// Carry at most one interval of unused credit, so a timer that fires a
	// little late doesn't lose throughput, but idle time doesn't bank a burst.
	start := now.Add(-interval)
	if s.granted {
		next := s.last.Add(interval)
		if now.Before(next) {
			return wait(), time.Time{}
		}
		if next.After(start) {
			start = next
		}
	}
	for k, q := range s.contenders {
		if now.After(q.until) {
			q.waiting = false
			delete(s.contenders, k)
			continue
		}
		if k == who || now.Before(q.readyAt) {
			continue
		}
		// A contender's pass may have fallen behind vtime while it waited
		// for its timer; it rejoins at vtime, like anyone else.
		pass := math.Max(q.pass, s.vtime)
		if pass < me.pass || (pass == me.pass && (!me.waiting || q.since.Before(me.since))) {
			return wait(), time.Time{}
		}
	}
	s.vtime = me.pass
	me.pass += stride
	me.stride = stride
	// It stays in line for its next turn, around when its own pace lets it
	// ask again.
	hold := min(own/4+2*time.Millisecond, interval)
	me.waiting, me.since = false, now
	me.readyAt, me.until = start.Add(own), start.Add(own+hold)
	s.contenders[who] = me
	s.last, s.granted = start, true
	return 0, start
}

// total is the target's current total rate: see sharedPace. Non-positive
// means unlimited.
func (s *sharedPace) total(now time.Time) float64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalLocked(now)
}

func (s *sharedPace) totalLocked(now time.Time) float64 {
	if s.followAdvertised {
		if s.topWho != nil {
			if q, ok := s.queues[s.topWho]; !ok || now.Sub(q.asked) > activeWindow {
				s.recomputeTopLocked(now)
			}
		}
		if s.top > 0 {
			return s.top
		}
	}
	return s.rate()
}

// noteAdvertisedLocked keeps top current after who asked advertising
// me.advertised.
func (s *sharedPace) noteAdvertisedLocked(who any, me *paceQueue, now time.Time) {
	switch {
	case me.advertised > 0 && me.advertised >= s.top:
		s.top, s.topWho = me.advertised, who
	case s.topWho == who:
		// The top queue lowered its rate or stopped advertising.
		s.recomputeTopLocked(now)
	}
}

func (s *sharedPace) recomputeTopLocked(now time.Time) {
	s.top, s.topWho = 0, nil
	for k, q := range s.queues {
		if q.advertised > s.top && now.Sub(q.asked) <= activeWindow {
			s.top, s.topWho = q.advertised, k
		}
	}
}

// sweepLocked forgets queues that stopped asking, at most once a second.
func (s *sharedPace) sweepLocked(now time.Time) {
	if now.Sub(s.swept) < time.Second {
		return
	}
	s.swept = now
	for k, q := range s.queues {
		if now.Sub(q.asked) > forgetQueue {
			delete(s.queues, k)
			delete(s.contenders, k)
		}
	}
}
