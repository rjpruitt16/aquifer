// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"math"
	"sync"
	"time"
)

// Gate is where every account queue sending to one destination takes its
// turn. It holds the destination's totals: how many requests may be in
// flight (X-Aqueduct-Max-Concurrent) and how many may start per second
// (X-Aqueduct-Rps, capped by the configured or pool budget). A queue asks
// for a turn before each dispatch; when none is free it joins a round-robin
// line, and the next turn to free up goes to the queue at its head.
//
// Without it each queue applied the advertised values on its own, so a
// destination had to divide its capacity by X-Aqueduct-Active-Queues, and a
// share smaller than one request rounded up: 8 workers split 15 ways sent
// up to 15 at once, while the shares of queues with nothing waiting went
// unused.
type Gate struct {
	mu sync.Mutex
	// limit is the advertised in-flight total; 0 until the destination
	// sends one, and then nothing but each queue's own ceiling applies.
	limit int
	used  int
	// rate is the advertised start rate; 0 until the destination sends one.
	rate float64
	// ceiling is the configured or pool budget the rate never exceeds.
	ceiling func() float64
	next    time.Time
	line    []*GateTurn
	timer   *time.Timer
}

// GateTurn is one queue's place at the gate. Wake receives a signal when a
// turn has been handed to the queue while it was waiting in line.
type GateTurn struct {
	Wake    chan struct{}
	granted int
	inLine  bool
}

func NewGate(ceiling func() float64) *Gate {
	return &Gate{ceiling: ceiling}
}

func NewGateTurn() *GateTurn {
	return &GateTurn{Wake: make(chan struct{}, 1)}
}

// Acquire takes a turn for t: one handed to it while it waited, or a free
// one if nobody is waiting ahead of it. Otherwise t joins the line and
// Acquire returns false; t.Wake fires when a turn is handed over.
func (g *Gate) Acquire(t *GateTurn) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if t.granted > 0 {
		t.granted--
		return true
	}
	now := time.Now()
	if len(g.line) == 0 && g.free(now) {
		g.take(now)
		return true
	}
	if !t.inLine {
		t.inLine = true
		g.line = append(g.line, t)
	}
	g.arm(now)
	return false
}

// TryDirect takes a turn for a request outside any queue (a /proxy direct
// attempt) if one is free under the in-flight total, without joining the
// line. Direct attempts were never paced, so the rate doesn't apply.
func (g *Gate) TryDirect() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.line) == 0 && (g.limit == 0 || g.used < g.limit) {
		g.used++
		return true
	}
	return false
}

// Release ends a turn whose request has finished.
func (g *Gate) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used--
	g.handOut(time.Now())
}

// Leave gives back turns handed to t that it won't use, and takes it out of
// the line. A queue calls it when it has nothing left to send or stops.
func (g *Gate) Leave(t *GateTurn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used -= t.granted
	t.granted = 0
	if t.inLine {
		t.inLine = false
		for i, w := range g.line {
			if w == t {
				g.line = append(g.line[:i], g.line[i+1:]...)
				break
			}
		}
	}
	g.handOut(time.Now())
}

// Advertised applies the destination's totals from a response; nil leaves
// a value as it was. A rate on a failed response may lower the rate but not
// raise it, as for a queue's own pace: a proxy that stamps a fixed
// X-Aqueduct-Rps on its 503s would otherwise keep the gate wide open
// against a destination that is down.
func (g *Gate) Advertised(maxConcurrent *int, rps *float64, failed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if maxConcurrent != nil && *maxConcurrent > 0 {
		g.limit = *maxConcurrent
	}
	if rps != nil && *rps > 0 && (!failed || g.rate == 0 || *rps < g.rate) {
		g.rate = *rps
	}
	g.handOut(time.Now())
}

// InFlight is how many turns are taken: requests in flight plus turns
// handed to queues that haven't sent yet.
func (g *Gate) InFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.used
}

// Limit is the advertised in-flight total, or 0 if none.
func (g *Gate) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

// startRate is the rate turns start at: the advertised rate within the
// budget, or the budget alone. 0 means no rate limit. g.mu must be held.
func (g *Gate) startRate() float64 {
	r := math.Inf(1)
	if g.ceiling != nil {
		if c := g.ceiling(); c > 0 {
			r = c
		}
	}
	if g.rate > 0 {
		r = math.Min(r, g.rate)
	}
	if math.IsInf(r, 1) {
		return 0
	}
	return math.Max(r, MinRPS)
}

// free reports whether a turn can start now. g.mu must be held.
func (g *Gate) free(now time.Time) bool {
	return (g.limit == 0 || g.used < g.limit) && !now.Before(g.next)
}

// take starts a turn and moves the next start out by one interval. g.mu
// must be held.
func (g *Gate) take(now time.Time) {
	g.used++
	if r := g.startRate(); r > 0 {
		if g.next.Before(now) {
			g.next = now
		}
		g.next = g.next.Add(time.Duration(float64(time.Second) / r))
	}
}

// handOut gives free turns to the queues at the head of the line. g.mu must
// be held.
func (g *Gate) handOut(now time.Time) {
	for len(g.line) > 0 && g.free(now) {
		t := g.line[0]
		g.line = g.line[1:]
		t.inLine = false
		t.granted++
		g.take(now)
		select {
		case t.Wake <- struct{}{}:
		default:
		}
	}
	g.arm(now)
}

// arm sets a timer for when the rate next allows a turn, if someone is
// waiting only on the rate. g.mu must be held.
func (g *Gate) arm(now time.Time) {
	if len(g.line) == 0 || (g.limit > 0 && g.used >= g.limit) || !now.Before(g.next) {
		return
	}
	wait := g.next.Sub(now)
	if g.timer == nil {
		g.timer = time.AfterFunc(wait, func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.handOut(time.Now())
		})
		return
	}
	g.timer.Reset(wait)
}
