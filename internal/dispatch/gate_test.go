// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGateHoldsTheAdvertisedTotal(t *testing.T) {
	g := NewGate(nil)
	limit := 2
	g.Advertised(&limit, nil, false)

	a, b, c := NewGateTurn(), NewGateTurn(), NewGateTurn()
	if !g.Acquire(a) || !g.Acquire(b) {
		t.Fatal("expected two turns under a total of 2")
	}
	if g.Acquire(c) {
		t.Fatal("expected a third turn to wait at a total of 2")
	}
	g.Release()
	select {
	case <-c.Wake:
	case <-time.After(time.Second):
		t.Fatal("expected the waiting queue to be woken when a turn freed")
	}
	if !g.Acquire(c) {
		t.Fatal("expected the turn handed over to be usable")
	}
	if got := g.InFlight(); got != 2 {
		t.Fatalf("in flight = %d, want 2", got)
	}
}

func TestGateHandsTurnsOutInOrder(t *testing.T) {
	g := NewGate(nil)
	limit := 1
	g.Advertised(&limit, nil, false)

	first := NewGateTurn()
	if !g.Acquire(first) {
		t.Fatal("expected the first turn")
	}
	waiting := []*GateTurn{NewGateTurn(), NewGateTurn(), NewGateTurn()}
	for _, w := range waiting {
		if g.Acquire(w) {
			t.Fatal("expected to wait")
		}
	}
	// A queue that comes back for another turn goes to the back of the line.
	if g.Acquire(first) {
		t.Fatal("expected the first queue to wait behind the others")
	}
	for i, w := range append(waiting, first) {
		g.Release()
		if !g.Acquire(w) {
			t.Fatalf("turn %d: expected the queue at the head of the line to get the freed turn", i)
		}
	}
}

func TestGateLeaveGivesBackUnusedTurns(t *testing.T) {
	g := NewGate(nil)
	limit := 1
	g.Advertised(&limit, nil, false)

	a, b, c := NewGateTurn(), NewGateTurn(), NewGateTurn()
	g.Acquire(a)
	g.Acquire(b)
	g.Acquire(c)
	g.Release() // handed to b
	g.Leave(b)  // b has nothing left to send: the turn goes on to c
	if !g.Acquire(c) {
		t.Fatal("expected the turn b gave back to reach c")
	}
	if got := g.InFlight(); got != 1 {
		t.Fatalf("in flight = %d, want 1", got)
	}
}

func TestGateSharesTheRate(t *testing.T) {
	g := NewGate(func() float64 { return 1000 })
	rate := 20.0
	g.Advertised(nil, &rate, false)

	var started atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			turn := NewGateTurn()
			for {
				if g.Acquire(turn) {
					started.Add(1)
					g.Release()
					continue
				}
				select {
				case <-turn.Wake:
				case <-stop:
					return
				}
			}
		}()
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()
	// 20/s in total across all four, not 20/s each.
	if n := started.Load(); n < 15 || n > 25 {
		t.Fatalf("started %d in 1 s, want about 20", n)
	}
}

func TestGateRateFromAFailureOnlyLowers(t *testing.T) {
	g := NewGate(nil)
	ten, hundred := 10.0, 100.0
	g.Advertised(nil, &ten, false)
	g.Advertised(nil, &hundred, true)
	g.mu.Lock()
	got := g.rate
	g.mu.Unlock()
	if got != 10 {
		t.Fatalf("rate = %v after a failed response advertising more, want 10", got)
	}
}

func TestGateDirectAttemptsCountTowardTheTotal(t *testing.T) {
	g := NewGate(func() float64 { return 1 })
	limit := 1
	g.Advertised(&limit, nil, false)
	if !g.TryDirect() {
		t.Fatal("expected a direct attempt under the total")
	}
	if g.TryDirect() {
		t.Fatal("expected a second direct attempt to be refused at the total")
	}
	g.Release()
	// The rate is for queued dispatch; direct attempts were never paced.
	if !g.TryDirect() {
		t.Fatal("expected a direct attempt once the turn was released")
	}
}

// Fifteen tenants share a destination with 4 workers that advertises its
// total, X-Aqueduct-Max-Concurrent: 4. Once Aquifer has heard it, the
// destination never sees more than 4 at once, and keeps all 4 busy.
func TestTenantQueuesShareTheDestinationTotal(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_DESTINATION_LIMITS", "true")
	const workers, tenants, perTenant = 4, 15, 8
	var (
		inFlight, peak atomic.Int64
		done           atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		n := inFlight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set("X-Aqueduct-Max-Concurrent", fmt.Sprint(workers))
		w.WriteHeader(http.StatusOK)
		done.Add(1)
	}))
	t.Cleanup(srv.Close)

	r := testRegistry(t)
	r.mu.Lock()
	r.cfg.Defaults = RateConfig{RPS: 1000, MaxConcurrent: 32}
	r.mu.Unlock()

	// One job first, so Aquifer has heard the total before the rest arrive;
	// until then nothing but each queue's own ceiling applies.
	r.Enqueue(localJob("first", srv.URL), "")
	if !waitUntil(5*time.Second, func() bool { return done.Load() == 1 }) {
		t.Fatal("first job never completed")
	}
	done.Store(0)

	start := time.Now()
	for range perTenant {
		for tenant := range tenants {
			r.Enqueue(localJob(fmt.Sprintf("tenant-%d", tenant), srv.URL), "")
		}
	}
	if !waitUntil(10*time.Second, func() bool { return done.Load() == tenants*perTenant }) {
		t.Fatalf("completed %d of %d", done.Load(), tenants*perTenant)
	}
	if p := peak.Load(); p > workers {
		t.Fatalf("destination saw %d at once after advertising a total of %d", p, workers)
	}
	// 120 jobs of 20 ms on 4 workers take 0.6 s when every worker stays busy.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %v; the advertised total was left unused", elapsed)
	}
}

func TestSharedDestinationLimitsFlag(t *testing.T) {
	t.Setenv("AQUIFER_SHARED_DESTINATION_LIMITS", "")
	off := NewURLWorker("off.example", 10, 4, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer off.Stop()
	if off.gate != nil || off.done == nil {
		t.Fatalf("flag off: want the budget loop and no gate, got gate=%v done=%v", off.gate, off.done)
	}
	release, ok := off.TryDirect()
	if !ok {
		t.Fatalf("flag off: direct attempts must always go")
	}
	release()

	t.Setenv("AQUIFER_SHARED_DESTINATION_LIMITS", "true")
	on := NewURLWorker("on.example", 10, 4, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer on.Stop()
	if on.gate == nil || on.done != nil {
		t.Fatalf("flag on: want a gate and no budget loop")
	}
	limit := 1
	on.gate.Advertised(&limit, nil, false)
	release, ok = on.TryDirect()
	if !ok {
		t.Fatalf("flag on: first direct attempt should get the free turn")
	}
	if _, ok := on.TryDirect(); ok {
		t.Fatalf("flag on: second direct attempt should find the total taken")
	}
	release()
	release() // releasing twice must not free a second turn
	if got := on.gate.InFlight(); got != 0 {
		t.Fatalf("in flight after a double release = %d, want 0", got)
	}
	if _, ok := on.TryDirect(); !ok {
		t.Fatalf("flag on: the released turn should be free again")
	}
}
