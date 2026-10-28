// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"testing"
	"time"
)

// feed simulates one second of inserts at the given latency, then ticks.
func feed(c *InboundRateController, inserts int, latency time.Duration) InboundRateSnapshot {
	for i := 0; i < inserts; i++ {
		c.Observe("caller", latency, true)
	}
	c.tick(time.Now())
	return *c.Snapshot()
}

func TestInboundRateStartsAtPercentOfBenchmark(t *testing.T) {
	c := newInboundRateController(400, 40)
	if got := c.Snapshot().AdvertisedRPS; got != 160 {
		t.Fatalf("expected 40%% of 400 = 160, got %v", got)
	}
}

func TestInboundRateRampsFastBelowBenchmarkAndCarefullyAbove(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 160, time.Millisecond); snap.AdvertisedRPS != 240 || snap.LastAdjustment != "raise" {
		t.Fatalf("healthy and fully used below the benchmark should grow 1.5x to 240, got %+v", snap)
	}
	c.rate = 400
	if snap := feed(c, 400, time.Millisecond); snap.AdvertisedRPS != 420 {
		t.Fatalf("at the threshold (initially the benchmark) growth should slow to 5%% (420), got %+v", snap)
	}
}

func TestInboundRateCutsWhenInsertLatencyRises(t *testing.T) {
	c := newInboundRateController(400, 40)
	feed(c, 160, time.Millisecond) // establishes a ~1ms baseline
	before := c.Snapshot().AdvertisedRPS
	snap := feed(c, 200, 20*time.Millisecond)
	if snap.LastAdjustment != "cut: insert latency" || snap.AdvertisedRPS != round2(before*0.7) || snap.Cuts != 1 {
		t.Fatalf("expected a 30%% cut on a latency spike from %v, got %+v", before, snap)
	}
}

func TestInboundRateDoesNotInflateUnusedHeadroom(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 50, time.Millisecond); snap.AdvertisedRPS != 160 || snap.LastAdjustment != "hold" {
		t.Fatalf("callers using under 80%% of the rate must not raise it, got %+v", snap)
	}
}

func TestInboundRateHoldsWhenCallersOvershoot(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 300, time.Millisecond); snap.LastAdjustment != "hold: callers over advertised rate" || snap.AdvertisedRPS != 160 {
		t.Fatalf("callers lagging a lower rate must not spiral it down, got %+v", snap)
	}
}

func TestInboundRateCutsOncePerCooldownAndLearnsTheKnee(t *testing.T) {
	c := newInboundRateController(400, 40)
	c.rate = 500
	feed(c, 400, time.Millisecond)
	c.rate = 500
	first := feed(c, 450, 20*time.Millisecond)
	if first.AdvertisedRPS != 350 || c.threshold != 450 {
		t.Fatalf("expected a cut to 350 with a learned knee of 450, got %+v threshold=%v", first, c.threshold)
	}
	for i := 0; i < 9; i++ {
		if snap := feed(c, 350, 20*time.Millisecond); snap.AdvertisedRPS != 350 {
			t.Fatalf("tick %d: within one congestion episode it must hold after the first cut, got %+v", i, snap)
		}
	}
	if snap := feed(c, 350, 20*time.Millisecond); snap.AdvertisedRPS != 245 {
		t.Fatalf("an episode lasting 10 ticks cuts again, got %+v", snap)
	}
}

func TestInboundRateSplitsAcrossActiveCallers(t *testing.T) {
	c := newInboundRateController(400, 40)
	for _, id := range []string{"a", "b", "c", "d"} {
		c.Observe(id, time.Millisecond, true)
	}
	if got := c.AdvertisedRPS("a"); got != 40 {
		t.Fatalf("160 split over 4 active callers should be 40 each, got %v", got)
	}
}

func TestInboundRateCascadeKeepsTheFirstKnee(t *testing.T) {
	c := newInboundRateController(400, 40)
	c.rate = 500
	feed(c, 400, time.Millisecond)
	c.rate = 500
	feed(c, 450, 20*time.Millisecond) // first cut: knee 450
	for i := 0; i < 6; i++ {
		feed(c, 300, 20*time.Millisecond) // still congested: more cuts
	}
	if c.threshold != 450 {
		t.Fatalf("cuts during one congestion episode must keep the first knee (450), got %v", c.threshold)
	}
}

func TestInboundRateBacklogGrowthHoldsThenCuts(t *testing.T) {
	c := newInboundRateController(400, 40)
	var backlog int64
	c.SetBacklogSource(func() int64 { return backlog })

	feed(c, 160, time.Millisecond) // first reading sets the baseline backlog
	start := c.Snapshot().AdvertisedRPS
	for i, want := range []string{"hold: backlog growing", "hold: backlog growing", "cut: backlog growing"} {
		backlog += 500 // dispatch falling behind while inserts stay fast
		if snap := feed(c, 160, time.Millisecond); snap.LastAdjustment != want {
			t.Fatalf("tick %d: expected %q, got %+v", i, want, snap)
		}
	}
	if got := c.Snapshot().AdvertisedRPS; got >= start {
		t.Fatalf("a backlog growing three ticks in a row must cut the rate, still %v (was %v)", got, start)
	}
}

func TestInboundRateSteadyBacklogAllowsRaises(t *testing.T) {
	c := newInboundRateController(400, 40)
	c.SetBacklogSource(func() int64 { return 50 })
	feed(c, 160, time.Millisecond)
	if snap := feed(c, 240, time.Millisecond); snap.LastAdjustment != "raise" {
		t.Fatalf("a steady backlog must not block raises, got %+v", snap)
	}
}

// Inserts don't wait for the disk, so they stay fast while it falls behind;
// a kill then loses everything acknowledged since the last sync. Canalis
// soak repro, 2026-10-27: jobs acknowledged 16-37s before a kill were lost.
// The controller has to slow callers on the disk's lag, not just latency.
func TestInboundRateCutsWhenTheDiskFallsBehind(t *testing.T) {
	c := newInboundRateController(400, 40)
	lag := 100 * time.Millisecond
	c.SetSyncLagSource(func() time.Duration { return lag }, 100*time.Millisecond)
	feed(c, 160, time.Millisecond)
	before := c.Snapshot().AdvertisedRPS

	lag = 2 * time.Second
	snap := feed(c, 200, time.Millisecond)
	if snap.LastAdjustment != "cut: disk behind" || snap.AdvertisedRPS != round2(before*0.7) || snap.SyncLagMs != 2000 {
		t.Fatalf("expected a 30%% cut from %v with fast inserts but a 2s sync lag, got %+v", before, snap)
	}

	// Caught up again: no more cuts, and it grows once callers use the rate.
	lag = 100 * time.Millisecond
	for range inboundCutCooldownTicks {
		feed(c, 0, time.Millisecond)
	}
	if snap := feed(c, int(snap.AdvertisedRPS), time.Millisecond); snap.LastAdjustment != "raise" {
		t.Fatalf("a disk that caught up should let the rate grow again, got %+v", snap)
	}
}

// With a longer flush interval, the gap between two healthy syncs is longer
// than 500ms; that alone isn't the disk falling behind.
func TestInboundRateAllowsForALongFlushInterval(t *testing.T) {
	c := newInboundRateController(400, 40)
	lag := 1500 * time.Millisecond
	c.SetSyncLagSource(func() time.Duration { return lag }, 2*time.Second)
	feed(c, 160, time.Millisecond)
	if snap := feed(c, 240, time.Millisecond); snap.LastAdjustment != "raise" {
		t.Fatalf("1.5s between 2s flushes is healthy, got %+v", snap)
	}
	lag = 5 * time.Second
	if snap := feed(c, 200, time.Millisecond); snap.LastAdjustment != "cut: disk behind" {
		t.Fatalf("over two intervals behind is the disk falling behind, got %+v", snap)
	}
}
