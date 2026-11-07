// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestSharedPaceHandsOutOneSlotPerInterval(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 }) // one slot per 100ms
	now := time.Unix(1000, 0)

	// One interval of credit is carried so a late timer doesn't lose
	// throughput, so two slots are free at once; the third must wait.
	for i := 0; i < 2; i++ {
		if wait := p.reserve("a", 10, 0, now); wait != 0 {
			t.Fatalf("reserve %d should get a slot, waited %v", i+1, wait)
		}
	}
	if wait := p.reserve("a", 10, 0, now); wait <= 0 || wait > 100*time.Millisecond {
		t.Fatalf("third reserve at the same instant should wait up to 100ms, got %v", wait)
	}
	if wait := p.reserve("a", 10, 0, now.Add(100*time.Millisecond)); wait != 0 {
		t.Fatalf("a reserve one interval later should get a slot, waited %v", wait)
	}
}

func TestSharedPaceDoesNotBankIdleTime(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 })
	now := time.Unix(1000, 0)
	p.reserve("a", 10, 0, now)

	// After a long idle stretch, at most one extra slot is available at once.
	later := now.Add(10 * time.Second)
	got := 0
	for i := 0; i < 5; i++ {
		if p.reserve("a", 10, 0, later) == 0 {
			got++
		}
	}
	if got > 2 {
		t.Fatalf("idle time banked a burst of %d slots; want at most 2", got)
	}
}

func TestSharedPaceWithoutARateNeverLimits(t *testing.T) {
	var nilPace *sharedPace
	if wait := nilPace.reserve("a", 10, 0, time.Now()); wait != 0 {
		t.Fatalf("nil pace limited: %v", wait)
	}
	p := newSharedPace(func() float64 { return 0 })
	for i := 0; i < 3; i++ {
		if wait := p.reserve("a", 10, 0, time.Now()); wait != 0 {
			t.Fatalf("zero-rate pace limited: %v", wait)
		}
	}
}

func TestSharedPaceUsesTheCurrentRate(t *testing.T) {
	rate := 1.0 // one slot per second
	p := newSharedPace(func() float64 { return rate })
	now := time.Unix(1000, 0)
	for p.reserve("a", 10, 0, now) == 0 {
	}
	if wait := p.reserve("a", 10, 0, now); wait < 500*time.Millisecond {
		t.Fatalf("at 1/s the next slot should be about 1s away, got %v", wait)
	}
	// Capacity rises tenfold: the wait must shrink right away, not stay at
	// the old one-second deadline.
	rate = 10
	if wait := p.reserve("a", 10, 0, now); wait > 100*time.Millisecond {
		t.Fatalf("after the rate rose to 10/s the wait should be at most 100ms, got %v", wait)
	}
}

func TestSharedPaceFloorsTheRateAtMinRPS(t *testing.T) {
	// A pool whose members' reputations collapsed reports a tiny capacity;
	// the shared pace must still allow MinRPS so the pool can recover.
	p := newSharedPace(func() float64 { return 0.001 })
	now := time.Unix(1000, 0)
	for p.reserve("a", 10, 0, now) == 0 {
	}
	if wait := p.reserve("a", 10, 0, now); wait > time.Duration(float64(time.Second)/MinRPS) {
		t.Fatalf("wait %v is longer than one MinRPS interval", wait)
	}
}

// A queue that asks constantly (new jobs keep waking it) must not take the
// slots from one that only asks when its timer fires.
func TestSharedPaceTakesTurnsBetweenAFrequentAndAPatientQueue(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 })
	now := time.Unix(1000, 0)
	end := now.Add(20 * time.Second)
	patientNext := now
	got := map[string]int{}
	for now.Before(end) {
		if p.reserve("busy", 10, 0, now) == 0 {
			got["busy"]++
		}
		if !now.Before(patientNext) {
			if wait := p.reserve("patient", 10, 0, now); wait == 0 {
				got["patient"]++
			} else {
				patientNext = now.Add(wait)
			}
		}
		now = now.Add(time.Millisecond)
	}
	total := got["busy"] + got["patient"]
	if total < 190 || total > 205 {
		t.Fatalf("expected about 200 slots in 20s at 10/s, got %d (%v)", total, got)
	}
	if share := float64(got["patient"]) / float64(total); share < 0.4 {
		t.Fatalf("the patient queue got %.0f%% of the slots (%v); turns should be about even", share*100, got)
	}
}

// contend runs every queue in weights as always backlogged (each asks again
// as soon as it's told to) for d at a 1 ms step, and returns the slots each
// one took.
func contend(p *sharedPace, weights map[string]float64, start time.Time, d time.Duration) map[string]int {
	got := map[string]int{}
	next := map[string]time.Time{}
	for now := start; now.Before(start.Add(d)); now = now.Add(time.Millisecond) {
		for name, w := range weights {
			if now.Before(next[name]) {
				continue
			}
			if wait := p.reserve(name, w, 0, now); wait == 0 {
				got[name]++
			} else {
				next[name] = now.Add(wait)
			}
		}
	}
	return got
}

func TestSharedPaceSplitsSlotsByAdvertisedRate(t *testing.T) {
	// Wildly unfair on purpose: the target advertises 900 to one tenant, 90
	// to another and 9 to a third, under a total of 20/s.
	p := newSharedPace(func() float64 { return 20 })
	got := contend(p, map[string]float64{"vip": 900, "standard": 90, "low": 9}, time.Unix(1000, 0), 100*time.Second)
	total := got["vip"] + got["standard"] + got["low"]
	if total < 1990 || total > 2005 {
		t.Fatalf("expected about 2000 slots in 100s at 20/s, got %d (%v)", total, got)
	}
	want := map[string]float64{"vip": 1800, "standard": 180, "low": 18}
	for name, w := range want {
		if g := float64(got[name]); math.Abs(g-w) > math.Max(w*0.05, 2) {
			t.Errorf("%s took %d slots, want about %.0f (%v)", name, got[name], w, got)
		}
	}
}

func TestSharedPaceEqualRatesGetEqualTurns(t *testing.T) {
	p := newSharedPace(func() float64 { return 20 })
	got := contend(p, map[string]float64{"a": 10, "b": 10, "c": 10, "d": 10}, time.Unix(1000, 0), 50*time.Second)
	for name, n := range got {
		if n < 240 || n > 260 {
			t.Errorf("%s took %d slots, want about 250 (%v)", name, n, got)
		}
	}
}

func TestSharedPaceFollowsAChangedRate(t *testing.T) {
	p := newSharedPace(func() float64 { return 20 })
	start := time.Unix(1000, 0)
	contend(p, map[string]float64{"a": 100, "b": 1}, start, 10*time.Second)
	// The target swaps them.
	got := contend(p, map[string]float64{"a": 1, "b": 100}, start.Add(10*time.Second), 20*time.Second)
	if got["b"] < 380 || got["a"] > 20 {
		t.Fatalf("after the swap b should take about 99%% of 400 slots, got %v", got)
	}
}

func TestSharedPaceIdleQueueRejoinsWithoutBankedTurns(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 })
	start := time.Unix(1000, 0)
	// a runs alone for 10 s; b was seen once, long ago.
	p.reserve("b", 10, 0, start)
	contend(p, map[string]float64{"a": 10}, start, 10*time.Second)
	// From here both contend with equal rates: b must not take a run of
	// slots for the time it was idle.
	got := contend(p, map[string]float64{"a": 10, "b": 10}, start.Add(10*time.Second), 2*time.Second)
	if got["b"] > got["a"]+2 {
		t.Fatalf("b caught up on idle time: %v", got)
	}
}

func TestSharedPaceSlowQueueLeavesItsShareToOthers(t *testing.T) {
	// A queue whose own pace is below its share (the target advertised it
	// little and it asks rarely) doesn't hold slots back: the rest go to the
	// busy queue, so the target still gets its full rate.
	p := newSharedPace(func() float64 { return 20 })
	now := time.Unix(1000, 0)
	end := now.Add(20 * time.Second)
	got := map[string]int{}
	slowNext, busyNext := now, now
	for ; now.Before(end); now = now.Add(time.Millisecond) {
		if !now.Before(slowNext) {
			if wait, slot := p.reserveSlot("slow", 1, time.Second, now); wait == 0 {
				got["slow"]++
				// Its own pace, 1/s, counted from its slot, and its timer
				// fires 10% late.
				slowNext = slot.Add(1100 * time.Millisecond)
			} else {
				slowNext = now.Add(wait)
			}
		}
		if !now.Before(busyNext) {
			if wait := p.reserve("busy", 1, 0, now); wait == 0 {
				got["busy"]++
			} else {
				busyNext = now.Add(wait)
			}
		}
	}
	if total := got["slow"] + got["busy"]; total < 395 || got["slow"] < 18 || got["slow"] > 21 {
		t.Fatalf("want about 400 slots with slow at its own 1/s, got %v", got)
	}
}

func TestSharedPaceKeepsAQueuesTurnWhileItWaitsOutItsOwnPace(t *testing.T) {
	// Each queue's own pace is capped at the ceiling, as in an account queue
	// whose configured rate is the target's: after every slot it sits out
	// its own interval (plus up to 20% timer jitter) before asking again.
	// The vip must not lose its turns to whoever asks in the meantime.
	const ceiling = 60.0
	p := newSharedPace(func() float64 { return ceiling })
	own := time.Second / time.Duration(ceiling)
	weights := map[string]float64{"vip": 600, "standard": 60, "low": 6}
	rng := rand.New(rand.NewPCG(1, 2))
	got := map[string]int{}
	next := map[string]time.Time{}
	start := time.Unix(1000, 0)
	for now := start; now.Before(start.Add(100 * time.Second)); now = now.Add(time.Millisecond) {
		for _, name := range []string{"low", "standard", "vip"} {
			if now.Before(next[name]) {
				continue
			}
			// Like an account queue: its own pace counts from the slot it
			// was given, and every timer fires up to 20% late.
			wait, slot := p.reserveSlot(name, weights[name], own, now)
			if wait == 0 {
				got[name]++
				wait = slot.Add(own).Sub(now)
			}
			next[name] = now.Add(wait + time.Duration(rng.Float64()*0.2*float64(wait)))
		}
	}
	total := got["vip"] + got["standard"] + got["low"]
	if total < 5700 {
		t.Fatalf("expected close to 6000 slots in 100s at 60/s, got %d (%v)", total, got)
	}
	for name, w := range weights {
		want := float64(total) * w / 666
		if g := float64(got[name]); math.Abs(g-want) > math.Max(want*0.05, 5) {
			t.Errorf("%s took %d slots, want about %.0f (%v)", name, got[name], want, got)
		}
	}
}
