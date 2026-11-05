// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"
)

func TestSharedPaceHandsOutOneSlotPerInterval(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 }) // one slot per 100ms
	now := time.Unix(1000, 0)

	// One interval of credit is carried so a late timer doesn't lose
	// throughput, so two slots are free at once; the third must wait.
	for i := 0; i < 2; i++ {
		if wait := p.reserve("a", now); wait != 0 {
			t.Fatalf("reserve %d should get a slot, waited %v", i+1, wait)
		}
	}
	if wait := p.reserve("a", now); wait <= 0 || wait > 100*time.Millisecond {
		t.Fatalf("third reserve at the same instant should wait up to 100ms, got %v", wait)
	}
	if wait := p.reserve("a", now.Add(100*time.Millisecond)); wait != 0 {
		t.Fatalf("a reserve one interval later should get a slot, waited %v", wait)
	}
}

func TestSharedPaceDoesNotBankIdleTime(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 })
	now := time.Unix(1000, 0)
	p.reserve("a", now)

	// After a long idle stretch, at most one extra slot is available at once.
	later := now.Add(10 * time.Second)
	got := 0
	for i := 0; i < 5; i++ {
		if p.reserve("a", later) == 0 {
			got++
		}
	}
	if got > 2 {
		t.Fatalf("idle time banked a burst of %d slots; want at most 2", got)
	}
}

func TestSharedPaceWithoutARateNeverLimits(t *testing.T) {
	var nilPace *sharedPace
	if wait := nilPace.reserve("a", time.Now()); wait != 0 {
		t.Fatalf("nil pace limited: %v", wait)
	}
	p := newSharedPace(func() float64 { return 0 })
	for i := 0; i < 3; i++ {
		if wait := p.reserve("a", time.Now()); wait != 0 {
			t.Fatalf("zero-rate pace limited: %v", wait)
		}
	}
}

func TestSharedPaceUsesTheCurrentRate(t *testing.T) {
	rate := 1.0 // one slot per second
	p := newSharedPace(func() float64 { return rate })
	now := time.Unix(1000, 0)
	for p.reserve("a", now) == 0 {
	}
	if wait := p.reserve("a", now); wait < 500*time.Millisecond {
		t.Fatalf("at 1/s the next slot should be about 1s away, got %v", wait)
	}
	// Capacity rises tenfold: the wait must shrink right away, not stay at
	// the old one-second deadline.
	rate = 10
	if wait := p.reserve("a", now); wait > 100*time.Millisecond {
		t.Fatalf("after the rate rose to 10/s the wait should be at most 100ms, got %v", wait)
	}
}

func TestSharedPaceFloorsTheRateAtMinRPS(t *testing.T) {
	// A pool whose members' reputations collapsed reports a tiny capacity;
	// the shared pace must still allow MinRPS so the pool can recover.
	p := newSharedPace(func() float64 { return 0.001 })
	now := time.Unix(1000, 0)
	for p.reserve("a", now) == 0 {
	}
	if wait := p.reserve("a", now); wait > time.Duration(float64(time.Second)/MinRPS) {
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
		if p.reserve("busy", now) == 0 {
			got["busy"]++
		}
		if !now.Before(patientNext) {
			if wait := p.reserve("patient", now); wait == 0 {
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
