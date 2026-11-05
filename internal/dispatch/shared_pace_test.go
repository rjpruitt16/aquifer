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
		if wait := p.reserve(now); wait != 0 {
			t.Fatalf("reserve %d should get a slot, waited %v", i+1, wait)
		}
	}
	if wait := p.reserve(now); wait <= 0 || wait > 100*time.Millisecond {
		t.Fatalf("third reserve at the same instant should wait up to 100ms, got %v", wait)
	}
	if wait := p.reserve(now.Add(100 * time.Millisecond)); wait != 0 {
		t.Fatalf("a reserve one interval later should get a slot, waited %v", wait)
	}
}

func TestSharedPaceDoesNotBankIdleTime(t *testing.T) {
	p := newSharedPace(func() float64 { return 10 })
	now := time.Unix(1000, 0)
	p.reserve(now)

	// After a long idle stretch, at most one extra slot is available at once.
	later := now.Add(10 * time.Second)
	got := 0
	for i := 0; i < 5; i++ {
		if p.reserve(later) == 0 {
			got++
		}
	}
	if got > 2 {
		t.Fatalf("idle time banked a burst of %d slots; want at most 2", got)
	}
}

func TestSharedPaceWithoutARateNeverLimits(t *testing.T) {
	var nilPace *sharedPace
	if wait := nilPace.reserve(time.Now()); wait != 0 {
		t.Fatalf("nil pace limited: %v", wait)
	}
	p := newSharedPace(func() float64 { return 0 })
	for i := 0; i < 3; i++ {
		if wait := p.reserve(time.Now()); wait != 0 {
			t.Fatalf("zero-rate pace limited: %v", wait)
		}
	}
}
