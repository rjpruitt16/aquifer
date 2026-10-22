// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"testing"
	"time"
)

func TestRendezvousPickIsDeterministic(t *testing.T) {
	candidates := []string{"iad", "lhr", "sjc", "nrt"}
	first := Pick(candidates, "some-idempotent-key")

	// Same inputs, different slice order -- must still pick the same
	// winner, since two independent origins won't necessarily enumerate
	// their known-live regions in the same order.
	reordered := []string{"nrt", "sjc", "lhr", "iad"}
	second := Pick(reordered, "some-idempotent-key")

	if first != second {
		t.Fatalf("expected the same pick regardless of input order, got %q vs %q", first, second)
	}

	// A different key can legitimately pick a different region -- just
	// confirm it's still deterministic for ITS OWN key.
	third := Pick(candidates, "some-idempotent-key")
	if third != first {
		t.Fatalf("expected repeat calls with the same inputs to agree, got %q vs %q", first, third)
	}
}

func TestRendezvousPickEmptyCandidates(t *testing.T) {
	if got := Pick(nil, "key"); got != "" {
		t.Fatalf("expected empty string for no candidates, got %q", got)
	}
}

func TestOrderedRedirectCandidatesExcludesSelfAndVisited(t *testing.T) {
	live := []string{"iad", "lhr", "sjc", "nrt"}
	visited := []string{"sjc"}
	candidates := OrderedCandidates(live, visited, "iad", "some-key")

	for _, c := range candidates {
		if c == "iad" {
			t.Fatalf("expected self (iad) excluded from candidates, got %v", candidates)
		}
		if c == "sjc" {
			t.Fatalf("expected already-visited (sjc) excluded from candidates, got %v", candidates)
		}
	}
	if len(candidates) != 2 {
		t.Fatalf("expected exactly 2 remaining candidates (lhr, nrt), got %v", candidates)
	}
}

func TestOrderedRedirectCandidatesPreferredIsAlwaysFirst(t *testing.T) {
	live := []string{"iad", "lhr", "sjc", "nrt", "fra"}
	preferred := Pick(live, "some-key")

	candidates := OrderedCandidates(live, nil, "", "some-key")
	if len(candidates) == 0 || candidates[0] != preferred {
		t.Fatalf("expected the rendezvous-preferred region (%q) first, got %v", preferred, candidates)
	}
}

func TestOrderedRedirectCandidatesEmptyWhenNothingEligible(t *testing.T) {
	candidates := OrderedCandidates([]string{"iad"}, nil, "iad", "key")
	if candidates != nil {
		t.Fatalf("expected nil when the only live region is self, got %v", candidates)
	}
}

func TestRedirectGateOpenClosedAndCooldown(t *testing.T) {
	g := &Gate{}
	if g.Open() {
		t.Fatalf("expected a fresh gate to be closed")
	}

	g.Trip(50 * time.Millisecond)
	if !g.Open() {
		t.Fatalf("expected the gate to be open immediately after tripping")
	}

	time.Sleep(60 * time.Millisecond)
	if g.Open() {
		t.Fatalf("expected the gate to close again after its cooldown elapsed")
	}
}
