// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"testing"
	"time"
)

func TestJitterAddsSmallPositiveDelay(t *testing.T) {
	base := time.Second
	got := Jitter(base)

	if got < base {
		t.Fatalf("expected jittered duration to be at least %s, got %s", base, got)
	}
	if got > base+100*time.Millisecond {
		t.Fatalf("expected jittered duration to stay within 10%% of %s, got %s", base, got)
	}
}

func TestJitterLeavesNonPositiveDurationAlone(t *testing.T) {
	if got := Jitter(0); got != 0 {
		t.Fatalf("expected zero duration to remain zero, got %s", got)
	}
	if got := Jitter(-time.Second); got != -time.Second {
		t.Fatalf("expected negative duration to remain unchanged, got %s", got)
	}
}
