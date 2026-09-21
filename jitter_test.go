package aquifer

import (
	"testing"
	"time"
)

func TestWithJitterAddsSmallPositiveDelay(t *testing.T) {
	base := time.Second
	got := withJitter(base)

	if got < base {
		t.Fatalf("expected jittered duration to be at least %s, got %s", base, got)
	}
	if got > base+100*time.Millisecond {
		t.Fatalf("expected jittered duration to stay within 10%% of %s, got %s", base, got)
	}
}

func TestWithJitterLeavesNonPositiveDurationAlone(t *testing.T) {
	if got := withJitter(0); got != 0 {
		t.Fatalf("expected zero duration to remain zero, got %s", got)
	}
	if got := withJitter(-time.Second); got != -time.Second {
		t.Fatalf("expected negative duration to remain unchanged, got %s", got)
	}
}
