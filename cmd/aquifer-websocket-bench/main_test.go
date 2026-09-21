package main

import (
	"testing"
	"time"
)

func TestPercentileMilliseconds(t *testing.T) {
	values := []time.Duration{50 * time.Millisecond, 10 * time.Millisecond, 30 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	if got := percentileMilliseconds(values, 0.50); got != 30 {
		t.Fatalf("p50 = %v, want 30", got)
	}
	if got := percentileMilliseconds(values, 0.95); got != 40 {
		t.Fatalf("p95 = %v, want 40", got)
	}
	if got := percentileMilliseconds(nil, 0.99); got != 0 {
		t.Fatalf("empty percentile = %v, want 0", got)
	}
}
