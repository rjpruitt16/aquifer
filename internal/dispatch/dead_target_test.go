// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"
)

func TestTargetHealthResetsOnAnyAnswer(t *testing.T) {
	var h targetHealth
	now := time.Now()
	if h.failingFor(now) != 0 {
		t.Fatal("a target with no failures isn't failing")
	}
	h.failed(now)
	h.failed(now.Add(5 * time.Second)) // the first failure is when it started
	if got := h.failingFor(now.Add(10 * time.Second)); got < 9*time.Second {
		t.Fatalf("failingFor = %v, want about 10s from the first failure", got)
	}
	h.answered()
	if h.failingFor(now.Add(11*time.Second)) != 0 {
		t.Fatal("one answer means the target is up again")
	}
}
