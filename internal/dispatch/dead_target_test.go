// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"errors"
	"fmt"
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

func TestOnlyTargetFailuresCountAgainstTargetHealth(t *testing.T) {
	w := &URLWorker{}
	w.ReportAnswer(0, localRequestError{errors.New(`net/http: invalid method "BAD METHOD"`)})
	if w.health.failingFor(time.Now()) != 0 {
		t.Fatal("a request that couldn't be built counted against the target")
	}
	w.ReportAnswer(503, nil)
	if w.health.failingFor(time.Now().Add(time.Second)) == 0 {
		t.Fatal("a 503 should count as the target failing")
	}
	w.ReportAnswer(200, nil)
	if w.health.failingFor(time.Now()) != 0 {
		t.Fatal("a direct success should mark the target up")
	}
	if !IsLocalRequestError(fmt.Errorf("wrapped: %w", localRequestError{errors.New("x")})) {
		t.Fatal("a wrapped local error is still local")
	}
}
