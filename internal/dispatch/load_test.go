// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"

	"github.com/rjpruitt16/aquifer/internal/cluster"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// The HTTP user and WebSocket session capacities this instance reports to
// Canalis: one CPU budget, shared by whichever kind of traffic uses it.
// Mirrors Spillway's load_test.exs.

func TestRegistrationReportsLoad(t *testing.T) {
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	if _, ok := r.registrationPayload(cluster.StateActive)["http_capacity"]; ok {
		t.Fatalf("expected no load before a source is wired")
	}

	r.SetLoadSource(func() Load { return Load{HTTPUsers: 3, HTTPCapacity: 60, WSSessions: 7, WSCapacity: 1000} })
	payload := r.registrationPayload(cluster.StateActive)
	if payload["http_users"] != int64(3) || payload["http_capacity"] != int64(60) || payload["ws_sessions"] != int64(7) || payload["ws_capacity"] != int64(1000) {
		t.Fatalf("expected the load in the ping, got %v", payload)
	}
}

func TestAcceptedJobsAndActiveUsersAreCounted(t *testing.T) {
	tracker := newUserLoadTracker()
	a, b := &jobs.Job{UserID: "a"}, &jobs.Job{UserID: "b"}
	tracker.add(a)
	tracker.add(b)
	tracker.done(a)
	if got := tracker.accepted.Load(); got != 2 {
		t.Fatalf("expected 2 accepted, got %d", got)
	}
	if got := tracker.active(); got != 1 {
		t.Fatalf("expected 1 active user, got %d", got)
	}
}
