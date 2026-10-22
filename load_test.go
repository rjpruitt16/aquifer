// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// The HTTP user and WebSocket session capacities this instance reports to
// Canalis: one CPU budget, shared by whichever kind of traffic uses it.
// Mirrors Spillway's load_test.exs.

func idleReadings() loadReadings {
	return loadReadings{CPUs: 1, MaxSessions: 1000}
}

func TestIdleLoadAssumesTenJobsPerUserAndOneMessagePerSession(t *testing.T) {
	// 80% of one CPU / (10 / 750) = 60 users; / (1 / 3,000) = 2,400
	// sessions, capped at 1,000.
	got := computeLoad(idleReadings())
	want := Load{HTTPUsers: 0, HTTPCapacity: 60, WSSessions: 0, WSCapacity: 1000}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestLoadScalesWithCPUs(t *testing.T) {
	r := idleReadings()
	r.CPUs = 2
	if got := computeLoad(r).HTTPCapacity; got != 120 {
		t.Fatalf("expected 120 users on 2 CPUs, got %d", got)
	}
}

func TestIdleSessionsLeaveHTTPUsersTheCPUTheyDontUse(t *testing.T) {
	// The Canalis soak's shape: 100 open sessions moving almost nothing,
	// 50 users at 500 jobs/s, half of two CPUs busy.
	r := idleReadings()
	r.CPU, r.CPUs, r.JobRate, r.MessageRate, r.HTTPUsers, r.WSSessions = 0.5, 2, 500, 10, 50, 100
	got := computeLoad(r)
	if got.HTTPCapacity <= 50 || got.HTTPCapacity >= 100 {
		t.Fatalf("expected room for some more users, got %d", got.HTTPCapacity)
	}
	if got.WSCapacity != 1000 {
		t.Fatalf("expected the session cap, got %d", got.WSCapacity)
	}
}

func TestBusySessionsLeaveHTTPLessRoom(t *testing.T) {
	quiet := idleReadings()
	quiet.CPU, quiet.JobRate, quiet.HTTPUsers = 0.3, 100, 10
	busy := idleReadings()
	busy.CPU, busy.JobRate, busy.MessageRate, busy.HTTPUsers, busy.WSSessions = 0.7, 100, 1000, 10, 300

	q, b := computeLoad(quiet), computeLoad(busy)
	if b.HTTPCapacity >= q.HTTPCapacity {
		t.Fatalf("busy sessions should leave less room: %d vs %d", b.HTTPCapacity, q.HTTPCapacity)
	}
	if b.WSCapacity >= 1000 {
		t.Fatalf("busy sessions should be bounded by CPU, got %d", b.WSCapacity)
	}
}

func TestLoadNeverReportsLessThanItHas(t *testing.T) {
	r := idleReadings()
	r.CPU, r.JobRate, r.HTTPUsers, r.WSSessions = 1, 900, 80, 40
	got := computeLoad(r)
	if got.HTTPCapacity != 80 || got.WSCapacity != 40 {
		t.Fatalf("got %+v", got)
	}
}

func TestMemoryBoundsSessionsAtAbout90KBEach(t *testing.T) {
	r := idleReadings()
	r.MemoryLimitMB, r.MemoryMB = 1000, 990
	// 10 MB free / 90 KB = 113 sessions.
	if got := computeLoad(r).WSCapacity; got != 113 {
		t.Fatalf("expected 113 sessions, got %d", got)
	}
}

func TestLoadHeadersAreOnEveryResponse(t *testing.T) {
	routes := testServer(t, AdmissionLimits{}).Routes()
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	for _, name := range []string{"X-Aqueduct-Http-Users", "X-Aqueduct-Http-Capacity", "X-Aqueduct-WS-Sessions", "X-Aqueduct-WS-Capacity"} {
		if _, err := strconv.Atoi(rec.Header().Get(name)); err != nil {
			t.Fatalf("expected a number in %s, got %q", name, rec.Header().Get(name))
		}
	}
}

func TestRegistrationReportsLoad(t *testing.T) {
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	if _, ok := r.registrationPayload(LifecycleStateActive)["http_capacity"]; ok {
		t.Fatalf("expected no load before a source is wired")
	}

	r.SetLoadSource(func() Load { return Load{HTTPUsers: 3, HTTPCapacity: 60, WSSessions: 7, WSCapacity: 1000} })
	payload := r.registrationPayload(LifecycleStateActive)
	if payload["http_users"] != int64(3) || payload["http_capacity"] != int64(60) || payload["ws_sessions"] != int64(7) || payload["ws_capacity"] != int64(1000) {
		t.Fatalf("expected the load in the ping, got %v", payload)
	}
}

func TestAcceptedJobsAndActiveUsersAreCounted(t *testing.T) {
	tracker := newUserLoadTracker()
	a, b := &Job{UserID: "a"}, &Job{UserID: "b"}
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
