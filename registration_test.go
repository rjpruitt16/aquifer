// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestRegistrationDisabledByDefault proves the opt-in: with no
// AQUIFER_REGISTRY_URL set, the feature resolves to disabled and
// NewRegistry never starts the ping goroutine (mirrors
// TestDrainDisabledNeverTransitionsState's "prove nothing happens" shape).
func TestRegistrationDisabledByDefault(t *testing.T) {
	t.Setenv("AQUIFER_REGISTRY_URL", "")

	cfg := LoadRegistrationConfig()
	if cfg.Enabled() {
		t.Fatalf("expected registration disabled by default")
	}
}

// TestRegistrationPortDefaultsTo8080 confirms this reuses the exact same
// PORT env var and default region_adapter_fly.go already established,
// rather than introducing a second convention for the same fact.
func TestRegistrationPortDefaultsTo8080(t *testing.T) {
	t.Setenv("PORT", "")

	cfg := LoadRegistrationConfig()
	if cfg.Port != "8080" {
		t.Fatalf("expected default port 8080, got %q", cfg.Port)
	}
}

// TestRegistrationPingsImmediatelyAndOnInterval drives the real goroutine
// (not a direct pingRegistry call) end to end: a freshly constructed
// Registry should ping once immediately (not wait a full interval to
// appear for the first time) and again on the next tick, carrying this
// instance's actual listening port -- not a self-reported address, since
// whatever receives the ping already sees the real source IP itself.
func TestRegistrationPingsImmediatelyAndOnInterval(t *testing.T) {
	var pings atomic.Int64
	var lastPort atomic.Value

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if port, ok := body["port"].(string); ok {
			lastPort.Store(port)
		}
		pings.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("PORT", "9999")
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "1")

	r := drainTestRegistry(t, NoopMetricsAdapter{})
	if !r.registrationCfg.Enabled() {
		t.Fatalf("expected registration enabled")
	}

	// Immediate first ping -- should already have arrived without waiting
	// for a full interval.
	deadline := time.Now().Add(2 * time.Second)
	for pings.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := pings.Load(); got < 1 {
		t.Fatalf("expected an immediate ping on start, got %d", got)
	}
	if port, _ := lastPort.Load().(string); port != "9999" {
		t.Fatalf("expected reported port %q, got %q", "9999", port)
	}

	// Second ping on the next tick, proving this is a recurring loop, not
	// a one-shot registration.
	deadline = time.Now().Add(3 * time.Second)
	for pings.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := pings.Load(); got < 2 {
		t.Fatalf("expected at least 2 pings within two intervals, got %d", got)
	}
}

func TestRegistrationReportsShutdownLifecycle(t *testing.T) {
	states := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if state, ok := body["state"].(string); ok {
			states <- state
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "3600")
	r := drainTestRegistry(t, NoopMetricsAdapter{})

	waitForRegistrationState(t, states, string(LifecycleStateActive))
	r.BeginDrain()
	waitForRegistrationState(t, states, string(LifecycleStateDraining))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r.ReportOffline(ctx)
	waitForRegistrationState(t, states, string(LifecycleStateOffline))
}

func waitForRegistrationState(t *testing.T, states <-chan string, expected string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case state := <-states:
			if state == expected {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for registration state %q", expected)
		}
	}
}

// pingBodies collects every registration ping's decoded body.
func pingBodies(t *testing.T) (*httptest.Server, <-chan map[string]any) {
	t.Helper()
	bodies := make(chan map[string]any, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		bodies <- body
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, bodies
}

func nextPing(t *testing.T, bodies <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case body := <-bodies:
		return body
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for a registration ping")
		return nil
	}
}

// TestRegistrationReportsReservedForAndRole: a control plane like Canalis
// needs to know an instance is dedicated to one user, or meant for one
// kind of traffic, before it hands the instance to anyone.
func TestRegistrationReportsReservedForAndRole(t *testing.T) {
	srv, bodies := pingBodies(t)
	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "3600")
	t.Setenv("AQUIFER_REGISTRY_RESERVED_FOR", " acme ")
	t.Setenv("AQUIFER_REGISTRY_ROLE", "WS")
	drainTestRegistry(t, NoopMetricsAdapter{})

	body := nextPing(t, bodies)
	if body["reserved_for"] != "acme" {
		t.Fatalf("expected reserved_for %q, got %v", "acme", body["reserved_for"])
	}
	if body["role"] != "ws" {
		t.Fatalf("expected role %q, got %v", "ws", body["role"])
	}
}

// TestRegistrationReportsPools: the pools this instance joins, so a control
// plane only sends it requests for those pools.
func TestRegistrationReportsPools(t *testing.T) {
	srv, bodies := pingBodies(t)
	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "3600")
	t.Setenv("AQUIFER_REGISTRY_POOLS", " gpu, ,eu ")
	drainTestRegistry(t, NoopMetricsAdapter{})

	body := nextPing(t, bodies)
	pools, ok := body["pools"].([]any)
	if !ok || len(pools) != 2 || pools[0] != "gpu" || pools[1] != "eu" {
		t.Fatalf("expected pools [gpu eu], got %v", body["pools"])
	}
}

// TestRegistrationOmitsOptionalFieldsByDefault keeps the ping exactly as it
// was for anyone not using these features.
func TestRegistrationOmitsOptionalFieldsByDefault(t *testing.T) {
	srv, bodies := pingBodies(t)
	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "3600")
	t.Setenv("AQUIFER_REGISTRY_RESERVED_FOR", "")
	t.Setenv("AQUIFER_REGISTRY_ROLE", "")
	t.Setenv("AQUIFER_REGISTRY_POOLS", "")
	drainTestRegistry(t, NoopMetricsAdapter{})

	body := nextPing(t, bodies)
	for _, key := range []string{"reserved_for", "role", "drain_state", "pools"} {
		if _, ok := body[key]; ok {
			t.Fatalf("expected no %q in a default ping, got %v", key, body)
		}
	}
}

// TestRegistrationPingsAsSoonAsDrainModeIsUnassigned: a drain-mode
// instance that just freed itself shouldn't sit idle until the next
// heartbeat before the control plane can hand it to another tenant.
func TestRegistrationPingsAsSoonAsDrainModeIsUnassigned(t *testing.T) {
	srv, bodies := pingBodies(t)
	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "3600")
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	nextPing(t, bodies) // the immediate startup ping

	r.drainCfg.Enabled = true
	r.setDrainState(DrainStateUnassigned)

	body := nextPing(t, bodies)
	if body["drain_state"] != string(DrainStateUnassigned) {
		t.Fatalf("expected drain_state %q, got %v", DrainStateUnassigned, body["drain_state"])
	}
	if body["state"] != string(LifecycleStateActive) {
		t.Fatalf("drain mode's state must not change the lifecycle state, got %v", body["state"])
	}

	// Already unassigned: no second extra ping for a repeat transition.
	r.setDrainState(DrainStateUnassigned)
	select {
	case body := <-bodies:
		t.Fatalf("expected no ping for a repeated unassigned state, got %v", body)
	case <-time.After(300 * time.Millisecond):
	}
}
