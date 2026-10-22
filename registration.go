// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
)

// External registration: opt-in, off by default. When AQUIFER_REGISTRY_URL
// is set, this instance periodically reports its own listening port to that
// URL so an external control plane (Canalis, or anything else someone
// builds against the same contract) can assign tenants to it. Generic on
// purpose, not Canalis-specific: this instance has no idea what's on the
// other end, it's just a webhook target, delivered through the exact same
// deliverWebhookSync path (retries, L8 signing) drain mode's ledger flush
// already uses.
//
// Only the port is reported, not a full address -- whatever's receiving
// this ping already sees the caller's real source IP on the connection
// itself, so a full self-reported address would just be redundant with
// that. This also makes it more portable than region-redirect's
// Fly-specific ".internal" DNS convention (internal/region/fly.go) -- this
// feature isn't Fly-specific at all.
//
// Disabled by default means exactly that: when AQUIFER_REGISTRY_URL is
// unset, no goroutine is ever started (see NewRegistry) -- not a loop that
// runs and no-ops.
const defaultRegistrationIntervalSeconds = 15

type RegistrationConfig struct {
	URL             string // AQUIFER_REGISTRY_URL -- presence enables this feature
	Port            string // PORT (same env var and "8080" default internal/region/fly.go already uses)
	IntervalSeconds int64  // AQUIFER_REGISTRY_INTERVAL_SECONDS
	// ReservedFor (AQUIFER_REGISTRY_RESERVED_FOR) tells the control plane
	// this instance is dedicated to one user_id and shouldn't be handed to
	// anyone else. Role (AQUIFER_REGISTRY_ROLE, e.g. "http" or "ws") says
	// which kind of traffic it's meant for. Both are reported as-is and
	// left out of the ping when unset; what they mean is the control
	// plane's call, same as everything else in this ping.
	ReservedFor string
	Role        string
	// Pools (AQUIFER_REGISTRY_POOLS, comma-separated, e.g. "gpu,eu") are
	// the pools this instance joins; a control plane like Canalis only
	// sends it requests for those pools. None means the default pool.
	// Every ping carries the current list, so changing it and restarting
	// is how an instance joins or leaves a pool.
	Pools []string
}

func (cfg RegistrationConfig) Enabled() bool {
	return cfg.URL != ""
}

// LoadRegistrationConfig reads the AQUIFER_REGISTRY_* env vars (plus the
// already-established PORT var, not a new one of its own).
func LoadRegistrationConfig() RegistrationConfig {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return RegistrationConfig{
		URL:             os.Getenv("AQUIFER_REGISTRY_URL"),
		Port:            port,
		IntervalSeconds: env.Int64("AQUIFER_REGISTRY_INTERVAL_SECONDS", defaultRegistrationIntervalSeconds),
		ReservedFor:     strings.TrimSpace(os.Getenv("AQUIFER_REGISTRY_RESERVED_FOR")),
		Role:            strings.ToLower(strings.TrimSpace(os.Getenv("AQUIFER_REGISTRY_ROLE"))),
		Pools:           parsePools(os.Getenv("AQUIFER_REGISTRY_POOLS")),
	}
}

// parsePools splits a comma-separated pool list, dropping blanks.
func parsePools(raw string) []string {
	var pools []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			pools = append(pools, p)
		}
	}
	return pools
}

// registrationLoop pings cfg.URL every IntervalSeconds with this instance's
// listening port. Only ever started when cfg.Enabled() -- see NewRegistry.
// Pings immediately on start rather than waiting a full interval, so a
// freshly-booted instance doesn't sit unregistered for the first tick.
func (r *Registry) registrationLoop() {
	ticker := time.NewTicker(time.Duration(r.registrationCfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	r.pingRegistry()
	for {
		select {
		case <-ticker.C:
			r.pingRegistry()
		case <-r.stop:
			return
		}
	}
}

func (r *Registry) pingRegistry() {
	state, _ := r.registrationState.Load().(LifecycleState)
	payload := r.registrationPayload(state)
	if !deliverWebhookSyncContext(r.ctx, r.registrationCfg.URL, payload, r.l8, r.metrics) && r.ctx.Err() == nil {
		log.Printf("registration: failed to report state to %s after retries", r.registrationCfg.URL)
	}
}

func (r *Registry) ReportOffline(ctx context.Context) {
	if r == nil || !r.registrationCfg.Enabled() {
		return
	}
	r.registrationState.Store(LifecycleStateOffline)
	if !deliverWebhookOnceContext(ctx, r.registrationCfg.URL, r.registrationPayload(LifecycleStateOffline), r.l8, r.metrics) {
		log.Printf("registration: failed to report offline state to %s", r.registrationCfg.URL)
	}
}

func (r *Registry) registrationPayload(state LifecycleState) map[string]any {
	payload := map[string]any{
		"port":        r.registrationCfg.Port,
		"state":       state,
		"reported_at": time.Now().UTC().Format(time.RFC3339),
	}
	if r.registrationCfg.ReservedFor != "" {
		payload["reserved_for"] = r.registrationCfg.ReservedFor
	}
	if r.registrationCfg.Role != "" {
		payload["role"] = r.registrationCfg.Role
	}
	if len(r.registrationCfg.Pools) > 0 {
		payload["pools"] = r.registrationCfg.Pools
	}
	// Drain mode's own position, separate from "state": drain mode's
	// "draining" means "finishing one tenant's burst", not "shutting down",
	// and "unassigned" is the control plane's cue to hand this instance to
	// someone else.
	if r.drainCfg.Enabled {
		payload["drain_state"] = r.DrainState()
	}
	// 0-100, same number as the X-Aqueduct-Fullness response header, so a
	// control plane knows how full an instance is before sending it
	// anything.
	if source, ok := r.fullnessSource.Load().(func() int); ok {
		payload["fullness"] = source()
	}
	return payload
}
