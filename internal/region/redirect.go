// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
)

const (
	defaultRedirectGateCooldownSeconds        = 500
	defaultRedirectExhaustedRetryAfterSeconds = 900
)

// DefaultTargetURL is the real, production URL builder for a
// redirect hop — the region-prefixed internal DNS form,
// <region>.$FLY_APP_NAME.internal, confirmed against Fly's own docs to
// resolve directly to that region's machines over the private 6PN
// network, bypassing the edge proxy entirely. Set as Aquifer.redirectTargetURL
// by NewAquifer; see that field's doc comment for why it's overridable.
func DefaultTargetURL(region string) string {
	appName := os.Getenv("FLY_APP_NAME")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return fmt.Sprintf("http://%s.%s.internal:%s/proxy", region, appName, port)
}

// Gate answers "is attempting cross-region redirect itself worth
// trying right now" — distinct from URLWorker's per-domain breaker, which
// answers "is this one target API domain healthy." Trips after a redirect
// tour finds no reachable alternate region at all; while tripped, /proxy
// skips redirect entirely and behaves exactly as it does without this
// feature — a safe, already-understood fallback state. Decided
// independently per-instance, no fleet coordination: each instance
// converges on the same conclusion within a request or two on its own.
type Gate struct {
	mu    sync.Mutex
	until time.Time
}

func (g *Gate) Open() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.until.IsZero() && time.Now().Before(g.until)
}

func (g *Gate) Trip(cooldown time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.until = time.Now().Add(cooldown)
}

func GateCooldown() time.Duration {
	return time.Duration(env.Int64("AQUIFER_REDIRECT_GATE_COOLDOWN_SECONDS", defaultRedirectGateCooldownSeconds)) * time.Second
}

// Pick deterministically selects one candidate for a given key —
// rendezvous/highest-random-weight hashing. Two independent callers
// computing this over the same candidates and key always get the same
// answer, with no coordination between them. Used to choose which region
// gets first crack at owning a job's durable queue if no region can
// dispatch it directly: two origins racing the same idempotent_key (e.g. a
// caller's own retry-after-timeout landing on a different region) converge
// on the same final target as long as their known-live-region sets agree
// — narrowing, not closing, the documented cross-origin duplicate-delivery
// gap (Aquifer's idempotency check itself stays per-instance).
func Pick(candidates []string, key string) string {
	if len(candidates) == 0 {
		return ""
	}
	best := candidates[0]
	bestScore := Score(best, key)
	for _, c := range candidates[1:] {
		if score := Score(c, key); score > bestScore {
			best = c
			bestScore = score
		}
	}
	return best
}

func Score(candidate, key string) string {
	sum := sha256.Sum256([]byte(candidate + ":" + key))
	// Comparing equal-length hex strings lexicographically matches
	// comparing the underlying bytes — no need to parse into a big.Int.
	return hex.EncodeToString(sum[:])
}

// OrderedCandidates builds the tour order for a job: live regions
// minus self and anything already visited, with the rendezvous-preferred
// region always first (see Pick, for cross-origin collision
// avoidance — see attemptRedirect's doc comment) and everything else in
// the order LiveRegions() already provided it in. For FlyAdapter that
// order is nearest-first by measured health-check round-trip time (see
// fly.go's pollOnce) — the only real proximity signal
// available, since Fly doesn't publish a region distance/latency table.
// Earlier versions of this function randomized the non-preferred
// candidates instead, reasoning that a fixed order risked every origin
// piling onto the same "second choice" region during a correlated outage
// — but phase 1 of attemptRedirect already probes every live candidate
// regardless of order (DirectOnly, non-committing), so order only affects
// which one wins the race to respond first, and biasing that toward the
// fastest region is strictly better than leaving it random.
func OrderedCandidates(live, visited []string, self, idempotentKey string) []string {
	visitedSet := make(map[string]bool, len(visited))
	for _, v := range visited {
		visitedSet[v] = true
	}

	var eligible []string
	for _, region := range live {
		if region == self || visitedSet[region] {
			continue
		}
		eligible = append(eligible, region)
	}
	if len(eligible) == 0 {
		return nil
	}

	preferred := Pick(eligible, idempotentKey)
	rest := make([]string, 0, len(eligible)-1)
	for _, region := range eligible {
		if region != preferred {
			rest = append(rest, region)
		}
	}

	return append([]string{preferred}, rest...)
}

// ExhaustedError is returned by Aquifer.AttemptDirect (via
// fallbackOutcome) when cross-region redirect is configured and was
// actually attempted (or is still within its post-exhaustion gate cooldown
// — see Gate) but no known-live region could serve the request
// directly or accept it into its own queue. Callers (the HTTP server)
// type-assert on this to build a 429 with a long Retry-After, the same way
// AdmissionRejectedError already does for admission control.
//
// This is deliberately NOT the same as "queue it locally instead" —
// queueing on total redirect exhaustion was never actually decided; the
// default is to fail loudly so a caller (or its own retry/alerting logic)
// knows the whole fleet is degraded, not just this one instance. A future
// per-deployment option to queue locally instead (e.g. for an Aquifer
// instance dedicated to a single customer, where "queue and eventually
// deliver" might be preferable to erroring) is a real possibility, just
// not the default and not built here.
type ExhaustedError struct {
	JobID string
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("job %s: cross-region redirect exhausted, no known-live region could help", e.JobID)
}

func ExhaustedRetryAfterSeconds() int64 {
	return env.Int64("AQUIFER_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS", defaultRedirectExhaustedRetryAfterSeconds)
}
