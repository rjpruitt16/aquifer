// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rjpruitt16/aquifer/internal/region"
)

var (
	selfMachineIDOnce  sync.Once
	selfMachineIDValue string
)

// selfMachineID identifies this instance for OriginMachineID purposes only
// — it's not a security boundary (see region_redirect.go's DirectOnly/
// OriginMachineID doc comments), just a way for a receiving instance to
// know a request already belongs to someone else's redirect tour. FLY_MACHINE_ID
// on Fly; hostname, then a random ID, as local-dev fallbacks.
func selfMachineID() string {
	selfMachineIDOnce.Do(func() {
		if id := os.Getenv("FLY_MACHINE_ID"); id != "" {
			selfMachineIDValue = id
			return
		}
		if host, err := os.Hostname(); err == nil && host != "" {
			selfMachineIDValue = host
			return
		}
		selfMachineIDValue = generateID()
	})
	return selfMachineIDValue
}

// redirectResult distinguishes "this feature has nothing to say about this
// request" from "this feature was tried and every region it knows about
// failed" — attemptRedirect's caller (proxy.go's fallbackOutcome) needs
// that distinction because the two cases mean opposite things: the former
// falls through to today's existing local-fallback behavior, unchanged;
// the latter is a hard error (RedirectExhaustedError), not a silent local
// queue. See attemptRedirect's own doc comment for why.
type redirectResult int

const (
	redirectNotApplicable redirectResult = iota
	redirectSucceeded
	redirectExhausted
)

// attemptRedirect is called from AttemptDirect's local-fallback points
// (proxy.go) — never for a request already inside someone else's redirect
// tour, checked as this function's first line. It tours other known-live
// regions directly over Fly's private network before this instance falls
// back to its own local queue, mirroring what AttemptDirect already does
// for the local target API, just aimed at sibling Aquifer instances.
//
// Returns redirectNotApplicable when the feature isn't configured at all
// (no RegionAdapter ever set) or this request is itself someone else's
// redirect hop — the caller proceeds with its own existing local-fallback
// behavior, completely unchanged. Returns redirectExhausted when the
// feature IS configured but couldn't help — zero known-live regions right
// now, still within the post-exhaustion gate cooldown, or every candidate
// was actually tried and none succeeded — the caller must hard-error
// (RedirectExhaustedError), not silently fall back to a local queue.
func (a *Aquifer) attemptRedirect(ctx context.Context, job *Job, accountQueueHeader string, timeout time.Duration) (ProxyOutcome, redirectResult) {
	if job.OriginMachineID != "" {
		return ProxyOutcome{}, redirectNotApplicable
	}
	if a.regionAdapter == nil {
		return ProxyOutcome{}, redirectNotApplicable
	}

	adapter := a.regionAdapterOrDefault()

	if a.redirectGate.Open() {
		return ProxyOutcome{}, redirectExhausted
	}

	self := adapter.SelfRegion()
	live := adapter.LiveRegions()
	job.OriginMachineID = selfMachineID()
	job.OriginRegion = self

	candidates := region.OrderedCandidates(live, job.VisitedRegions, self, job.IdempotentKey)
	if len(candidates) == 0 {
		return ProxyOutcome{}, redirectExhausted
	}

	log.Printf("[region_redirect] job %s: origin=%s trying candidates %v", job.ID, self, candidates)

	anyReached := false

	// Phase 1: try every live candidate for a fast direct success only —
	// none are allowed to commit to their own local queue yet (DirectOnly),
	// so trying several in sequence can never leave the job durably
	// committed in more than one place.
	for _, region := range candidates {
		outcome, reached := a.tryRedirectHop(ctx, job, region, accountQueueHeader, timeout, true)
		if reached {
			anyReached = true
		}
		if outcome != nil {
			log.Printf("[region_redirect] job %s: %s succeeded directly", job.ID, region)
			a.store.DeleteJob(job.ID) // our own local row is now moot
			return *outcome, redirectSucceeded
		}
	}

	// Phase 2: nobody could succeed directly — make exactly one committing
	// call, to the same deterministically-chosen candidate every origin
	// computing this function over the same inputs would also pick first
	// (candidates[0] by construction — see orderedRedirectCandidates).
	final := candidates[0]
	outcome, reached := a.tryRedirectHop(ctx, job, final, accountQueueHeader, timeout, false)
	if reached {
		anyReached = true
	}
	if outcome != nil {
		log.Printf("[region_redirect] job %s: %s accepted it into its own queue", job.ID, final)
		a.store.DeleteJob(job.ID)
		return *outcome, redirectSucceeded
	}

	if anyReached {
		log.Printf("[region_redirect] job %s: every candidate tried, none could help — gating redirect for %s", job.ID, region.GateCooldown())
		a.redirectGate.Trip(region.GateCooldown())
	}
	return ProxyOutcome{}, redirectExhausted
}

// tryRedirectHop dials one candidate region directly over Fly's private 6PN
// network (<region>.$FLY_APP_NAME.internal — confirmed against Fly's own
// docs to resolve to that region's machines, bypassing the edge proxy
// entirely, no header needed for addressing) and interprets its response.
//
// A nil outcome with reached=true means the target gave a real, clean
// answer that just isn't usable (a DirectOnly rejection, or its own
// admission control saying no) — nothing was committed there, safe to try
// the next candidate. reached=false means the target couldn't be
// contacted at all (DNS, refused, timeout).
func (a *Aquifer) tryRedirectHop(ctx context.Context, job *Job, region, accountQueueHeader string, timeout time.Duration, directOnly bool) (*ProxyOutcome, bool) {
	hopReq := JobRequest{
		UserID:            job.UserID,
		IdempotentKey:     job.IdempotentKey,
		IdempotencyScope:  job.IdempotencyScope,
		MaxRetries:        &job.MaxRetries,
		WebhookMaxRetries: job.WebhookMaxRetries,
		URL:               job.URL,
		Method:            job.Method,
		Headers:           job.Headers,
		Body:              job.Body,
		WebhookURL:        job.WebhookURL,
		ExecuteBefore:     job.ExecuteBefore,
		OriginMachineID:   job.OriginMachineID,
		OriginRegion:      job.OriginRegion,
		VisitedRegions:    append(append([]string{}, job.VisitedRegions...), region),
		RerouteCount:      job.RerouteCount + 1,
		DirectOnly:        directOnly,
		AccountQueueMode:  accountQueueHeader,
	}
	body, err := json.Marshal(hopReq)
	if err != nil {
		return nil, false
	}

	url := a.redirectTargetURL(region)

	// directOnly=true calls can never produce a streaming response (that's
	// the whole point of the flag), so they're safe to bound with the same
	// short per-hop timeout AttemptDirect's own local attempt uses. The
	// single directOnly=false (committing) call is different: if it turns
	// into an SSE stream, that stream needs to live as long as the real
	// caller's own connection does, not a short per-hop dial deadline — so
	// it uses ctx directly, exactly like server.go's own local
	// streamEvents relies on r.Context()'s natural lifetime rather than an
	// artificial timeout.
	reqCtx := ctx
	if directOnly {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	httpReq.Header.Set("Content-Type", "application/json")
	// AccountQueueMode is always specified by the caller's own header
	// (server.go re-derives it fresh from the incoming request's headers,
	// never trusting a body field) -- without this, target's own
	// re-derivation finds nothing and silently drops the caller's explicit
	// per-tenant isolation choice, even though hopReq's AccountQueueMode
	// field carries it correctly in the body the whole way.
	if accountQueueHeader != "" {
		httpReq.Header.Set("X-Aquifer-Account-Queue", accountQueueHeader)
	}

	resp, err := (&http.Client{}).Do(httpReq)
	if err != nil {
		return nil, false
	}

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		// Target accepted it into its own durable queue — relay its
		// stream live. server.go's proxyJob owns closing resp.Body once
		// the stream ends. RerouteRegion tells it to announce this via a
		// "rerouted" event before relaying target's own stream.
		return &ProxyOutcome{RelayFrom: resp, RerouteRegion: region}, true
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Direct success via redirect isn't a held-open connection the way
		// RelayFrom is, but the region info is just as cheap to surface --
		// a response header, not a body/status change, so it can't affect
		// anything a caller is already parsing.
		respHeader := resp.Header
		respHeader.Set("X-Aquifer-Served-By-Region", region)
		return &ProxyOutcome{Direct: true, Status: resp.StatusCode, Header: respHeader, Body: respBody}, true
	}

	// A clean, well-formed rejection (DirectOnly's 503, an admission 429,
	// etc.) — reached the target, it gave a real answer, just not a usable
	// one.
	return nil, true
}
