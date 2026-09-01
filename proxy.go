package aquifer

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	defaultProxyBreakerRetryMultiplier   = 3
	defaultProxyBreakerCooldownSeconds   = 5
	defaultProxyDirectAttemptTimeoutSecs = 3
)

// ProxyOutcome is AttemptDirect's result. Exactly one of the following
// holds: Err is set (validation/admission failure, same contract as
// Enqueue); Duplicate is true (ExistingJob holds the prior job); or Job is
// set, with Direct indicating whether it was completed synchronously
// (Status/Header/Body then valid) or needs the caller to Dispatch it and
// stream the result the normal way.
type ProxyOutcome struct {
	Err         error
	Duplicate   bool
	ExistingJob *Job
	Job         *Job
	Direct      bool
	Status      int
	Header      http.Header
	Body        []byte

	// FallbackReason and FallbackStatus are set whenever Job is set but
	// Direct is false — a short, stable label for why a direct attempt
	// wasn't completed, surfaced to the caller as a proxy_fallback SSE
	// event before the normal queued/dispatching/terminal sequence, so a
	// client watching the stream (a browser, an agent with no server of
	// its own to explain this some other way) knows it's in the queue
	// because something specific happened, not just "queued" with no
	// context. FallbackStatus is the upstream's real status code when one
	// was actually received (0 for a skipped or timed-out attempt).
	FallbackReason string
	FallbackStatus int

	// RelayFrom is set when this request was redirected to another region
	// (region_redirect.go) and that region accepted it into its own
	// durable queue rather than completing directly — the caller
	// (server.go's proxyJob) should relay every SSE event read from this
	// response body onto the original caller's connection in real time,
	// rather than subscribing to a local job's events the normal way.
	// Job/Direct/FallbackReason are meaningless in this case; the real job
	// now lives on the target region under its own ID — the caller learns
	// it from the relayed stream itself, the same place it always would
	// for any fallback. The caller owns closing RelayFrom.Body once the
	// stream ends. RerouteRegion is always set alongside it — which region
	// actually ended up owning the job — so the caller can tell the client
	// via a synthetic "rerouted" event before relaying, same rationale as
	// FallbackReason/proxy_fallback: a client with no server of its own to
	// explain this (a browser, an agent) shouldn't have to wonder why its
	// connection is still open or where the response actually came from.
	RelayFrom     *http.Response
	RerouteRegion string
}

// AttemptDirect is proxy mode's entry point: persist the job (same
// idempotency/admission path Enqueue uses), then — for URL-based jobs only,
// see the PoolID check below — try dispatching it directly and
// synchronously before ever touching the durable queue. A direct attempt
// is skipped entirely if this job's target already has its circuit breaker
// open (see URLWorker.BreakerOpen), so a known-bad upstream doesn't cost
// every subsequent request the latency of a doomed attempt.
func (a *Aquifer) AttemptDirect(ctx context.Context, req JobRequest, timeout time.Duration) ProxyOutcome {
	job, duplicate, err := a.PrepareJob(req)
	if err != nil {
		return ProxyOutcome{Err: err}
	}
	if duplicate != nil {
		return ProxyOutcome{Duplicate: true, ExistingJob: a.store.GetJob(duplicate.JobID)}
	}

	// Pool-routed jobs have no single canonical upstream to try directly —
	// pool routing's whole premise (spread across members, one might be
	// unhealthy) is in tension with "there's one upstream, try it". Fall
	// straight back to queue+stream, same as any other job the caller
	// couldn't attempt directly.
	if job.PoolID != "" {
		// Pool-routed jobs also skip cross-region redirect consideration:
		// a redirected target instance has no reason to share the same
		// pool membership, so there's no single canonical destination to
		// even try there either.
		return a.fallbackOutcome(ctx, req, job, "pool_routed", 0, timeout, false)
	}

	worker := a.registry.workerFor(job)
	if worker.BreakerOpen() || worker.QueueActive() {
		return a.fallbackOutcome(ctx, req, job, "domain_degraded", 0, timeout, true)
	}

	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	counts := a.store.Counts()
	resp, attemptErr := makeRequest(attemptCtx, job, job.URL, counts.TotalJobs, counts.QueueDepth, 0, a.l8)
	if attemptErr != nil {
		return a.fallbackOutcome(ctx, req, job, "upstream_unreachable", 0, timeout, true)
	}
	defer resp.Body.Close()

	if overloadCooldown, overloaded := isOverloadSignal(resp); overloaded {
		worker.TripBreaker(overloadCooldown)
		return a.fallbackOutcome(ctx, req, job, "upstream_overloaded", resp.StatusCode, timeout, true)
	}

	// The upstream can proactively ask to be routed through the durable
	// queue going forward — X-Aqueduct-Queue-Active: true — even on an
	// otherwise-healthy response, e.g. "I'm nearing capacity, stop firing
	// directly at me." Unlike isOverloadSignal, this response is still a
	// real, valid answer already in hand: it's relayed to the caller as
	// normal below, only future requests to this domain start queuing.
	if pacingHeader(resp.Header, "Queue-Active") == "true" {
		worker.TripBreaker(breakerCooldown(resp.Header))
	}

	body, _ := io.ReadAll(resp.Body)
	a.store.UpdateStatus(job.ID, StatusCompleted)
	a.broker.Publish(job.ID, SSEEvent{Event: "completed", Data: map[string]any{
		"job_id": job.ID, "response_status": resp.StatusCode, "body": string(body),
	}})
	if job.WebhookURL != "" {
		a.registry.EnqueueWebhook(job.ID, job.UserID, job.WebhookURL, map[string]any{
			"job_id": job.ID, "status": "completed", "response_status": resp.StatusCode, "body": string(body),
		})
	}

	return ProxyOutcome{Job: job, Direct: true, Status: resp.StatusCode, Header: resp.Header, Body: body}
}

// fallbackOutcome builds AttemptDirect's local-fallback result — but tries
// cross-region redirect first (region_redirect.go) when tryRedirect is
// true, so a caller only ever falls back to its own local queue after a
// sibling region genuinely couldn't take the job either (or redirect isn't
// configured/available/gated at all, in which case attemptRedirect returns
// immediately with no side effects). tryRedirect is false only for
// pool-routed jobs, which have no single canonical destination for a
// redirected target to even try.
//
// When req.DirectOnly is set (only ever true on an internal cross-region
// redirect hop — a real caller never sets it), this instance has been
// explicitly told not to commit to its own local queue: it deletes the job
// row PrepareJob already inserted, mirroring exactly how PrepareJob itself
// cleans up an admission-rejected job, so a DirectOnly hop that couldn't
// succeed doesn't leave a ghost "queued" row that's never actually
// dispatched. Without this, an origin's redirect tour trying several
// regions in sequence could leave the SAME job durably committed in more
// than one place — the earlier candidate queuing locally while the tour
// moves on to try the next. (attemptRedirect itself never runs for a
// DirectOnly request — job.OriginMachineID is already set by construction,
// which is attemptRedirect's own first check.)
func (a *Aquifer) fallbackOutcome(ctx context.Context, req JobRequest, job *Job, reason string, status int, timeout time.Duration, tryRedirect bool) ProxyOutcome {
	if tryRedirect {
		outcome, result := a.attemptRedirect(ctx, job, req.AccountQueueMode, timeout)
		switch result {
		case redirectSucceeded:
			return outcome
		case redirectExhausted:
			// Nothing was committed anywhere -- this instance's own row is
			// moot, same as the DirectOnly-rejection cleanup below.
			a.store.DeleteJob(job.ID)
			return ProxyOutcome{Err: &RedirectExhaustedError{JobID: job.ID}}
		}
		// redirectNotApplicable: fall through to today's existing
		// local-fallback behavior, completely unchanged.
	}
	if req.DirectOnly {
		a.store.DeleteJob(job.ID)
	}
	return ProxyOutcome{Job: job, FallbackReason: reason, FallbackStatus: status}
}

// isOverloadSignal reports whether a direct-dispatch response means "hand
// this off to the durable, paced queue instead" — 429, any 5xx, or an ORCA
// endpoint-load-metrics header indicating overload (orcaRps returning
// non-nil, i.e. >=70% KV-cache utilization). Distinct from execute()'s own
// >=500-only retry classification (account_queue.go): a direct attempt has
// no retry loop of its own, so any of these means fall back, not retry
// inline. When true, the second return value is how long to trip this
// upstream's circuit breaker for — anchored to the upstream's own
// Retry-After header when it sends one (times a configurable safety
// multiplier), falling back to a fixed configured default otherwise, since
// 5xx/timeout/ORCA signals don't carry that header.
func isOverloadSignal(resp *http.Response) (cooldown time.Duration, overloaded bool) {
	isOverload := resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= 500 ||
		orcaRps(resp.Header) != nil
	if !isOverload {
		return 0, false
	}
	return breakerCooldown(resp.Header), true
}

func breakerCooldown(headers http.Header) time.Duration {
	if raw := headers.Get("Retry-After"); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			multiplier := envInt64("AQUIFER_PROXY_BREAKER_RETRY_MULTIPLIER", defaultProxyBreakerRetryMultiplier)
			return time.Duration(secs) * time.Duration(multiplier) * time.Second
		}
	}
	return time.Duration(envInt64("AQUIFER_PROXY_BREAKER_DEFAULT_COOLDOWN_SECONDS", defaultProxyBreakerCooldownSeconds)) * time.Second
}

// proxyDirectAttemptTimeout is how long AttemptDirect waits for a direct
// response before giving up and falling back — deliberately short and
// separate from the queue path's own 30s client timeout (makeRequest),
// since the whole point of proxy mode is failing fast, not tying up the
// caller's connection.
func proxyDirectAttemptTimeout() time.Duration {
	return time.Duration(envInt64("AQUIFER_PROXY_DIRECT_ATTEMPT_TIMEOUT_SECONDS", defaultProxyDirectAttemptTimeoutSecs)) * time.Second
}
