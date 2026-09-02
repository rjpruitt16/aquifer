package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func proxyJobRequest(userID, idempotentKey, url string) JobRequest {
	return JobRequest{
		UserID:        userID,
		IdempotentKey: idempotentKey,
		URL:           url,
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	}
}

func TestIsOverloadSignal(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		headers  map[string]string
		overload bool
		kind     string
	}{
		{"200 plain", 200, nil, false, ""},
		{"429 defaults to queue", 429, nil, true, "queue"},
		{"503 defaults to reroute", 503, nil, true, "reroute"},
		// Deliberate narrowing from earlier behavior (every 5xx used to
		// mean the same thing): 500/502/504 aren't in either default set,
		// so they're not overload signals at all unless the upstream
		// configures them explicitly -- relayed to the caller as a
		// normal, if unfortunate, direct response.
		{"500 not overload by default", 500, nil, false, ""},
		{"502 not overload by default", 502, nil, false, ""},
		{"200 with orca overload header is reroute-eligible", 200, map[string]string{orcaHeaderName: "TEXT named_metrics.kv_cache_usage_perc=0.95"}, true, "reroute"},
		{"200 with orca healthy header", 200, map[string]string{orcaHeaderName: "TEXT named_metrics.kv_cache_usage_perc=0.10"}, false, ""},
		{"upstream widens reroute codes to 5xx", 500, map[string]string{"X-Aqueduct-Reroute-Codes": "5xx"}, true, "reroute"},
		{"upstream configures a custom queue code", 529, map[string]string{"X-Aqueduct-Queue-Codes": "429,529"}, true, "queue"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: c.status, Header: http.Header{}}
			for k, v := range c.headers {
				resp.Header.Set(k, v)
			}
			_, kind, overloaded := isOverloadSignal(resp)
			if overloaded != c.overload {
				t.Fatalf("expected overloaded=%v, got %v", c.overload, overloaded)
			}
			if kind != c.kind {
				t.Fatalf("expected kind=%q, got %q", c.kind, kind)
			}
		})
	}
}

func TestBreakerCooldownUsesRetryAfterTimesMultiplier(t *testing.T) {
	t.Setenv("AQUIFER_PROXY_BREAKER_RETRY_MULTIPLIER", "2")
	headers := http.Header{}
	headers.Set("Retry-After", "3")

	got := breakerCooldown(headers)
	want := 6 * time.Second
	if got != want {
		t.Fatalf("expected cooldown %v (3s Retry-After x 2 multiplier), got %v", want, got)
	}
}

func TestBreakerCooldownFallsBackToDefaultWithoutRetryAfter(t *testing.T) {
	t.Setenv("AQUIFER_PROXY_BREAKER_DEFAULT_COOLDOWN_SECONDS", "9")
	got := breakerCooldown(http.Header{})
	want := 9 * time.Second
	if got != want {
		t.Fatalf("expected default cooldown 9s, got %v", got)
	}
}

func TestAttemptDirectSuccessBypassesQueue(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("real upstream response"))
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-1", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if !outcome.Direct {
		t.Fatalf("expected a direct completion, got fallback: %+v", outcome)
	}
	if outcome.Status != http.StatusOK {
		t.Fatalf("expected status 200, got %d", outcome.Status)
	}
	if string(outcome.Body) != "real upstream response" {
		t.Fatalf("expected real upstream body relayed, got %q", outcome.Body)
	}

	job := store.GetJob(outcome.Job.ID)
	if job.Status != StatusCompleted {
		t.Fatalf("expected job status completed (never queued/in_flight), got %q", job.Status)
	}
}

func TestAttemptDirectFallsBackOn503WithoutCompletingTheJob(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	// 503 is the default reroute-eligible code (classifyOverload,
	// proxy.go) -- a plain 500 deliberately no longer triggers fallback by
	// default, see TestIsOverloadSignal.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-2", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if outcome.Direct {
		t.Fatalf("expected fallback on 503, got a direct completion: %+v", outcome)
	}
	if outcome.Job == nil {
		t.Fatalf("expected a persisted job to fall back with, got nil")
	}

	job := store.GetJob(outcome.Job.ID)
	if job.Status != StatusQueued {
		t.Fatalf("expected job left in queued state for the caller to Dispatch, got %q", job.Status)
	}
}

func TestAttemptDirectDoesNotFallBackOnPlain500ByDefault(t *testing.T) {
	// Deliberate narrowing from earlier behavior: a bare 500 isn't in
	// either default classification set (queue: 429, reroute: 503), so
	// it's relayed to the caller as a normal, if unfortunate, direct
	// response -- not treated as an overload signal at all.
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-2-plain-500", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if !outcome.Direct {
		t.Fatalf("expected a plain 500 to relay directly (not overload by default), got: %+v", outcome)
	}
	if outcome.Status != http.StatusInternalServerError {
		t.Fatalf("expected status 500 relayed verbatim, got %d", outcome.Status)
	}
}

func TestAttemptDirectTimeoutTriggersFallback(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-3", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 20*time.Millisecond)

	if outcome.Direct {
		t.Fatalf("expected the short timeout to force a fallback, got a direct completion")
	}
	if outcome.Job == nil {
		t.Fatalf("expected a persisted job to fall back with")
	}
}

func TestAttemptDirect429TripsBreakerForThatDomain(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-4", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)
	if outcome.Direct {
		t.Fatalf("expected fallback on 429")
	}

	worker := app.registry.workerFor(outcome.Job)
	if !worker.BreakerOpen() {
		t.Fatalf("expected the breaker for this domain to be open after a 429")
	}
}

func TestBreakerOpenSkipsDirectAttemptEntirely(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	first := app.AttemptDirect(context.Background(), proxyJobRequest("user-1", "key-5", srv.URL), 2*time.Second)
	if first.Direct {
		t.Fatalf("expected first attempt to fall back and trip the breaker")
	}
	if hits.Load() != 1 {
		t.Fatalf("expected exactly 1 hit for the first attempt, got %d", hits.Load())
	}

	// A second request to the SAME domain (different idempotent key, so it
	// isn't deduped) should skip the direct attempt entirely, since the
	// breaker tripped for 60s x the default multiplier.
	second := app.AttemptDirect(context.Background(), proxyJobRequest("user-1", "key-6", srv.URL), 2*time.Second)
	if second.Direct {
		t.Fatalf("expected the open breaker to force a fallback without attempting direct")
	}
	if hits.Load() != 1 {
		t.Fatalf("expected the breaker to prevent a second hit to the upstream, got %d total hits", hits.Load())
	}
}

func TestAttemptDirectHonorsQueueActiveHeaderWithoutDiscardingTheResponse(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Aqueduct-Queue-Active", "true")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("still a real answer"))
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-queue-active", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)

	if !outcome.Direct {
		t.Fatalf("expected the successful response to still be relayed directly, got fallback: %+v", outcome)
	}
	if outcome.Status != http.StatusOK || string(outcome.Body) != "still a real answer" {
		t.Fatalf("expected the real response relayed verbatim, got status=%d body=%q", outcome.Status, outcome.Body)
	}

	worker := app.registry.workerFor(outcome.Job)
	if !worker.BreakerOpen() {
		t.Fatalf("expected X-Aqueduct-Queue-Active: true to trip the breaker for future requests")
	}
}

func TestAttemptDirectFallsBackWhileQueueHasBacklogEvenAfterBreakerCooldownExpires(t *testing.T) {
	t.Setenv("AQUIFER_PROXY_BREAKER_DEFAULT_COOLDOWN_SECONDS", "1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	block := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests) // trips the breaker
			return
		}
		<-block // the dispatched fallback job's own request blocks here
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	first := app.AttemptDirect(context.Background(), proxyJobRequest("user-1", "key-backlog-a", srv.URL), 2*time.Second)
	if first.Direct {
		t.Fatalf("expected first attempt to fall back and trip the breaker")
	}
	// Simulate what the real HTTP handler (server.go's proxyJob) does on
	// fallback: actually enqueue the job, so the domain's AccountQueue has
	// real backlog, not just a persisted-but-untouched job record.
	app.Dispatch(first.Job, "")

	worker := app.registry.workerFor(first.Job)
	deadline := time.Now().Add(2 * time.Second)
	for !worker.QueueActive() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !worker.QueueActive() {
		t.Fatalf("expected the dispatched fallback job to show up as queue backlog")
	}

	time.Sleep(1100 * time.Millisecond) // let the breaker cooldown elapse
	if worker.BreakerOpen() {
		t.Fatalf("test setup: expected the breaker cooldown to have elapsed by now")
	}

	// The breaker itself is closed now, but the domain still has a real
	// backlog draining (the blocked in-flight request from the first
	// fallback) -- a third request should still fall back, not attempt
	// direct, since the cooldown expiring doesn't mean the backlog it
	// caused has actually finished.
	third := app.AttemptDirect(context.Background(), proxyJobRequest("user-1", "key-backlog-b", srv.URL), 2*time.Second)
	if third.Direct {
		t.Fatalf("expected fallback while queue still has backlog, even though the breaker cooldown elapsed")
	}
	if hits.Load() != 2 {
		t.Fatalf("expected no direct attempt against the upstream while backlog is active, got %d hits", hits.Load())
	}

	// Release the blocked in-flight request and wait for the AccountQueue's
	// dispatch goroutine to actually finish (not just for the HTTP call to
	// return, but for it to write its result back through the store) before
	// the test ends -- otherwise that goroutine can still be touching the
	// per-test SQLite tmpdir after t.Cleanup starts tearing it down, which
	// shows up as an intermittent "database is closed" or tmpdir
	// "directory not empty" failure under -count>1 or full-suite runs.
	close(block)
	deadline = time.Now().Add(2 * time.Second)
	for worker.QueueActive() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if worker.QueueActive() {
		t.Fatalf("expected the backlog to drain after releasing the blocked request")
	}
}

func TestAttemptDirectDuplicateSkipsSecondDirectAttempt(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "same-key", srv.URL)
	first := app.AttemptDirect(context.Background(), req, 2*time.Second)
	if !first.Direct {
		t.Fatalf("expected the first request to complete directly")
	}

	second := app.AttemptDirect(context.Background(), req, 2*time.Second)
	if !second.Duplicate {
		t.Fatalf("expected the second identical request to be reported as a duplicate")
	}
	if second.ExistingJob == nil || second.ExistingJob.ID != first.Job.ID {
		t.Fatalf("expected the duplicate to reference the original job")
	}
	if hits.Load() != 1 {
		t.Fatalf("expected the duplicate to never re-attempt the upstream, got %d total hits", hits.Load())
	}
}

func TestProxyJobHTTPDirectSuccess(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	body, _ := json.Marshal(proxyJobRequest("user-1", "http-key-1", upstream.URL))
	req := httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 relayed from upstream, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Fatalf("expected the real upstream body relayed, got: %s", rec.Body.String())
	}
}

func TestProxyJobHTTPDuplicateOfTerminalJobReturnsStatusWithoutHanging(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	body, _ := json.Marshal(proxyJobRequest("user-1", "http-key-2", upstream.URL))

	first := httptest.NewRecorder()
	srv.Routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body)))
	if first.Code != http.StatusOK {
		t.Fatalf("expected first request to succeed directly, got %d", first.Code)
	}

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body)))
		done <- rec
	}()

	select {
	case rec := <-done:
		var got map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("expected JSON status body for a terminal duplicate, got %q: %v", rec.Body.String(), err)
		}
		if dup, _ := got["duplicate"].(bool); !dup {
			t.Fatalf("expected duplicate:true in response, got: %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("duplicate-of-terminal-job request hung instead of returning a synchronous status")
	}
}

func TestProxyJobHTTPFallsBackAndStreamsToEventualCompletion(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)

	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fails the direct attempt (hit 1) and the queue's own first retry
		// (hit 2), then succeeds — well within maxRetries — so the job
		// reaches a real "completed" event via the normal AccountQueue path
		// this test is proving proxy mode falls back into correctly. 503
		// (not 500) for the direct attempt specifically, since that's the
		// default reroute-eligible/overload code now (classifyOverload,
		// proxy.go) — a plain 500 no longer triggers fallback by default.
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("eventually ok"))
	}))
	defer upstream.Close()

	body, _ := json.Marshal(proxyJobRequest("user-1", "http-key-3", upstream.URL))
	req := httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		srv.Routes().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback-and-stream request never completed")
	}

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected the fallback response to be an SSE stream, got Content-Type %q (body: %s)", ct, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "event: completed") {
		t.Fatalf("expected an eventual 'completed' SSE event, got: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "eventually ok") {
		t.Fatalf("expected the real upstream body once it succeeded, got: %s", rec.Body.String())
	}
}

func TestProxyJobHTTPFallbackStreamsAProxyFallbackEventFirst(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	body, _ := json.Marshal(proxyJobRequest("user-1", "http-key-proxy-fallback-event", upstream.URL))
	req := httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		srv.Routes().ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback request never completed")
	}

	out := rec.Body.String()
	if !strings.Contains(out, "event: proxy_fallback") {
		t.Fatalf("expected a proxy_fallback event before the normal queued/dispatching sequence, got: %s", out)
	}
	if !strings.Contains(out, `"reason":"upstream_overloaded"`) {
		t.Fatalf("expected reason=upstream_overloaded for a 503, got: %s", out)
	}
	if !strings.Contains(out, `"upstream_status":503`) {
		t.Fatalf("expected the real upstream status code included, got: %s", out)
	}
	// proxy_fallback must come before queued, not after — a client
	// shouldn't see "you're queued" before it sees why.
	if strings.Index(out, "event: proxy_fallback") > strings.Index(out, "event: queued") {
		t.Fatalf("expected proxy_fallback before queued, got: %s", out)
	}
}

func TestFallbackOutcomeDeletesJobWhenDirectOnly(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	req := proxyJobRequest("user-1", "key-direct-only-cleanup", "http://example.invalid")
	req.DirectOnly = true
	job, _, err := app.PrepareJob(req)
	if err != nil {
		t.Fatalf("unexpected error preparing job: %v", err)
	}

	outcome := app.fallbackOutcome(context.Background(), req, job, "upstream_unreachable", 0, time.Second, true)
	if outcome.FallbackReason != "upstream_unreachable" {
		t.Fatalf("expected the reason to be preserved, got %q", outcome.FallbackReason)
	}

	if got := store.GetJob(job.ID); got != nil {
		t.Fatalf("expected DirectOnly to delete the job row it had inserted, but it still exists: %+v", got)
	}
}

func TestFallbackOutcomeKeepsJobWhenNotDirectOnly(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	req := proxyJobRequest("user-1", "key-normal-fallback-keeps-job", "http://example.invalid")
	job, _, err := app.PrepareJob(req)
	if err != nil {
		t.Fatalf("unexpected error preparing job: %v", err)
	}

	app.fallbackOutcome(context.Background(), req, job, "upstream_unreachable", 0, time.Second, true)

	if got := store.GetJob(job.ID); got == nil {
		t.Fatalf("expected a normal (non-DirectOnly) fallback to leave the job row intact for the caller to Dispatch")
	}
}

func TestProxyJobHTTPDirectOnlyReturnsCleanRejectionWithoutStreaming(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	reqBody := proxyJobRequest("user-1", "key-direct-only-http", upstream.URL)
	reqBody.DirectOnly = true
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected a synchronous JSON response, not a stream, got Content-Type %q", ct)
	}

	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid JSON, got: %s", rec.Body.String())
	}
	if decoded["reason"] != "upstream_overloaded" {
		t.Fatalf("expected reason=upstream_overloaded, got: %v", decoded["reason"])
	}

	// The whole point of DirectOnly: no ghost queued row left behind, and
	// nothing was ever actually dispatched.
	jobs := store.GetQueuedJobs()
	for _, j := range jobs {
		if j.IdempotentKey == "key-direct-only-http" {
			t.Fatalf("expected no job row left behind for a DirectOnly request that couldn't succeed, found: %+v", j)
		}
	}
}
