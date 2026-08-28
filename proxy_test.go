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
	}{
		{"200 plain", 200, nil, false},
		{"429", 429, nil, true},
		{"500", 500, nil, true},
		{"503", 503, nil, true},
		{"200 with orca overload header", 200, map[string]string{orcaHeaderName: "TEXT named_metrics.kv_cache_usage_perc=0.95"}, true},
		{"200 with orca healthy header", 200, map[string]string{orcaHeaderName: "TEXT named_metrics.kv_cache_usage_perc=0.10"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := &http.Response{StatusCode: c.status, Header: http.Header{}}
			for k, v := range c.headers {
				resp.Header.Set(k, v)
			}
			_, overloaded := isOverloadSignal(resp)
			if overloaded != c.overload {
				t.Fatalf("expected overloaded=%v, got %v", c.overload, overloaded)
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

func TestAttemptDirectFallsBackOn5xxWithoutCompletingTheJob(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	req := proxyJobRequest("user-1", "key-2", srv.URL)
	outcome := app.AttemptDirect(context.Background(), req, 2*time.Second)

	if outcome.Err != nil {
		t.Fatalf("unexpected error: %v", outcome.Err)
	}
	if outcome.Direct {
		t.Fatalf("expected fallback on 500, got a direct completion: %+v", outcome)
	}
	if outcome.Job == nil {
		t.Fatalf("expected a persisted job to fall back with, got nil")
	}

	job := store.GetJob(outcome.Job.ID)
	if job.Status != StatusQueued {
		t.Fatalf("expected job left in queued state for the caller to Dispatch, got %q", job.Status)
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
		// this test is proving proxy mode falls back into correctly.
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
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
