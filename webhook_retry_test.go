// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// flakyReceiver is a webhook receiver that answers 503 while down() is true,
// then 200. It counts every delivery attempt and records which job_ids it
// accepted.
type flakyReceiver struct {
	srv      *httptest.Server
	attempts atomic.Int64
	mu       sync.Mutex
	got      map[string]bool
}

func newFlakyReceiver(t *testing.T, down func(attempt int64) bool) *flakyReceiver {
	t.Helper()
	fr := &flakyReceiver{got: map[string]bool{}}
	fr.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		n := fr.attempts.Add(1)
		if down(n) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if id, ok := payload["job_id"].(string); ok {
			fr.mu.Lock()
			fr.got[id] = true
			fr.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(fr.srv.Close)
	return fr
}

func (fr *flakyReceiver) delivered() int {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return len(fr.got)
}

func okUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv, _ := failingUpstream(t, 0, http.StatusOK)
	return srv
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestWebhookDeliveryDefaultsToFourRetries(t *testing.T) {
	withRetryBackoff(t, 0)
	receiver := newFlakyReceiver(t, func(int64) bool { return true })
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	if _, err := app.Enqueue(retryJob("hook-default", okUpstream(t).URL, receiver.srv.URL, nil)); err != nil {
		t.Fatal(err)
	}
	want := int64(DefaultMaxRetries + 1)
	if !waitFor(t, 10*time.Second, func() bool { return receiver.attempts.Load() >= want }) {
		t.Fatalf("expected %d delivery attempts, got %d", want, receiver.attempts.Load())
	}
	time.Sleep(500 * time.Millisecond)
	if got := receiver.attempts.Load(); got != want {
		t.Fatalf("delivery should stop after %d attempts, got %d", want, got)
	}
}

func TestWebhookMaxRetriesUntilDelivered(t *testing.T) {
	withRetryBackoff(t, 0)
	receiver := newFlakyReceiver(t, func(n int64) bool { return n <= 7 })
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	req := retryJob("hook-forever", okUpstream(t).URL, receiver.srv.URL, nil)
	req.WebhookMaxRetries = intPtr(RetryUntilComplete)
	if _, err := app.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 15*time.Second, func() bool { return receiver.delivered() == 1 }) {
		t.Fatalf("webhook was not delivered after %d attempts", receiver.attempts.Load())
	}
	if got := receiver.attempts.Load(); got != 8 {
		t.Fatalf("expected delivery on attempt 8, got %d attempts", got)
	}
}

// webhook_max_retries must not change how often the target API is tried.
func TestWebhookMaxRetriesLeavesUpstreamRetriesAlone(t *testing.T) {
	withRetryBackoff(t, 0)
	up, upstreamHits := failingUpstream(t, -1, http.StatusBadGateway)
	receiver := newFlakyReceiver(t, func(n int64) bool { return n <= 6 })
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	req := retryJob("hook-split", up.URL, receiver.srv.URL, intPtr(0))
	req.WebhookMaxRetries = intPtr(10)
	app.Enqueue(req)
	if !waitFor(t, 15*time.Second, func() bool { return receiver.delivered() == 1 }) {
		t.Fatalf("webhook was not delivered after %d attempts", receiver.attempts.Load())
	}
	if upstreamHits.Load() != 1 {
		t.Fatalf("max_retries 0 should mean one upstream attempt, got %d", upstreamHits.Load())
	}
}

// A receiver outage outlasting the default retry budget: with -1 every
// webhook still arrives once the receiver is back.
func TestWebhookMaxRetriesSurvivesReceiverOutage(t *testing.T) {
	withRetryBackoff(t, 100*time.Millisecond)
	const jobs = 10
	outage := 1500 * time.Millisecond
	start := time.Now()
	receiver := newFlakyReceiver(t, func(int64) bool { return time.Since(start) < outage })
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	upstream := okUpstream(t).URL

	for i := 0; i < jobs; i++ {
		req := retryJob("outage-"+string(rune('a'+i)), upstream, receiver.srv.URL, nil)
		req.WebhookMaxRetries = intPtr(RetryUntilComplete)
		if _, err := app.Enqueue(req); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(t, 60*time.Second, func() bool { return receiver.delivered() == jobs }) {
		t.Fatalf("only %d of %d webhooks delivered after the outage (%d attempts)", receiver.delivered(), jobs, receiver.attempts.Load())
	}
}

func TestWebhookMaxRetriesValidation(t *testing.T) {
	req := retryJob("v", "https://example.com", "https://example.com/hook", nil)
	for _, n := range []int{-1, 0, 4, MaxAllowedRetries} {
		req.WebhookMaxRetries = intPtr(n)
		if msg := req.Validate(); msg != "" {
			t.Fatalf("webhook_max_retries %d should be valid, got %q", n, msg)
		}
	}
	for _, n := range []int{-2, MaxAllowedRetries + 1} {
		req.WebhookMaxRetries = intPtr(n)
		if msg := req.Validate(); !strings.Contains(msg, "webhook_max_retries") {
			t.Fatalf("webhook_max_retries %d should be rejected, got %q", n, msg)
		}
	}
}

func TestWebhookMaxRetriesHeaderOverridesBody(t *testing.T) {
	header := http.Header{}
	header.Set("X-Aqueduct-Webhook-Max-Retries", "-1")
	req := JobRequest{WebhookMaxRetries: intPtr(0)}
	if err := applyMaxRetriesHeader(header, &req); err != nil {
		t.Fatal(err)
	}
	if req.WebhookMaxRetries == nil || *req.WebhookMaxRetries != RetryUntilComplete {
		t.Fatalf("header should win, got %v", req.WebhookMaxRetries)
	}
	if req.MaxRetries != nil {
		t.Fatalf("the webhook header must not set max_retries, got %d", *req.MaxRetries)
	}

	header.Set("X-Aqueduct-Webhook-Max-Retries", "forever")
	if err := applyMaxRetriesHeader(header, &req); err == nil {
		t.Fatal("expected an error for a non-integer header")
	}
}

func TestWebhookMaxRetriesInvalidRequestIs400(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	routes := NewServer(app).Routes()

	body := `{"user_id":"u","idempotent_key":"k","url":"https://example.com","method":"POST","webhook_url":"https://example.com/hook","webhook_max_retries":101}`
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "webhook_max_retries") {
		t.Fatalf("expected 400 naming webhook_max_retries, got %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(strings.Replace(body, `,"webhook_max_retries":101`, "", 1)))
	request.Header.Set("X-Aqueduct-Webhook-Max-Retries", "nope")
	routes.ServeHTTP(rec, request)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a bad header, got %d %s", rec.Code, rec.Body.String())
	}
}

// The setting has to survive a restart: the original job can finish after
// a reload, and only then is its webhook delivery created.
func TestWebhookMaxRetriesPersists(t *testing.T) {
	stores := map[string]func(dir string) JobStore{
		"sqlite": func(dir string) JobStore { return NewStore(filepath.Join(dir, "aquifer.db")) },
		"pebble": func(dir string) JobStore { return NewPebbleStore(filepath.Join(dir, "pebble")) },
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store := open(dir)

			set := retryJob("persist-set", "https://example.com", "https://example.com/hook", nil)
			set.WebhookMaxRetries = intPtr(RetryUntilComplete)
			setJob := NewJob(&set)
			unset := retryJob("persist-unset", "https://example.com", "https://example.com/hook", nil)
			unsetJob := NewJob(&unset)
			store.CheckOrInsert(setJob)
			store.CheckOrInsert(unsetJob)
			store.Close()

			store = open(dir)
			defer store.Close()
			got := store.GetJob(setJob.ID)
			if got == nil || got.WebhookMaxRetries == nil || *got.WebhookMaxRetries != RetryUntilComplete {
				t.Fatalf("expected webhook_max_retries -1 after reload, got %+v", got)
			}
			got = store.GetJob(unsetJob.ID)
			if got == nil || got.WebhookMaxRetries != nil || got.webhookMaxRetries() != DefaultMaxRetries {
				t.Fatalf("expected an unset webhook_max_retries to stay unset, got %+v", got)
			}
		})
	}
}

// The soak's receiver stamps X-Aqueduct-Rps on every response, 503s
// included. That advertised rate used to override the halve-on-failure
// pacing, so the queue kept full speed against a down receiver and webhooks
// used up their default retries within seconds of an outage starting.
func TestFailedResponseCannotRaiseThePace(t *testing.T) {
	withRetryBackoff(t, 100*time.Millisecond)
	const jobs = 30
	start := time.Now()
	receiver := newFlakyReceiver(t, func(int64) bool { return time.Since(start) < 1500*time.Millisecond })
	advertising := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Aqueduct-Rps", "100000")
		receiver.srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(advertising.Close)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	upstream := okUpstream(t).URL

	for i := 0; i < jobs; i++ {
		if _, err := app.Enqueue(retryJob("advertised-"+string(rune('a'+i)), upstream, advertising.URL, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(t, 30*time.Second, func() bool { return receiver.delivered() == jobs }) {
		t.Fatalf("only %d of %d webhooks delivered with default retries (%d attempts)", receiver.delivered(), jobs, receiver.attempts.Load())
	}
}
