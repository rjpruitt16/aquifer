// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/dispatch"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// Helpers shared by the root tests that drive the dispatch path through a
// whole Aquifer. internal/dispatch has its own copies for its unit tests.

func localJob(userID, url string) *Job {
	return &Job{ID: jobs.NewID(), UserID: userID, URL: url, Method: "POST", Status: StatusQueued}
}

func okServer(t *testing.T, header string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if header != "" {
			w.Header().Set("X-Aqueduct-Account-Queue", header)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitForMode(t *testing.T, r *Registry, url string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w, ok := r.Worker(url)
		if ok && w.AccountQueueMode() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("account-queue mode for %s never became %v", url, want)
}

func workerFor(t *testing.T, r *Registry, url string) *URLWorker {
	t.Helper()
	w, ok := r.Worker(url)
	if !ok {
		t.Fatalf("expected a worker for %s", url)
	}
	return w
}

// skipL8Probe reports whether this request is deliverWebhookSync's own
// preliminary L8-trust-handshake probe (GET /.well-known/l8), writing 404
// and returning true if so, so test handlers below only count the actual
// webhook delivery attempt, not this unrelated preliminary request.
func skipL8Probe(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/.well-known/l8" {
		w.WriteHeader(http.StatusNotFound)
		return true
	}
	return false
}

func drainTestRegistry(t *testing.T, metrics MetricsAdapter) *Registry {
	t.Helper()
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	r := NewRegistry(store, cfg, broker, l8, metrics, nil)
	t.Cleanup(r.Close)
	return r
}

func seedLedgerEntry(t *testing.T, r *Registry) {
	t.Helper()
	job := &Job{
		ID: jobs.NewID(), UserID: "u1", IdempotentKey: "k1",
		URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com",
		Status: StatusQueued, CreatedAt: time.Now().UnixMilli(),
	}
	r.Store().CheckOrInsert(job)
	r.Store().UpdateStatus(job.ID, StatusCompleted)
}

func waitForWebhook(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()

	select {
	case payload := <-ch:
		return payload
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
	return nil
}

func jobFor(userID, apiKey string) *Job {
	return &Job{
		ID:     jobs.NewID(),
		UserID: userID,
		// postman-echo.com/post always returns a fast 200 for both the
		// dispatch and the webhook delivery — example.com's 405 on POST
		// made deliverWebhook retry 4x with exponential backoff (up to 15s),
		// leaving a background goroutine alive long after the test function
		// returns and racing t.TempDir()'s cleanup against the still-open
		// store.
		URL:        "https://postman-echo.com/post",
		Method:     "POST",
		Headers:    map[string]string{"Authorization": apiKey},
		WebhookURL: "https://postman-echo.com/post",
		Status:     StatusQueued,
	}
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	r := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	t.Cleanup(r.Close)
	return r
}

func waitUntil(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return condition()
}

func queueKeys(m map[string]*AccountQueue) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func retryJob(key, url, webhook string, maxRetries *int) JobRequest {
	return JobRequest{UserID: "u", IdempotentKey: key, URL: url, Method: "POST", WebhookURL: webhook, MaxRetries: maxRetries}
}

func withRetryBackoff(t *testing.T, d time.Duration) {
	t.Helper()
	old := dispatch.RetryBackoffFunc.Load()
	dispatch.RetryBackoffFunc.Store(func(int) time.Duration { return d })
	t.Cleanup(func() { dispatch.RetryBackoffFunc.Store(old) })
}

func webhookSink(t *testing.T) (*httptest.Server, chan map[string]any) {
	t.Helper()
	ch := make(chan map[string]any, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["job_id"]; ok {
			ch <- payload
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

// failingUpstream fails its first `failures` requests with `status` (or
// forever when failures < 0), then returns 200.
func failingUpstream(t *testing.T, failures int, status int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		n := hits.Add(1)
		if failures < 0 || n <= int64(failures) {
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func intPtr(n int) *int { return &n }
