// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryDefaultsToFourRetries(t *testing.T) {
	withRetryBackoff(t, 0)
	up, hits := failingUpstream(t, -1, http.StatusInternalServerError)
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	if _, err := app.Enqueue(retryJob("default", up.URL, hook.URL, nil)); err != nil {
		t.Fatal(err)
	}
	payload := waitForWebhook(t, payloads)
	if payload["status"] != "failed" || hits.Load() != DefaultMaxRetries+1 {
		t.Fatalf("expected %d attempts then failure, got %d attempts, payload %v", DefaultMaxRetries+1, hits.Load(), payload)
	}
}

func TestRetryZeroMeansSingleAttempt(t *testing.T) {
	withRetryBackoff(t, 0)
	up, hits := failingUpstream(t, -1, http.StatusBadGateway)
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	app.Enqueue(retryJob("zero", up.URL, hook.URL, intPtr(0)))
	if payload := waitForWebhook(t, payloads); payload["status"] != "failed" || hits.Load() != 1 {
		t.Fatalf("expected one attempt, got %d, payload %v", hits.Load(), payload)
	}
}

func TestRetryUntilCompleteOutlastsDefault(t *testing.T) {
	withRetryBackoff(t, 0)
	up, hits := failingUpstream(t, 7, http.StatusServiceUnavailable)
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	app.Enqueue(retryJob("forever", up.URL, hook.URL, intPtr(RetryUntilComplete)))
	payload := waitForWebhook(t, payloads)
	if payload["status"] != "completed" || hits.Load() != 8 {
		t.Fatalf("expected success on attempt 8, got %d attempts, payload %v", hits.Load(), payload)
	}
}

func TestRetryUntilCompleteStopsAtExecuteBefore(t *testing.T) {
	withRetryBackoff(t, 300*time.Millisecond)
	up, hits := failingUpstream(t, -1, http.StatusInternalServerError)
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	req := retryJob("bounded", up.URL, hook.URL, intPtr(RetryUntilComplete))
	req.ExecuteBefore = time.Now().Add(1200 * time.Millisecond).UnixMilli()
	app.Enqueue(req)

	payload := waitForWebhook(t, payloads)
	reason, _ := payload["reason"].(string)
	if payload["status"] != "failed" || !(strings.Contains(reason, "retry_window_exhausted") || reason == "execution_deadline_exceeded") {
		t.Fatalf("expected the deadline to end unlimited retries, got payload %v", payload)
	}
	stoppedAt := hits.Load()
	time.Sleep(700 * time.Millisecond)
	if hits.Load() != stoppedAt {
		t.Fatalf("attempts continued after the job failed: %d -> %d", stoppedAt, hits.Load())
	}
}

func TestRetryDoesNotRetryOtherClientErrors(t *testing.T) {
	withRetryBackoff(t, 0)
	up, hits := failingUpstream(t, -1, http.StatusBadRequest)
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	app.Enqueue(retryJob("bad-request", up.URL, hook.URL, nil))
	payload := waitForWebhook(t, payloads)
	if hits.Load() != 1 || payload["response_status"] != float64(http.StatusBadRequest) {
		t.Fatalf("a 400 must not be retried: %d attempts, payload %v", hits.Load(), payload)
	}
}

func TestRetryHonorsRetryAfterOn429(t *testing.T) {
	withRetryBackoff(t, 0)
	var hits atomic.Int64
	var firstAt, secondAt atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		switch hits.Add(1) {
		case 1:
			firstAt.Store(time.Now().UnixMilli())
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			secondAt.CompareAndSwap(0, time.Now().UnixMilli())
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer up.Close()
	hook, payloads := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	app.Enqueue(retryJob("rate-limited", up.URL, hook.URL, nil))
	if payload := waitForWebhook(t, payloads); payload["status"] != "completed" {
		t.Fatalf("expected the retry after 429 to complete, got %v", payload)
	}
	if gap := secondAt.Load() - firstAt.Load(); gap < 950 {
		t.Fatalf("expected Retry-After: 1 to delay the retry ~1s, got %dms", gap)
	}
}

func TestRetryBackoffFreesTheConcurrencySlot(t *testing.T) {
	withRetryBackoff(t, 2*time.Second)
	var failingHits atomic.Int64
	completedB := make(chan time.Time, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/a") {
			failingHits.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		completedB <- time.Now()
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()
	hook, _ := webhookSink(t)
	app, _ := testAquiferWithLimits(t, AdmissionLimits{}) // max_concurrent 1

	start := time.Now()
	app.Enqueue(retryJob("slot-a", up.URL+"/a", hook.URL, nil))
	for failingHits.Load() == 0 && time.Since(start) < 2*time.Second {
		time.Sleep(10 * time.Millisecond)
	}
	app.Enqueue(retryJob("slot-b", up.URL+"/b", hook.URL, nil))

	select {
	case at := <-completedB:
		if at.Sub(start) > 1500*time.Millisecond {
			t.Fatalf("job B waited %s behind A's backoff; the slot was not freed", at.Sub(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("job B never ran while A was backing off")
	}
}

func TestMaxRetriesValidationAndHeader(t *testing.T) {
	for _, bad := range []int{-2, MaxAllowedRetries + 1} {
		req := retryJob("v", "https://example.com", "https://example.com/hook", intPtr(bad))
		if msg := req.Validate(); !strings.Contains(msg, "max_retries") {
			t.Fatalf("expected max_retries=%d to be rejected, got %q", bad, msg)
		}
	}

	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	handler := NewServer(app).Routes()
	post := func(header string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(retryJob("hdr-"+header, "https://example.com", "https://example.com/hook", intPtr(2)))
		r := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		r.Header.Set("X-Aqueduct-Max-Retries", header)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec
	}

	if rec := post("abc"); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected a non-integer header to be rejected, got %d", rec.Code)
	}
	rec := post("-1")
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected -1 to be accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	var res EnqueueResult
	json.Unmarshal(rec.Body.Bytes(), &res)
	if job, _ := app.GetJob(res.JobID); job == nil || job.MaxRetries != RetryUntilComplete {
		t.Fatalf("expected the header to override the body's max_retries=2, got %+v", job)
	}
}
