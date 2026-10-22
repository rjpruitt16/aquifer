// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
	"github.com/rjpruitt16/aquifer/l8"
)

func withRetryBackoff(t *testing.T, d time.Duration) {
	t.Helper()
	old := RetryBackoffFunc.Load()
	RetryBackoffFunc.Store(func(int) time.Duration { return d })
	t.Cleanup(func() { RetryBackoffFunc.Store(old) })
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

func retryJob(key, url, webhook string, maxRetries *int) jobs.Request {
	return jobs.Request{UserID: "u", IdempotentKey: key, URL: url, Method: "POST", WebhookURL: webhook, MaxRetries: maxRetries}
}

func intPtr(n int) *int { return &n }

func TestRetryHalvesQueuePaceOnFailure(t *testing.T) {
	withRetryBackoff(t, time.Hour)
	up, hits := failingUpstream(t, -1, http.StatusInternalServerError)
	dir := t.TempDir()
	store := storage.NewSQLite(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() { store.Close() })
	broker := NewBroker()
	l8 := l8.NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	t.Cleanup(l8.Close)

	q := NewAccountQueue("pace", up.URL, 8, 1, nil, store, broker, l8, NoopMetricsAdapter{},
		func(string, string, string, int, map[string]any) {}, nil, nil, nil, nil, func(string) {}, false, nil, nil)
	t.Cleanup(q.Stop)

	req := retryJob("pace", up.URL, "https://example.com/hook", nil)
	job := jobs.New(&req)
	store.CheckOrInsert(job)
	q.Enqueue(job)

	deadline := time.Now().Add(3 * time.Second)
	for q.RPS() != 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hits.Load() != 1 || q.RPS() != 4 {
		t.Fatalf("expected one failure to halve the pace from 8 to 4 rps, got %v after %d attempts", q.RPS(), hits.Load())
	}
	if stored := store.GetJob(job.ID); stored == nil || stored.Attempts != 1 || stored.Status != jobs.StatusQueued {
		t.Fatalf("expected the retry to be persisted as queued with 1 attempt, got %+v", stored)
	}
}
