// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
	"github.com/rjpruitt16/aquifer/l8"
)

// A job that waits in the queue past the queued-job TTL used to be deleted by
// store cleanup with no webhook. The queue now fails it first.

type capturedWebhook struct {
	jobID   string
	payload map[string]any
}

func queuedTTLQueue(t *testing.T, upstream string, hooks chan capturedWebhook) (*AccountQueue, storage.Store) {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewSQLite(dir + "/aquifer.db")
	broker := NewBroker()
	reg := l8.NewRegistry(dir+"/.l8-key", dir+"/l8-trust")
	enqueue := func(jobID, _, _ string, _ int, payload map[string]any) {
		hooks <- capturedWebhook{jobID: jobID, payload: payload}
	}
	q := NewAccountQueue("ttl-user", upstream, 100, 1, nil, store, broker, reg, NoopMetricsAdapter{}, enqueue, nil, nil, nil, nil, func(string) {}, false, func(bool) {}, nil)
	t.Cleanup(func() {
		q.Stop()
		reg.Close()
		store.Close()
	})
	return q, store
}

func ttlJob(key, url string, createdAt time.Time) *jobs.Job {
	return &jobs.Job{
		ID: jobs.NewID(), UserID: "ttl-user", IdempotentKey: key,
		URL: url, Method: "POST", WebhookURL: "http://127.0.0.1:1/hook",
		Status: jobs.StatusQueued, CreatedAt: createdAt.UnixMilli(),
	}
}

func TestJobQueuedPastTheTTLFailsWithAWebhook(t *testing.T) {
	hooks := make(chan capturedWebhook, 4)
	q, store := queuedTTLQueue(t, "http://127.0.0.1:1", hooks)
	job := ttlJob("old", "http://127.0.0.1:1/should-not-run", time.Now().Add(-jobs.QueuedTTL-time.Second))
	store.CheckOrInsert(job)
	q.Enqueue(job)

	select {
	case hook := <-hooks:
		if hook.jobID != job.ID || hook.payload["reason"] != "queued_ttl_exceeded" || hook.payload["status"] != "failed" {
			t.Fatalf("webhook = %+v, want a failed webhook for %s with reason queued_ttl_exceeded", hook, job.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no webhook for a job queued past the TTL")
	}
	if stored := store.GetJob(job.ID); stored == nil || stored.Status != jobs.StatusFailed {
		t.Fatalf("stored job = %+v, want failed", stored)
	}
}

func TestQueuedTTLExpiresWhileTheOnlySlotIsStuck(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		close(release)
		upstream.Close()
	})

	hooks := make(chan capturedWebhook, 4)
	q, store := queuedTTLQueue(t, upstream.URL, hooks)
	busy := ttlJob("busy", upstream.URL, time.Now())
	// Expires 300 ms after it's queued, while busy holds the only slot.
	waiting := ttlJob("waiting", upstream.URL, time.Now().Add(-jobs.QueuedTTL+300*time.Millisecond))
	store.CheckOrInsert(busy)
	store.CheckOrInsert(waiting)
	q.Enqueue(busy)
	q.Enqueue(waiting)

	select {
	case hook := <-hooks:
		if hook.jobID != waiting.ID || hook.payload["reason"] != "queued_ttl_exceeded" {
			t.Fatalf("webhook = %+v, want queued_ttl_exceeded for the waiting job", hook)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("the waiting job never expired while the only slot was stuck")
	}
}

func TestQueueDeadline(t *testing.T) {
	now := time.Now()
	created := now.UnixMilli()
	if got := (&jobs.Job{CreatedAt: created}).QueueDeadline(); got != created+jobs.QueuedTTL.Milliseconds() {
		t.Fatalf("default deadline = %d, want created + QueuedTTL", got)
	}
	if got := (&jobs.Job{CreatedAt: created, ExecuteBefore: created + 5}).QueueDeadline(); got != created+5 {
		t.Fatalf("execute_before deadline = %d, want %d", got, created+5)
	}
	if got := (&jobs.Job{}).QueueDeadline(); got != 0 {
		t.Fatalf("deadline with no times = %d, want 0", got)
	}
	far := now.Add(2 * jobs.QueuedTTL).UnixMilli()
	if got := (&jobs.Job{CreatedAt: created, ExecuteBefore: far}).PendingExpiresAt(now); got <= far {
		t.Fatalf("a pending row must outlive a far execute_before: expires %d, deadline %d", got, far)
	}
}
