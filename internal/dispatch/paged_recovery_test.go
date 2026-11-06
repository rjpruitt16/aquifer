// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
)

func bareQueue(t *testing.T, hooks chan capturedWebhook) (*AccountQueue, storage.Store) {
	t.Helper()
	store := storage.NewSQLite(t.TempDir() + "/a.db")
	t.Cleanup(func() { store.Close() })
	q := &AccountQueue{
		key: "tenant", upstream: "http://x", store: store, broker: NewBroker(), metrics: NoopMetricsAdapter{},
		enqueueWebhook: func(jobID, _, _ string, _ int, payload map[string]any) {
			hooks <- capturedWebhook{jobID: jobID, payload: payload}
		},
	}
	return q, store
}

// A job still on disk past its queue deadline fails with a webhook, though
// the queue never drained far enough to load it.
func TestSweepFailsJobsThatExpiredOnDisk(t *testing.T) {
	hooks := make(chan capturedWebhook, 8)
	q, store := bareQueue(t, hooks)
	key := q.storeKey()
	stale := ttlJob("stale", "http://x/work", time.Now().Add(-jobs.QueuedTTL-time.Minute))
	young := ttlJob("young", "http://x/work", time.Now())
	for _, job := range []*jobs.Job{stale, young} {
		store.CheckOrInsert(job)
		store.SetQueueKey(job.ID, key)
	}
	q.spills = []spillSource{{key: key}}

	q.sweepSpilled(time.Now())

	select {
	case hook := <-hooks:
		if hook.jobID != stale.ID || hook.payload["reason"] != "queued_ttl_exceeded" {
			t.Fatalf("webhook = %+v, want queued_ttl_exceeded for the stale job", hook)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no webhook for a job that expired on disk")
	}
	if got := store.GetJob(young.ID); got == nil || got.Status != jobs.StatusQueued {
		t.Fatalf("the young job was touched: %+v", got)
	}
	select {
	case hook := <-hooks:
		t.Fatalf("unexpected second webhook %+v", hook)
	default:
	}
}

// While a queue's own jobs are still on disk, a hand-off past its cursor
// stays there (it loads in turn), one at or before it goes into memory, and
// one just loaded from a page isn't queued twice.
func TestHandoffsWhileJobsAreOnDisk(t *testing.T) {
	q, _ := bareQueue(t, make(chan capturedWebhook, 1))
	now := time.Now().UnixMilli()
	cursorJob := &jobs.Job{ID: "m", CreatedAt: now}
	q.spills = []spillSource{{key: q.storeKey(), after: jobs.CursorAt(cursorJob)}}

	if q.acceptHandoff(&jobs.Job{ID: "z", CreatedAt: now + 1}) {
		t.Fatal("a job past the cursor must stay on disk")
	}
	if !q.acceptHandoff(&jobs.Job{ID: "a", CreatedAt: now - 1}) {
		t.Fatal("a job at or before the cursor goes into memory")
	}
	q.recentLoads = map[string]time.Time{"b": time.Now()}
	if q.acceptHandoff(&jobs.Job{ID: "b", CreatedAt: now - 1}) {
		t.Fatal("a job just loaded from a page was queued again")
	}
	if !q.acceptHandoff(&jobs.Job{ID: "b", CreatedAt: now - 1}) {
		t.Fatal("the load is forgotten after its hand-off arrives once")
	}

	// Another source still paging doesn't hold this queue's own new jobs.
	q.spills = []spillSource{{key: "http://x|old-key", after: jobs.CursorAt(cursorJob)}}
	if !q.acceptHandoff(&jobs.Job{ID: "z", CreatedAt: now + 1}) {
		t.Fatal("with only another key on disk, a new job goes into memory")
	}
}
