// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"fmt"
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
// stays there (it loads in turn), one at or before it goes into memory, one
// already held is skipped, and one a page loaded and finished is skipped.
func TestHandoffsWhileJobsAreOnDisk(t *testing.T) {
	q, store := bareQueue(t, make(chan capturedWebhook, 1))
	now := time.Now()
	ms := now.UnixMilli()
	cursorJob := &jobs.Job{ID: "m", CreatedAt: ms}
	q.spills = []spillSource{{key: q.storeKey(), after: jobs.CursorAt(cursorJob)}}

	if q.acceptHandoff(&jobs.Job{ID: "z", CreatedAt: ms + 1}, now) {
		t.Fatal("a job past the cursor must stay on disk")
	}
	if !q.acceptHandoff(&jobs.Job{ID: "a", CreatedAt: ms - 1}, now) {
		t.Fatal("a job at or before the cursor goes into memory")
	}
	held := &jobs.Job{ID: "b", CreatedAt: ms - 1}
	q.hold(held)
	if q.acceptHandoff(held, now) {
		t.Fatal("a job already held was queued again")
	}

	// A job a page loaded and already finished: its late hand-off is
	// skipped by its stored status, however late it comes in the window.
	done := ttlJob("done", "http://x/work", now.Add(-time.Second))
	store.CheckOrInsert(done)
	store.UpdateStatus(done.ID, jobs.StatusCompleted)
	q.pagedOwnUntil = now.Add(recentPageWindow)
	if q.acceptHandoff(done, now.Add(4*time.Minute)) {
		t.Fatal("the hand-off of a job a page already finished was queued")
	}

	// Another source still paging doesn't hold this queue's own new jobs.
	q.spills = []spillSource{{key: "http://x|old-key", after: jobs.CursorAt(cursorJob)}}
	if !q.acceptHandoff(&jobs.Job{ID: "y", CreatedAt: ms + 1}, now) {
		t.Fatal("with only another key on disk, a new job goes into memory")
	}
}

// A source that runs dry releases what it reserved and never handed over
// (rows that expired or moved), so the backlog returns to exactly what is
// held.
func TestDrySourceReleasesWhatItNeverHandedOver(t *testing.T) {
	q, store := bareQueue(t, make(chan capturedWebhook, 1))
	key := q.storeKey()
	cutoff := time.Now().UnixMilli() + 1
	var stored []*jobs.Job
	for i := 0; i < 3; i++ {
		job := ttlJob(fmt.Sprintf("r-%d", i), "http://x/work", time.Now().Add(-time.Minute))
		store.CheckOrInsert(job)
		store.SetQueueKey(job.ID, key)
		stored = append(stored, job)
	}
	// Five were counted at recovery; two of them vanished before loading.
	q.attachSpill(key, jobs.PageCursor{}, 5, cutoff)
	q.takeSpills()

	queue := q.refill(nil, 0, time.Now())
	if len(queue) != 3 || len(q.spills) != 0 {
		t.Fatalf("loaded %d jobs, %d sources left; want 3 and none", len(queue), len(q.spills))
	}
	if got := q.Backlog(); got != 3 {
		t.Fatalf("backlog = %d after the source ran dry, want the 3 it holds", got)
	}
}

// Refill counts jobs waiting to retry and in flight, not just queued ones.
func TestRefillCountsWaitingAndInFlight(t *testing.T) {
	t.Setenv("AQUIFER_RECOVERY_PAGE_SIZE", "8")
	q, store := bareQueue(t, make(chan capturedWebhook, 1))
	key := q.storeKey()
	for i := 0; i < 20; i++ {
		job := ttlJob(fmt.Sprintf("w-%02d", i), "http://x/work", time.Now().Add(-time.Minute))
		store.CheckOrInsert(job)
		store.SetQueueKey(job.ID, key)
	}
	q.attachSpill(key, jobs.PageCursor{}, 20, time.Now().UnixMilli()+1)
	q.takeSpills()

	// 8 held, waiting out a long Retry-After: nothing more loads.
	if queue := q.refill(nil, 8, time.Now()); len(queue) != 0 {
		t.Fatalf("loaded %d more while a full page waits to retry", len(queue))
	}
	if queue := q.refill(nil, 1, time.Now()); len(queue) != 8 {
		t.Fatalf("loaded %d with one held, want a page of 8", len(queue))
	}
}

// After a panic every source starts over, so jobs its lost loop held load
// again, without counting the ones already handed over twice.
func TestPanicReloadStartsSourcesOverWithoutDoubleCounting(t *testing.T) {
	t.Setenv("AQUIFER_RECOVERY_PAGE_SIZE", "4")
	q, store := bareQueue(t, make(chan capturedWebhook, 1))
	key := q.storeKey()
	for i := 0; i < 6; i++ {
		job := ttlJob(fmt.Sprintf("p-%02d", i), "http://x/work", time.Now().Add(-time.Minute))
		store.CheckOrInsert(job)
		store.SetQueueKey(job.ID, key)
	}
	q.attachSpill(key, jobs.PageCursor{}, 6, time.Now().UnixMilli()+1)
	q.takeSpills()
	first := q.refill(nil, 0, time.Now()) // 4 loaded, 2 left on disk
	if len(first) != 4 || q.Backlog() != 6 {
		t.Fatalf("setup: %d loaded, backlog %d", len(first), q.Backlog())
	}

	q.reloadAfterPanic()
	again := q.refill(nil, 0, time.Now())
	again = q.refill(again, 0, time.Now())
	if len(again) != 6 {
		t.Fatalf("reloaded %d jobs after the panic, want all 6", len(again))
	}
	if len(q.spills) != 0 || q.Backlog() != 6 {
		t.Fatalf("after reloading: %d sources, backlog %d; want none and 6", len(q.spills), q.Backlog())
	}
}
