// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// Restart recovery used to load every queued job into memory before serving.
// In storm test 1 a backlog of about 200k jobs ran the process out of memory
// on every restart, and it crash-looped with no traffic at all (issue #88).
// Now each queue starts with one page of its jobs and loads the next page as
// it drains; the rest stay on disk, still counted in its backlog.

const defaultRecoveryPageSize = 1000

// RecoveryPageSize is AQUIFER_RECOVERY_PAGE_SIZE: how many of a queue's
// stored jobs it holds in memory at a time while it has more on disk.
func RecoveryPageSize() int {
	if n := env.Int64("AQUIFER_RECOVERY_PAGE_SIZE", defaultRecoveryPageSize); n > 0 {
		return int(n)
	}
	return defaultRecoveryPageSize
}

// spillSweepEvery is how often a queue with jobs on disk fails the ones whose
// queue deadline passed before they could load (see sweepSpilled).
const spillSweepEvery = time.Minute

// recentLoadWindow is how long a job loaded from a page is remembered, so
// its own hand-off, still on its way through q.cmds, isn't queued twice.
const recentLoadWindow = time.Minute

// spillSource is a run of a queue's jobs still on disk: those stored under
// key after the cursor.
type spillSource struct {
	key   string
	after jobs.PageCursor
}

// StoreQueueKey is the key a queue's jobs are stored under: target and
// tenant. The tenant key alone (or "shared") is the same for every target.
func StoreQueueKey(upstream, queueKey string) string { return upstream + "|" + queueKey }

func (q *AccountQueue) storeKey() string { return StoreQueueKey(q.upstream, q.key) }

// attachSpill tells the queue it has remaining more jobs on disk under key
// after the cursor. They count in its backlog now; it loads them a page at
// a time. Safe from any goroutine.
func (q *AccountQueue) attachSpill(key string, after jobs.PageCursor, remaining int) {
	for i := 0; i < remaining; i++ {
		q.reserveBacklog()
	}
	q.spillMu.Lock()
	q.spillPending = append(q.spillPending, spillSource{key: key, after: after})
	q.spillMu.Unlock()
	select {
	case q.spillWake <- struct{}{}:
	default:
	}
}

// takeSpills moves attached sources into the run loop's own list.
func (q *AccountQueue) takeSpills() {
	q.spillMu.Lock()
	pending := q.spillPending
	q.spillPending = nil
	q.spillMu.Unlock()
	q.spills = append(q.spills, pending...)
}

// ownSpill is the cursor of this queue's own key while jobs stored under it
// are still on disk.
func (q *AccountQueue) ownSpill() (jobs.PageCursor, bool) {
	own := q.storeKey()
	for _, src := range q.spills {
		if src.key == own {
			return src.after, true
		}
	}
	return jobs.PageCursor{}, false
}

// acceptHandoff reports whether a job arriving on q.cmds goes into memory.
// One past the cursor of this queue's own key on disk is stored there
// already and will load in its turn; one just loaded from a page is
// already in memory.
func (q *AccountQueue) acceptHandoff(job *jobs.Job) bool {
	if after, ok := q.ownSpill(); ok && !after.Covers(job) {
		return false
	}
	if _, ok := q.recentLoads[job.ID]; ok {
		delete(q.recentLoads, job.ID)
		return false
	}
	return true
}

// refill loads the next page while memory holds less than a quarter page.
func (q *AccountQueue) refill(queue []*jobs.Job, now time.Time) []*jobs.Job {
	size := RecoveryPageSize()
	low := max(size/4, 1)
	own := q.storeKey()
	for len(queue) < low && len(q.spills) > 0 {
		src := &q.spills[0]
		page := q.store.QueuedPage(src.key, src.after, 0, size)
		for _, job := range page {
			if src.key == own {
				if q.recentLoads == nil {
					q.recentLoads = map[string]time.Time{}
				}
				q.recentLoads[job.ID] = now
			}
			queue = append(queue, job)
			q.trackDeadline(job)
		}
		if len(page) > 0 {
			src.after = jobs.CursorAt(page[len(page)-1])
		}
		if len(page) < size {
			q.spills = q.spills[1:]
		}
		q.reportDepth(len(queue))
	}
	return queue
}

// sweepSpilled fails jobs still on disk whose queue deadline passed: a queue
// stuck on a target that is down never drains far enough to load them, and
// their stored rows would otherwise expire without a webhook. Their default
// deadline is QueuedTTL after creation, so only jobs that old are read.
func (q *AccountQueue) sweepSpilled(now time.Time) {
	for id, at := range q.recentLoads {
		if now.Sub(at) > recentLoadWindow {
			delete(q.recentLoads, id)
		}
	}
	if len(q.spills) == 0 || now.Sub(q.lastSpillSweep) < spillSweepEvery {
		return
	}
	q.lastSpillSweep = now
	before := now.Add(-jobs.QueuedTTL).UnixMilli()
	for _, src := range q.spills {
		for _, job := range q.store.QueuedPage(src.key, src.after, before, RecoveryPageSize()) {
			if !job.QueueExpired(now) {
				continue
			}
			q.releaseBacklog()
			failExpiredJob(job, "", q.upstream, q.store, q.broker, q.metrics, q.enqueueWebhook, q.resultRecorder)
			if q.onJobDone != nil {
				q.onJobDone(job)
			}
		}
	}
}

// queueFor is the queue job would be handed to, if it exists.
func (w *URLWorker) queueFor(job *jobs.Job) *AccountQueue {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := SharedQueueKey
	if w.accountQueueMode {
		key = JobQueueKey(job)
	}
	return w.queues[key]
}

// AttachRecoveredBacklog tells the queue that job was recovered into that it
// has remaining more jobs on disk under storeKey after the cursor. It
// reports false if there is no such queue.
func (r *Registry) AttachRecoveredBacklog(job *jobs.Job, storeKey string, after jobs.PageCursor, remaining int) bool {
	w := r.WorkerFor(job)
	if w == nil {
		return false
	}
	q := w.queueFor(job)
	if q == nil {
		return false
	}
	q.attachSpill(storeKey, after, remaining)
	return true
}
