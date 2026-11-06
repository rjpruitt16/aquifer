// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"log"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// Restart recovery used to load every queued job into memory before serving.
// In storm test 1 a backlog of about 200k jobs ran the process out of memory
// on every restart, and it crash-looped with no traffic at all (issue #88).
// Now each queue starts with one page of its jobs and loads the next as it
// drains; the rest stay on disk, still counted in its backlog.

const defaultRecoveryPageSize = 1000

// RecoveryPageSize is AQUIFER_RECOVERY_PAGE_SIZE: how many jobs a queue with
// more on disk holds at a time (queued, waiting to retry and in flight).
func RecoveryPageSize() int {
	if n := env.Int64("AQUIFER_RECOVERY_PAGE_SIZE", defaultRecoveryPageSize); n > 0 {
		return int(n)
	}
	return defaultRecoveryPageSize
}

// spillSweepEvery is how often a queue with jobs on disk fails the ones whose
// queue deadline passed before they could load (see sweepSpilled).
const spillSweepEvery = time.Minute

// recentPageWindow is how long after loading a page of its own key a queue
// checks a hand-off's stored status: a job it loaded and finished may still
// have its hand-off on the way.
const recentPageWindow = 5 * time.Minute

// refillRetryEvery spaces retries after a failed page read.
const refillRetryEvery = time.Second

// spillSource is a run of a queue's jobs still on disk: those stored under
// key after the cursor. reserved of them were counted in the queue's backlog
// when it was attached; consumed of those have since loaded or failed. Only
// jobs created before cutoff, and after consumedThrough, were among them.
// When the source runs dry, what was reserved but never consumed (rows that
// expired, moved or were deleted) is released, so the backlog stays exact.
type spillSource struct {
	key             string
	after           jobs.PageCursor
	reserved        int
	consumed        int
	cutoff          int64
	consumedThrough jobs.PageCursor
}

func (src *spillSource) counts(job *jobs.Job) bool {
	return job.CreatedAt < src.cutoff && !src.consumedThrough.Covers(job) && src.consumed < src.reserved
}

// StoreQueueKey is the key a queue's jobs are stored under: target and
// tenant. The tenant key alone (or "shared") is the same for every target.
func StoreQueueKey(upstream, queueKey string) string { return upstream + "|" + queueKey }

func (q *AccountQueue) storeKey() string { return StoreQueueKey(q.upstream, q.key) }

// attachSpill tells the queue it has jobs on disk under key after the
// cursor, remaining of them created before cutoff (Unix ms). Those count in
// its backlog now; it loads them a page at a time. Safe from any goroutine.
func (q *AccountQueue) attachSpill(key string, after jobs.PageCursor, remaining int, cutoff int64) {
	for i := 0; i < remaining; i++ {
		q.reserveBacklog()
	}
	q.spillMu.Lock()
	q.spillPending = append(q.spillPending, spillSource{key: key, after: after, reserved: remaining, cutoff: cutoff})
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

// reloadAfterPanic starts every source over, plus this queue's own key, so
// the jobs its lost loop held load again. What each source already handed
// over keeps its count (consumedThrough), so nothing is counted twice.
func (q *AccountQueue) reloadAfterPanic() {
	q.takeSpills()
	own := q.storeKey()
	hasOwn := false
	for i := range q.spills {
		src := &q.spills[i]
		// consumedThrough only moves forward: after a second panic, after
		// may still be behind where the first one left it.
		if !src.consumedThrough.Covers(&jobs.Job{CreatedAt: src.after.CreatedAt, ID: src.after.ID}) {
			src.consumedThrough = src.after
		}
		src.after = jobs.PageCursor{}
		hasOwn = hasOwn || src.key == own
	}
	if !hasOwn {
		q.spills = append(q.spills, spillSource{key: own})
	}
	q.present = nil
	q.pagedOwnUntil = time.Now().Add(recentPageWindow)
}

// hold and drop track the jobs this queue holds in memory: queued, waiting
// to retry or in flight. A page row or hand-off for one of them is skipped.
func (q *AccountQueue) hold(job *jobs.Job) {
	if q.present == nil {
		q.present = map[string]struct{}{}
	}
	q.present[job.ID] = struct{}{}
}

func (q *AccountQueue) drop(job *jobs.Job) {
	if job != nil {
		delete(q.present, job.ID)
	}
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
// Not if it's already held; not if it's past the cursor of this queue's own
// key on disk (it's stored there and loads in its turn); and not if a page
// already loaded and finished it.
func (q *AccountQueue) acceptHandoff(job *jobs.Job, now time.Time) bool {
	if _, ok := q.present[job.ID]; ok {
		return false
	}
	if after, ok := q.ownSpill(); ok && !after.Covers(job) {
		return false
	}
	if now.Before(q.pagedOwnUntil) {
		if stored := q.store.GetJob(job.ID); stored != nil && stored.Status != jobs.StatusQueued {
			return false
		}
	}
	return true
}

// refill loads the next page while the queue holds (queued, waiting to
// retry and in flight) less than a quarter page, so retries waiting out a
// long Retry-After don't pull in page after page.
func (q *AccountQueue) refill(queue []*jobs.Job, held int, now time.Time) []*jobs.Job {
	if now.Before(q.refillRetryAt) {
		return queue
	}
	size := RecoveryPageSize()
	low := max(size/4, 1)
	own := q.storeKey()
	for held < low && len(q.spills) > 0 {
		src := &q.spills[0]
		page, err := q.store.QueuedPage(src.key, src.after, 0, size)
		if err != nil {
			// Keep the source and its count; try again shortly.
			log.Printf("[AccountQueue] %s: reading jobs from storage failed, retrying: %v", q.key, err)
			q.refillRetryAt = now.Add(refillRetryEvery)
			break
		}
		for _, job := range page {
			counted := src.counts(job)
			if counted {
				src.consumed++
			}
			if _, ok := q.present[job.ID]; ok {
				// Already held with its own count; drop the reserved one.
				if counted {
					q.releaseBacklog()
				}
				continue
			}
			q.hold(job)
			queue = append(queue, job)
			held++
			q.trackDeadline(job)
		}
		if len(page) > 0 {
			src.after = jobs.CursorAt(page[len(page)-1])
		}
		if src.key == own && len(page) > 0 {
			q.pagedOwnUntil = now.Add(recentPageWindow)
		}
		if len(page) < size {
			q.finishSpill(0)
		}
		q.reportDepth(len(queue))
	}
	return queue
}

// finishSpill removes source i, which has run dry, releasing what it
// reserved and never handed over.
func (q *AccountQueue) finishSpill(i int) {
	src := q.spills[i]
	for n := src.reserved - src.consumed; n > 0; n-- {
		q.releaseBacklog()
	}
	q.spills = append(q.spills[:i], q.spills[i+1:]...)
}

// sweepSpilled fails jobs still on disk whose queue deadline passed: a queue
// stuck on a target that is down never drains far enough to load them, and
// their stored rows would otherwise expire without a webhook. Every such
// job's default deadline is QueuedTTL after creation, so the sweep reads
// only jobs that old, all of them (a page at a time), each minute. A job
// with an earlier execute_before fails at that point at the latest, before
// its row can expire (rows outlive the TTL).
func (q *AccountQueue) sweepSpilled(now time.Time) {
	if len(q.spills) == 0 || now.Sub(q.lastSpillSweep) < spillSweepEvery {
		return
	}
	q.lastSpillSweep = now
	before := now.Add(-jobs.QueuedTTL).UnixMilli()
	size := RecoveryPageSize()
	for i := range q.spills {
		src := &q.spills[i]
		cursor := src.after
		for {
			page, err := q.store.QueuedPage(src.key, cursor, before, size)
			if err != nil {
				log.Printf("[AccountQueue] %s: sweeping jobs on disk failed: %v", q.key, err)
				break
			}
			for _, job := range page {
				if _, held := q.present[job.ID]; held || !job.QueueExpired(now) {
					continue
				}
				// Every job on disk holds one backlog count: the source's, or
				// its own from its hand-off.
				if src.counts(job) {
					src.consumed++
				}
				q.releaseBacklog()
				failExpiredJob(job, "", q.upstream, q.store, q.broker, q.metrics, q.enqueueWebhook, q.resultRecorder)
				if q.onJobDone != nil {
					q.onJobDone(job)
				}
			}
			if len(page) < size {
				break
			}
			cursor = jobs.CursorAt(page[len(page)-1])
		}
	}
}

// queueFor is the queue job would be handed to, if it exists.
func (w *URLWorker) queueFor(job *jobs.Job) *AccountQueue {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.queues[w.queueKeyLocked(job)]
}

func (w *URLWorker) queueKeyLocked(job *jobs.Job) string {
	if w.accountQueueMode {
		return JobQueueKey(job)
	}
	return SharedQueueKey
}

// StoreKeyFor is the key job would be stored under if it were handed to its
// queue now: recovery pages a stored key only when its jobs still route to
// the queue that owns it.
func (r *Registry) StoreKeyFor(job *jobs.Job) string {
	w := r.WorkerFor(job)
	if w == nil {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return StoreQueueKey(w.domain, w.queueKeyLocked(job))
}

// AttachRecoveredBacklog tells the queue that job was recovered into that it
// has remaining more jobs on disk under storeKey after the cursor, all
// created before cutoff. It reports false if there is no such queue.
func (r *Registry) AttachRecoveredBacklog(job *jobs.Job, storeKey string, after jobs.PageCursor, remaining int, cutoff int64) bool {
	w := r.WorkerFor(job)
	if w == nil {
		return false
	}
	q := w.queueFor(job)
	if q == nil {
		return false
	}
	q.attachSpill(storeKey, after, remaining, cutoff)
	return true
}
