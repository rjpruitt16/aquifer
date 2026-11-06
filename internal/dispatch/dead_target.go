// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// deadTargetFailAfter is AQUIFER_DEAD_TARGET_FAIL_AFTER_SECONDS: once a
// target has had no successful answer for this long, a job that has itself
// waited this long fails with a webhook instead of waiting behind a target
// that is down. 0 (the default) turns it off: callers that retry until done
// expect accepted work to wait (issue #87).
func deadTargetFailAfter() time.Duration {
	return time.Duration(env.Int64("AQUIFER_DEAD_TARGET_FAIL_AFTER_SECONDS", 0)) * time.Second
}

// deadTargetSweepEvery bounds how often a queue scans itself for such jobs.
const deadTargetSweepEvery = 2 * time.Second

// localRequestError is a failure before a request was sent: it couldn't be
// built or sealed. It says nothing about whether the target is up.
type localRequestError struct{ err error }

func (e localRequestError) Error() string { return e.err.Error() }
func (e localRequestError) Unwrap() error { return e.err }

// IsLocalRequestError reports whether err came from building a request, not
// from sending it.
func IsLocalRequestError(err error) bool {
	var local localRequestError
	return errors.As(err, &local)
}

// ReportAnswer records a direct attempt's outcome (POST /proxy) in the
// target's health, as a queued attempt's is: an answer that isn't a
// retryable failure means it is up.
func (w *URLWorker) ReportAnswer(status int, err error) {
	switch {
	case err != nil && IsLocalRequestError(err):
	case err != nil || retryableStatus(status):
		w.health.failed(time.Now())
	default:
		w.health.answered()
	}
}

// targetHealth is how long a target has gone without a successful answer,
// shared by every queue for it. Any answer that isn't a retryable failure
// (a 2xx, or a 4xx that won't be retried) counts as the target being up.
type targetHealth struct {
	failingSince atomic.Int64 // Unix ms of the first failure since the last answer; 0 when up
}

func (h *targetHealth) answered() { h.failingSince.Store(0) }

func (h *targetHealth) failed(now time.Time) { h.failingSince.CompareAndSwap(0, now.UnixMilli()) }

// failingFor is how long the target has been failing, 0 when it isn't.
func (h *targetHealth) failingFor(now time.Time) time.Duration {
	if h == nil {
		return 0
	}
	since := h.failingSince.Load()
	if since == 0 {
		return 0
	}
	return now.Sub(time.UnixMilli(since))
}

// failDeadTarget fails queued and retry-waiting jobs that have waited at
// least the configured time for a target that has failed for at least as
// long. Newer jobs stay queued and keep trying, so a target that comes back
// is noticed by its next success.
func (q *AccountQueue) failDeadTarget(queue []*jobs.Job, waiting []waitingJob, now time.Time) ([]*jobs.Job, []waitingJob) {
	limit := q.deadAfter
	if limit <= 0 || q.health.failingFor(now) < limit || now.Sub(q.lastDeadSweep) < deadTargetSweepEvery {
		return queue, waiting
	}
	q.lastDeadSweep = now
	cutoff := now.Add(-limit).UnixMilli()
	reason := fmt.Sprintf("target_unavailable: no successful response for %ds", int(q.health.failingFor(now).Seconds()))

	// Like an expired job: nothing is sent, the webhook goes from its own
	// goroutine (it may route back to this queue's buffer), and it counts as
	// served.
	fail := func(job *jobs.Job) {
		q.releaseBacklog()
		q.served.Add(1)
		failQueuedJob(job, reason, "failed", q.upstream, q.store, q.broker, q.metrics, q.enqueueWebhook, q.resultRecorder)
		if q.onJobDone != nil {
			q.onJobDone(job)
		}
	}
	keptQueue := queue[:0]
	for _, job := range queue {
		if job.CreatedAt > 0 && job.CreatedAt <= cutoff {
			fail(job)
			continue
		}
		keptQueue = append(keptQueue, job)
	}
	keptWaiting := waiting[:0]
	for _, w := range waiting {
		if w.job.CreatedAt > 0 && w.job.CreatedAt <= cutoff {
			fail(w.job)
			continue
		}
		keptWaiting = append(keptWaiting, w)
	}
	if len(keptQueue) != len(queue) {
		q.metrics.QueueDepth(q.upstream, len(keptQueue))
	}
	return keptQueue, keptWaiting
}
