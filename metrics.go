// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "time"

type MetricsAdapter interface {
	JobQueued(userID, upstream string)
	JobDispatched(userID, upstream string)
	JobCompleted(userID, upstream string, durationMs int64)
	JobFailed(userID, upstream string, reason string)
	WebhookDelivered(url string, attempt int)
	WebhookFailed(url string, attempts int)
	QueueDepth(upstream string, depth int)
	FlowRate(upstream string, rps float64)
	// DrainFlushSucceeded/DrainFlushFailed only ever fire when drain mode
	// is enabled (see drain.go) -- unreached on a deployment that never
	// turns it on.
	DrainFlushSucceeded(instanceKey string, ledgerSize int)
	DrainFlushFailed(instanceKey string, ledgerSize int)
}

type NoopMetricsAdapter struct{}

func (NoopMetricsAdapter) JobQueued(userID, upstream string)                      {}
func (NoopMetricsAdapter) JobDispatched(userID, upstream string)                  {}
func (NoopMetricsAdapter) JobCompleted(userID, upstream string, durationMs int64) {}
func (NoopMetricsAdapter) JobFailed(userID, upstream string, reason string)       {}
func (NoopMetricsAdapter) WebhookDelivered(url string, attempt int)               {}
func (NoopMetricsAdapter) WebhookFailed(url string, attempts int)                 {}
func (NoopMetricsAdapter) QueueDepth(upstream string, depth int)                  {}
func (NoopMetricsAdapter) FlowRate(upstream string, rps float64)                  {}
func (NoopMetricsAdapter) DrainFlushSucceeded(instanceKey string, ledgerSize int) {}
func (NoopMetricsAdapter) DrainFlushFailed(instanceKey string, ledgerSize int)    {}

func ensureMetrics(metrics MetricsAdapter) MetricsAdapter {
	if metrics == nil {
		return NoopMetricsAdapter{}
	}
	return metrics
}

// The interfaces below are optional: a MetricsAdapter that also implements
// one gets those events too. They're separate so that adapters written
// against the interface above keep compiling.

// JobTimingMetrics learns how long jobs wait in Aquifer and take end to end.
type JobTimingMetrics interface {
	// JobWaited fires at a job's first dispatch: how long it waited in line.
	JobWaited(userID, upstream string, wait time.Duration)
	// JobFinished fires once per job, from submission to its terminal state.
	// outcome is "completed", "failed" or "expired".
	JobFinished(userID, upstream, outcome string, total time.Duration)
}

// DispatchMetrics learns the result of every attempt at a target API,
// retries included. status is 0 when no response came back.
type DispatchMetrics interface {
	DispatchAttempt(upstream string, status int)
}

// AdmissionMetrics learns why Aquifer refused jobs (429): "memory",
// "db_size", "sync_lag", "upstream_queue", "queue_fair_share", ...
type AdmissionMetrics interface {
	AdmissionRejected(reason string)
}

// sinceCreated is how long ago a job was submitted (CreatedAt is Unix ms).
func sinceCreated(job *Job, now time.Time) time.Duration {
	if job.CreatedAt <= 0 {
		return 0
	}
	return max(now.Sub(time.UnixMilli(job.CreatedAt)), 0)
}

func observeJobWaited(metrics MetricsAdapter, job *Job, upstream string) {
	if t, ok := metrics.(JobTimingMetrics); ok && job.Attempts == 0 && !job.isWebhookDeliveryJob() {
		t.JobWaited(job.UserID, upstream, sinceCreated(job, time.Now()))
	}
}

func observeJobFinished(metrics MetricsAdapter, job *Job, upstream, outcome string) {
	if t, ok := metrics.(JobTimingMetrics); ok && !job.isWebhookDeliveryJob() {
		t.JobFinished(job.UserID, upstream, outcome, sinceCreated(job, time.Now()))
	}
}

func observeDispatch(metrics MetricsAdapter, upstream string, status int) {
	if d, ok := metrics.(DispatchMetrics); ok {
		d.DispatchAttempt(upstream, status)
	}
}

func observeRejected(metrics MetricsAdapter, reason string) {
	if a, ok := metrics.(AdmissionMetrics); ok {
		a.AdmissionRejected(reason)
	}
}
