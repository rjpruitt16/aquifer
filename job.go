// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/jobs"

// The job model lives in internal/jobs. These names are the public API
// embedding applications use to submit and read jobs.
type (
	Job         = jobs.Job
	JobRequest  = jobs.Request
	JobResult   = jobs.Result
	Status      = jobs.Status
	LedgerEntry = jobs.LedgerEntry
	DrainEvent  = jobs.DrainEvent
)

const (
	StatusQueued    = jobs.StatusQueued
	StatusInFlight  = jobs.StatusInFlight
	StatusCompleted = jobs.StatusCompleted
	StatusFailed    = jobs.StatusFailed

	DefaultMaxRetries        = jobs.DefaultMaxRetries
	RetryUntilComplete       = jobs.RetryUntilComplete
	MaxAllowedRetries        = jobs.MaxAllowedRetries
	DefaultWebhookMaxRetries = jobs.DefaultWebhookMaxRetries

	IdempotencyScopeUser   = jobs.IdempotencyScopeUser
	IdempotencyScopeShared = jobs.IdempotencyScopeShared
)

func NewJob(r *JobRequest) *Job { return jobs.New(r) }
