// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// A pending row must outlive the job's own deadline, even an execute_before
// further out than the queued TTL, or cleanup deletes it before the queue can
// fail it with a webhook.
func TestPendingRowOutlivesTheJobsDeadline(t *testing.T) {
	far := time.Now().Add(2 * jobs.QueuedTTL).UnixMilli()
	newJob := func(key string) *jobs.Job {
		job := jobs.New(&jobs.Request{UserID: "u", IdempotentKey: key, URL: "http://x", Method: "POST", WebhookURL: "http://w"})
		job.ExecuteBefore = far
		return job
	}

	t.Run("sqlite", func(t *testing.T) {
		s := NewSQLite(filepath.Join(t.TempDir(), "a.db"))
		defer s.Close()
		job := newJob("sqlite")
		s.CheckOrInsert(job)
		expires := func() int64 {
			var v int64
			if err := s.db.QueryRow(`SELECT expires_at FROM jobs WHERE id = ?`, job.ID).Scan(&v); err != nil {
				t.Fatal(err)
			}
			return v
		}
		if got := expires(); got <= far {
			t.Fatalf("inserted row expires %d, before the job's deadline %d", got, far)
		}
		s.UpdateStatus(job.ID, jobs.StatusInFlight)
		if got := expires(); got <= far {
			t.Fatalf("in-flight row expires %d, before the job's deadline %d", got, far)
		}
	})

	t.Run("pebble", func(t *testing.T) {
		s := NewPebble(filepath.Join(t.TempDir(), "p"))
		defer s.Close()
		job := newJob("pebble")
		s.CheckOrInsert(job)
		rec, _ := s.getRecord(job.ID)
		if rec == nil || rec.ExpiresAt <= far {
			t.Fatalf("inserted record %+v expires before the job's deadline %d", rec, far)
		}
		s.UpdateStatus(job.ID, jobs.StatusInFlight)
		rec, _ = s.getRecord(job.ID)
		if rec == nil || rec.ExpiresAt <= far {
			t.Fatalf("in-flight record %+v expires before the job's deadline %d", rec, far)
		}
	})
}
