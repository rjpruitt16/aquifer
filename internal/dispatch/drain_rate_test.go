// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
)

func TestBacklogStuckComparesBacklogWithTheDrainRate(t *testing.T) {
	w := &URLWorker{}
	now := time.Now()
	limit := 60 * time.Second

	w.backlog.Store(100)
	if w.BacklogStuck(limit) {
		t.Fatal("a target not yet measured must not count as stuck")
	}
	w.sampleDrain(now)
	w.health.served.Add(30) // 10 jobs/s over 3 s: 100 queued drain in 10 s
	w.sampleDrain(now.Add(3 * time.Second))
	if w.BacklogStuck(limit) {
		t.Fatal("a backlog that drains in about 10 s is not stuck")
	}

	// Nothing finishes for a while: the smoothed rate falls toward 0.
	for i := 2; i < 12; i++ {
		w.sampleDrain(now.Add(time.Duration(3*i) * time.Second))
	}
	if !w.BacklogStuck(limit) {
		t.Fatal("a backlog that stopped draining should be stuck")
	}
	w.backlog.Store(0)
	if w.BacklogStuck(limit) {
		t.Fatal("no backlog is never stuck")
	}

	// Idle for a while, then work resumes: the old near-zero rate is
	// forgotten, so the target isn't stuck until it is measured again.
	w.sampleDrain(now.Add(60 * time.Second))
	w.backlog.Store(5)
	if w.BacklogStuck(limit) {
		t.Fatal("a target that was idle is judged by its pace before going idle")
	}
}

// A job leaving by expiry counts toward the drain rate, and the total
// survives the queue it left from.
func TestExpiredJobsCountTowardTheTargetsDrain(t *testing.T) {
	health := &targetHealth{}
	q := &AccountQueue{health: health, store: storage.NewSQLite(t.TempDir() + "/a.db"), broker: NewBroker(), metrics: NoopMetricsAdapter{}, enqueueWebhook: func(string, string, string, int, map[string]any) {}}
	t.Cleanup(func() { q.store.Close() })
	q.nextDeadline = 1
	expired := &jobs.Job{ID: jobs.NewID(), CreatedAt: time.Now().Add(-jobs.QueuedTTL - time.Second).UnixMilli()}
	q.dropExpiredJobs([]*jobs.Job{expired})
	if got := health.served.Load(); got != 1 {
		t.Fatalf("target served = %d after one expiry, want 1", got)
	}
}
