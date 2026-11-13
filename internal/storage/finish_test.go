// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"github.com/cockroachdb/pebble"
	"github.com/rjpruitt16/aquifer/internal/jobs"
	"path/filepath"
	"sync"
	"testing"
)

func TestFinishSurvivesReopen(t *testing.T) {
	t.Setenv("AQUIFER_PEBBLE_FLUSH_INTERVAL_MS", "0")
	for _, backend := range []string{"sqlite", "pebble"} {
		for _, status := range []jobs.Status{jobs.StatusCompleted, jobs.StatusFailed} {
			t.Run(backend+"/"+string(status), func(t *testing.T) {
				t.Setenv("AQUIFER_STORE_BACKEND", backend)
				path := filepath.Join(t.TempDir(), "jobs")
				s := Open(path)
				req := sampleJobRequest("user", "key")
				job := jobs.New(&req)
				s.CheckOrInsert(job)
				result := jobs.Result{JobID: job.ID, Status: status, Body: "saved response", ResponseStatus: 201}
				if err := s.Finish(job.ID, status, &result); err != nil {
					t.Fatal(err)
				}
				if got := s.Counts(); got.TotalJobs != 0 || got.QueueDepth != 0 {
					t.Fatalf("terminal job still counted: %+v", got)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s = Open(path)
				defer s.Close()
				if got := s.GetJob(job.ID); got == nil || got.Status != status {
					t.Fatalf("status after reopen: %+v", got)
				}
				if got, ok := s.GetResult(job.ID); !ok || got != result {
					t.Fatalf("result after reopen: %+v, %v", got, ok)
				}
				if len(s.GetQueuedJobs()) != 0 {
					t.Fatal("terminal job recovered for execution")
				}
				if err := s.Finish("missing", status, &result); err == nil {
					t.Fatal("missing job must return an error")
				}
			})
		}
	}
}

func TestFinishFailureLeavesJobQueued(t *testing.T) {
	for _, backend := range []string{"sqlite", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			t.Setenv("AQUIFER_STORE_BACKEND", backend)
			t.Setenv("AQUIFER_PEBBLE_FLUSH_INTERVAL_MS", "0")
			path := filepath.Join(t.TempDir(), "jobs")
			s := Open(path)
			req := sampleJobRequest("user", "key")
			job := jobs.New(&req)
			s.CheckOrInsert(job)
			if sql, ok := s.(*SQLite); ok {
				_, err := sql.db.Exec(`CREATE TRIGGER reject_finish BEFORE UPDATE ON jobs WHEN NEW.status = 'completed' BEGIN SELECT RAISE(ABORT, 'injected write failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
			} else {
				s.Close()
				db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				p := &Pebble{db: db, syncOpts: pebble.Sync}
				p.skipDrainEvents.Store(true)
				s = p
			}
			before := s.Counts()
			result := jobs.Result{JobID: job.ID, Status: jobs.StatusCompleted, Body: "response"}
			if err := s.Finish(job.ID, jobs.StatusCompleted, &result); err == nil {
				t.Fatal("write failure was swallowed")
			}
			if got := s.GetJob(job.ID); got == nil || got.Status != jobs.StatusQueued {
				t.Fatalf("failed commit changed status: %+v", got)
			}
			if _, ok := s.GetResult(job.ID); ok {
				t.Fatal("failed commit saved a partial result")
			}
			if got := s.Counts(); got != before {
				t.Fatalf("failed commit changed counts: %+v -> %+v", before, got)
			}
			if len(s.ListDrainEvents(10)) != 0 {
				t.Fatal("failed commit emitted terminal drain event")
			}
		})
	}
}

func TestPebbleConcurrentFinishCountsOnce(t *testing.T) {
	s := NewPebble(filepath.Join(t.TempDir(), "jobs"))
	defer s.Close()
	req := sampleJobRequest("user", "key")
	job := jobs.New(&req)
	s.CheckOrInsert(job)
	s.SetQueueKey(job.ID, "queue")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Finish(job.ID, jobs.StatusCompleted, &jobs.Result{JobID: job.ID, Status: jobs.StatusCompleted}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := s.Counts(); got.TotalJobs != 0 || got.QueueDepth != 0 {
		t.Fatalf("incorrect counts: %+v", got)
	}
	if _, closer, err := s.db.Get(queueIndexKey("queue", job.CreatedAt, job.ID)); err != pebble.ErrNotFound {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("terminal job left in queue index: %v", err)
	}
}
