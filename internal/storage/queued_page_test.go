// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

func eachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("sqlite", func(t *testing.T) {
		s := NewSQLite(filepath.Join(t.TempDir(), "a.db"))
		defer s.Close()
		fn(t, s)
	})
	t.Run("pebble", func(t *testing.T) {
		s := NewPebble(filepath.Join(t.TempDir(), "p"))
		defer s.Close()
		fn(t, s)
	})
}

func mustCounts(t *testing.T, s Store) map[string]int {
	t.Helper()
	counts, err := s.PendingQueueKeys()
	if err != nil {
		t.Fatal(err)
	}
	return counts
}

func mustPage(t *testing.T, s Store, key string, after jobs.PageCursor, before int64, limit int) []*jobs.Job {
	t.Helper()
	page, err := s.QueuedPage(key, after, before, limit)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func pagedJob(s Store, key string, n int, createdAt int64) *jobs.Job {
	job := jobs.New(&jobs.Request{UserID: "u", IdempotentKey: fmt.Sprintf("%s-%d", key, n), URL: "http://x", Method: "POST", WebhookURL: "http://w"})
	job.CreatedAt = createdAt
	s.CheckOrInsert(job)
	s.SetQueueKey(job.ID, key)
	return job
}

func TestQueuedPagesReadAQueueInOrder(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		const base = int64(1_700_000_000_000)
		var mine []*jobs.Job
		for i := 0; i < 25; i++ {
			mine = append(mine, pagedJob(s, "http://x|alice", i, base+int64(i)))
			pagedJob(s, "http://x|bob", i, base+int64(i))
		}
		never := jobs.New(&jobs.Request{UserID: "u", IdempotentKey: "never-handed", URL: "http://x", Method: "POST", WebhookURL: "http://w"})
		s.CheckOrInsert(never)

		counts := mustCounts(t, s)
		if counts["http://x|alice"] != 25 || counts["http://x|bob"] != 25 || counts[""] != 1 {
			t.Fatalf("PendingQueueKeys = %v", counts)
		}

		// Pages of 10 return alice's jobs in creation order, each once.
		var got []*jobs.Job
		cursor := jobs.PageCursor{}
		for {
			page := mustPage(t, s, "http://x|alice", cursor, 0, 10)
			got = append(got, page...)
			if len(page) < 10 {
				break
			}
			cursor = jobs.CursorAt(page[len(page)-1])
		}
		if len(got) != 25 {
			t.Fatalf("paged %d jobs, want 25", len(got))
		}
		for i, job := range got {
			if job.ID != mine[i].ID || job.Status != jobs.StatusQueued {
				t.Fatalf("page position %d: got %s (%s), want %s", i, job.ID, job.Status, mine[i].ID)
			}
		}

		// A finished job leaves the queue's pages and its count.
		s.UpdateStatus(mine[0].ID, jobs.StatusCompleted)
		if page := mustPage(t, s, "http://x|alice", jobs.PageCursor{}, 0, 1); len(page) != 1 || page[0].ID != mine[1].ID {
			t.Fatalf("after finishing the first job, the first page starts with %v", page)
		}
		if n := mustCounts(t, s)["http://x|alice"]; n != 24 {
			t.Fatalf("count after one finished = %d, want 24", n)
		}

		// createdBefore stops before jobs created at or after it.
		if page := mustPage(t, s, "http://x|alice", jobs.PageCursor{}, base+5, 100); len(page) != 4 {
			t.Fatalf("jobs created before base+5 (minus the finished one) = %d, want 4", len(page))
		}

		// Moving a job to another queue moves it between pages.
		s.SetQueueKey(mine[2].ID, "http://x|carol")
		if page := mustPage(t, s, "http://x|carol", jobs.PageCursor{}, 0, 10); len(page) != 1 || page[0].ID != mine[2].ID {
			t.Fatalf("carol's page = %v", page)
		}
		for _, job := range mustPage(t, s, "http://x|alice", jobs.PageCursor{}, 0, 100) {
			if job.ID == mine[2].ID {
				t.Fatal("a job moved to another queue still pages under its old key")
			}
		}
	})
}

// A Pebble store written before the queue index existed is backfilled the
// first time recovery counts it.
func TestPebbleBackfillsTheQueueIndex(t *testing.T) {
	s := NewPebble(filepath.Join(t.TempDir(), "p"))
	defer s.Close()
	for i := 0; i < 5; i++ {
		pagedJob(s, "http://x|old", i, int64(1_700_000_000_000+i))
	}
	// Forget the index, as an older version would have written the store.
	iter, _ := s.db.NewIter(nil)
	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		if k := iter.Key(); len(k) > 3 && string(k[:3]) == "qk:" {
			keys = append(keys, append([]byte(nil), k...))
		}
	}
	iter.Close()
	for _, k := range keys {
		s.db.Delete(k, nil)
	}
	s.db.Delete(queueIndexMarker, nil)
	if page := mustPage(t, s, "http://x|old", jobs.PageCursor{}, 0, 10); len(page) != 0 {
		t.Fatalf("setup: index still present: %d", len(page))
	}

	if n := mustCounts(t, s)["http://x|old"]; n != 5 {
		t.Fatalf("count = %d, want 5", n)
	}
	if page := mustPage(t, s, "http://x|old", jobs.PageCursor{}, 0, 10); len(page) != 5 {
		t.Fatalf("after backfill the queue pages %d jobs, want 5", len(page))
	}
}

// A queue whose index starts with many stale entries still pages its live
// jobs, and the stale entries are dropped as they're met.
func TestPebblePageSkipsAndDropsAStalePrefix(t *testing.T) {
	s := NewPebble(filepath.Join(t.TempDir(), "p"))
	defer s.Close()
	const base = int64(1_700_000_000_000)
	for i := 0; i < 2_500; i++ {
		job := pagedJob(s, "http://x|stale", i, base+int64(i))
		s.UpdateStatus(job.ID, jobs.StatusCompleted)
		// Leave its index entry behind, as a crash between writes would.
		s.db.Set(queueIndexKey("http://x|stale", job.CreatedAt, job.ID), nil, nil)
	}
	live := pagedJob(s, "http://x|stale", 9_999, base+10_000)

	page := mustPage(t, s, "http://x|stale", jobs.PageCursor{}, 0, 10)
	if len(page) != 1 || page[0].ID != live.ID {
		t.Fatalf("page = %d jobs, want only the live one", len(page))
	}
	iter, _ := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("qk:http://x|stale\x00"), UpperBound: []byte("qk:http://x|stale\x01")})
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	iter.Close()
	if n != 1 {
		t.Fatalf("%d index entries left, want only the live job's", n)
	}
}
