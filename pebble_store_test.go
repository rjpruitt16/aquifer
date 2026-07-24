package aquifer

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestPebbleStoreHandlesConcurrentInserts mirrors
// TestStoreHandlesConcurrentInserts — the same regression class (a
// lookup-then-write race under real concurrency) applies here just as it
// did for both the SQLite and Mnesia stores this session. The shard lock
// in CheckOrInsert is what's meant to prevent it.
func TestPebbleStoreHandlesConcurrentInserts(t *testing.T) {
	dir := t.TempDir()
	store := NewPebbleStore(filepath.Join(dir, "pebble"))
	t.Cleanup(func() { store.Close() })

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan string, n)

	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			job := &Job{
				ID:            generateID(),
				UserID:        "concurrent-user",
				IdempotentKey: generateID(),
				URL:           "https://example.com/webhook",
				Method:        "POST",
				WebhookURL:    "https://example.com/callback",
				Status:        StatusQueued,
				CreatedAt:     time.Now().UnixMilli(),
			}
			if _, isDuplicate := store.CheckOrInsert(job); isDuplicate {
				errs <- "unexpected duplicate for a unique idempotent key"
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	elapsed := time.Since(start)

	for e := range errs {
		t.Error(e)
	}

	if elapsed > 5*time.Second {
		t.Fatalf("%d concurrent inserts took %s — looks serialized through a single lock again", n, elapsed)
	}

	counts := store.Counts()
	if counts.TotalJobs != n {
		t.Fatalf("expected %d jobs recorded, got %d", n, counts.TotalJobs)
	}
}

func TestPebbleStoreDuplicateDetection(t *testing.T) {
	dir := t.TempDir()
	store := NewPebbleStore(filepath.Join(dir, "pebble"))
	t.Cleanup(func() { store.Close() })

	job := &Job{
		ID:            generateID(),
		UserID:        "dup-user",
		IdempotentKey: "dup-key",
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}

	if _, isDuplicate := store.CheckOrInsert(job); isDuplicate {
		t.Fatal("first insert should not be a duplicate")
	}

	retry := &Job{
		ID:            generateID(),
		UserID:        "dup-user",
		IdempotentKey: "dup-key",
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}

	existingID, isDuplicate := store.CheckOrInsert(retry)
	if !isDuplicate {
		t.Fatal("resubmission with the same idempotent key should be a duplicate")
	}
	if existingID != job.ID {
		t.Fatalf("expected existing id %s, got %s", job.ID, existingID)
	}
}

// TestPebbleStoreDeleteJobLeavesNoGhostIndex mirrors the SQLite
// DeleteJob contract: deleting a rejected job must also drop its
// idempotency index entry, or a retry of the same key would forever see
// a "duplicate" pointing at a job that no longer exists.
func TestPebbleStoreDeleteJobLeavesNoGhostIndex(t *testing.T) {
	dir := t.TempDir()
	store := NewPebbleStore(filepath.Join(dir, "pebble"))
	t.Cleanup(func() { store.Close() })

	job := &Job{
		ID:            generateID(),
		UserID:        "del-user",
		IdempotentKey: "del-key",
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}

	if _, isDuplicate := store.CheckOrInsert(job); isDuplicate {
		t.Fatal("expected a fresh insert")
	}

	store.DeleteJob(job.ID)

	if got := store.GetJob(job.ID); got != nil {
		t.Fatalf("expected job to be gone after DeleteJob, got %+v", got)
	}

	retry := &Job{
		ID:            generateID(),
		UserID:        "del-user",
		IdempotentKey: "del-key",
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}

	if _, isDuplicate := store.CheckOrInsert(retry); isDuplicate {
		t.Fatal("expected the same idempotent key to be treated as fresh after DeleteJob removed the prior entry")
	}
}
