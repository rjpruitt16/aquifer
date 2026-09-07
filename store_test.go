package aquifer

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestStoreListAndClearIdempotentKeys covers drain mode's store-level
// primitives: enumerate returns exactly what was inserted, and clear wipes
// it so a previously-duplicate key is seen as fresh afterward.
func TestStoreListAndClearIdempotentKeys(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() { store.Close() })

	job1 := &Job{ID: generateID(), UserID: "u1", IdempotentKey: "k1", URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com", Status: StatusQueued, CreatedAt: time.Now().UnixMilli()}
	job2 := &Job{ID: generateID(), UserID: "u2", IdempotentKey: "k2", URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com", Status: StatusQueued, CreatedAt: time.Now().UnixMilli()}
	store.CheckOrInsert(job1)
	store.CheckOrInsert(job2)
	store.UpdateStatus(job2.ID, StatusCompleted)

	entries := store.ListIdempotentKeys()
	if len(entries) != 2 {
		t.Fatalf("expected 2 ledger entries, got %d", len(entries))
	}
	byJobID := map[string]LedgerEntry{}
	for _, e := range entries {
		byJobID[e.JobID] = e
	}
	if e, ok := byJobID[job2.ID]; !ok || e.Status != StatusCompleted {
		t.Fatalf("expected job2 entry with status completed, got %+v (ok=%v)", e, ok)
	}
	if e, ok := byJobID[job1.ID]; !ok || e.HashKey == "" {
		t.Fatalf("expected job1 entry with a non-empty hash, got %+v (ok=%v)", e, ok)
	}

	store.ClearIdempotentKeys()

	if entries := store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("expected empty ledger after clear, got %d entries", len(entries))
	}

	// A key that was a duplicate before the clear should now be seen as
	// fresh -- proving the clear actually happened, not just that the
	// enumerate function returns empty.
	if _, isDuplicate := store.CheckOrInsert(job1); isDuplicate {
		t.Fatalf("expected job1's idempotent key to be fresh after clear, got duplicate")
	}
}

func TestStoreDrainEventsAreAcknowledgedBySequence(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() { store.Close() })

	job1 := &Job{ID: generateID(), UserID: "u1", IdempotentKey: "k1", URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com", Status: StatusQueued, CreatedAt: time.Now().UnixMilli()}
	job2 := &Job{ID: generateID(), UserID: "u2", IdempotentKey: "k2", URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com", Status: StatusQueued, CreatedAt: time.Now().UnixMilli()}
	webhookJob := &Job{ID: generateID(), UserID: "u3", IdempotentKey: "webhook:" + generateID(), URL: "https://example.com", Method: "POST", Status: StatusQueued, CreatedAt: time.Now().UnixMilli()}

	store.CheckOrInsert(job1)
	store.CheckOrInsert(job2)
	store.CheckOrInsert(webhookJob)
	store.UpdateStatus(job1.ID, StatusCompleted)
	store.UpdateStatus(job2.ID, StatusFailed)
	store.UpdateStatus(webhookJob.ID, StatusCompleted)

	events := store.ListDrainEvents(10)
	if len(events) != 2 {
		t.Fatalf("expected 2 drain events, got %d", len(events))
	}
	if events[0].Sequence == 0 || events[1].Sequence <= events[0].Sequence {
		t.Fatalf("expected ordered non-zero sequences, got %+v", events)
	}
	if events[0].JobID != job1.ID || events[0].Status != StatusCompleted {
		t.Fatalf("expected first event for completed job1, got %+v", events[0])
	}
	if events[1].JobID != job2.ID || events[1].Status != StatusFailed {
		t.Fatalf("expected second event for failed job2, got %+v", events[1])
	}

	store.AcknowledgeDrainEventsThrough(events[0].Sequence)
	remaining := store.ListDrainEvents(10)
	if len(remaining) != 1 || remaining[0].JobID != job2.ID {
		t.Fatalf("expected only job2 after acknowledging first event, got %+v", remaining)
	}

	store.AcknowledgeDrainEventsThrough(events[1].Sequence)
	if remaining := store.ListDrainEvents(10); len(remaining) != 0 {
		t.Fatalf("expected no drain events after acknowledging through second event, got %+v", remaining)
	}
}

// TestStoreHandlesConcurrentInserts is a regression guard for the
// SetMaxOpenConns(1) bug: a single shared connection serialized every
// request through one handle regardless of WAL mode, so N concurrent
// requests took roughly N times as long as one. With a real pool, this
// should complete quickly rather than queueing one-at-a-time.
func TestStoreHandlesConcurrentInserts(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
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
		t.Fatalf("%d concurrent inserts took %s — looks serialized through a single connection again", n, elapsed)
	}

	counts := store.Counts()
	if counts.TotalJobs != n {
		t.Fatalf("expected %d jobs recorded, got %d", n, counts.TotalJobs)
	}
}
