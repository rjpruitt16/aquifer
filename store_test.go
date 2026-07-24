package aquifer

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

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
