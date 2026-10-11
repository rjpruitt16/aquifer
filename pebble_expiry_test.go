package aquifer

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func expiryTestJob(store *PebbleStore, key string) *Job {
	job := NewJob(&JobRequest{UserID: "u", IdempotentKey: key, URL: "http://x", Method: "POST", WebhookURL: "http://w"})
	store.CheckOrInsert(job)
	return job
}

// Cleanup reads the expiry index instead of decoding every job: completed
// jobs past their 30-minute retention go, queued jobs (24h) stay, and a
// stale index key left behind when a status change extended a job's expiry
// doesn't delete the live job.
func TestPebbleCleanupUsesExpiryIndex(t *testing.T) {
	store := NewPebbleStore(filepath.Join(t.TempDir(), "pebble"))
	defer store.Close()

	done1 := expiryTestJob(store, "done-1")
	done2 := expiryTestJob(store, "done-2")
	queued := expiryTestJob(store, "queued")
	store.UpdateStatus(done1.ID, StatusCompleted)
	store.UpdateStatus(done2.ID, StatusCompleted)

	// 31 minutes from now is past completed retention (30m) but not queued
	// retention (24h).
	store.deleteExpired(time.Now().Add(31 * time.Minute).UnixMilli())

	for _, id := range []string{done1.ID, done2.ID} {
		if _, ok := store.getRecord(id); ok {
			t.Fatalf("expired job %s was not deleted", id)
		}
	}
	if _, ok := store.getRecord(queued.ID); !ok {
		t.Fatal("queued job was deleted before its expiry")
	}
	if counts := store.Counts(); counts.QueueDepth != 1 {
		t.Fatalf("expected 1 queued job counted, got %+v", counts)
	}

	// A failed job's expiry (2h) moved past its first, completed-time key
	// (30m): the stale key comes due first and must not delete it.
	moved := expiryTestJob(store, "moved")
	store.UpdateStatus(moved.ID, StatusCompleted)
	store.UpdateStatus(moved.ID, StatusFailed)
	store.deleteExpired(time.Now().Add(31 * time.Minute).UnixMilli())
	if _, ok := store.getRecord(moved.ID); !ok {
		t.Fatal("job deleted by a stale expiry key although its expiry was extended")
	}
	store.deleteExpired(time.Now().Add(121 * time.Minute).UnixMilli())
	if _, ok := store.getRecord(moved.ID); ok {
		t.Fatal("job not deleted once its extended expiry passed")
	}
}

// A store written before the expiry index existed gets one backfill pass.
func TestPebbleExpiryIndexBackfill(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	store := NewPebbleStore(dir)
	var ids []string
	for i := 0; i < 5; i++ {
		job := expiryTestJob(store, fmt.Sprintf("old-%d", i))
		store.UpdateStatus(job.ID, StatusCompleted)
		ids = append(ids, job.ID)
	}
	// Simulate an older store: no index keys, no marker.
	store.db.DeleteRange([]byte("exp:"), []byte("exp;"), pebble.Sync)
	store.db.Delete(expIndexMarker, pebble.Sync)
	store.Close()

	store = NewPebbleStore(dir)
	defer store.Close()
	store.deleteExpired(time.Now().Add(31 * time.Minute).UnixMilli())
	for _, id := range ids {
		if _, ok := store.getRecord(id); ok {
			t.Fatalf("job %s from before the index was not cleaned up after backfill", id)
		}
	}
}

func countExpiryKeys(t *testing.T, store *PebbleStore) int {
	t.Helper()
	iter, err := store.db.NewIter(&pebble.IterOptions{LowerBound: []byte("exp:"), UpperBound: []byte("exp;")})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

// Each job keeps one index key. The soak test found the key written at
// creation (24h) outliving the completed job (30m): one leftover entry per
// job, which grew the database by hundreds of MB an hour.
func TestPebbleStatusChangeReplacesExpiryKey(t *testing.T) {
	store := NewPebbleStore(filepath.Join(t.TempDir(), "pebble"))
	defer store.Close()

	const n = 50
	for i := 0; i < n; i++ {
		job := expiryTestJob(store, fmt.Sprintf("job-%d", i))
		store.UpdateStatus(job.ID, StatusCompleted)
	}
	if got := countExpiryKeys(t, store); got != n {
		t.Fatalf("expected one expiry key per job (%d), got %d", n, got)
	}

	store.deleteExpired(time.Now().Add(31 * time.Minute).UnixMilli())
	if got := countExpiryKeys(t, store); got != 0 {
		t.Fatalf("expected no expiry keys after completed retention passed, got %d", got)
	}
	if counts := store.Counts(); counts.QueueDepth != 0 {
		t.Fatalf("expected empty store, got %+v", counts)
	}
}
