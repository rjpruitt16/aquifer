package aquifer

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// Older versions persisted jobs as in_flight while dispatching. After an
// upgrade those rows must come back as ordinary queued work.
func TestLegacyInFlightJobsAreRecoveredAsQueued(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "a.db")
		s := NewStore(path)
		req := sampleJobRequest("u", "legacy")
		job := NewJob(&req)
		s.CheckOrInsert(job)
		s.db.Exec(`UPDATE jobs SET status = 'in_flight' WHERE id = ?`, job.ID)
		s.Close()

		reopened := NewStore(path)
		defer reopened.Close()
		queued := reopened.GetQueuedJobs()
		if len(queued) != 1 || queued[0].ID != job.ID || queued[0].Status != StatusQueued {
			t.Fatalf("legacy in_flight row should recover as queued, got %+v", queued)
		}
	})

	t.Run("pebble", func(t *testing.T) {
		s := NewPebbleStore(filepath.Join(t.TempDir(), "p"))
		defer s.Close()
		req := sampleJobRequest("u", "legacy")
		job := NewJob(&req)
		job.Status = StatusInFlight
		data, _ := json.Marshal(pebbleRecord{Job: job, QueueKey: "q", ExpiresAt: time.Now().Add(time.Hour).UnixMilli()})
		s.db.Set(jobKey(job.ID), data, pebble.Sync)

		if queued := s.GetQueuedJobs(); len(queued) != 1 || queued[0].Status != StatusQueued {
			t.Fatalf("legacy in_flight record should recover as queued, got %+v", queued)
		}
		if recovered := s.RecoverQueued("q"); len(recovered) != 1 {
			t.Fatalf("panic recovery should include the legacy record, got %+v", recovered)
		}
	})
}
