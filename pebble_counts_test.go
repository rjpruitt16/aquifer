package aquifer

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func scannedCounts(s *PebbleStore) StoreCounts {
	var c StoreCounts
	s.forEachJob(func(rec *pebbleRecord) {
		if rec.Job.Status == StatusQueued || rec.Job.Status == StatusInFlight {
			c.TotalJobs++
		}
		if rec.Job.Status == StatusQueued {
			c.QueueDepth++
		}
	})
	return c
}

func TestPebbleRunningCountsMatchAFullScan(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p")
	s := NewPebbleStore(dir)
	var ids []string
	for i := 0; i < 40; i++ {
		req := sampleJobRequest("u", fmt.Sprintf("k-%d", i))
		job := NewJob(&req)
		s.CheckOrInsert(job)
		s.CheckOrInsert(NewJob(&req)) // duplicate: must not count twice
		ids = append(ids, job.ID)
	}
	for i, id := range ids {
		switch i % 5 {
		case 1:
			s.UpdateStatus(id, StatusCompleted)
		case 2:
			s.UpdateStatus(id, StatusFailed)
		case 3:
			s.UpdateStatus(id, StatusCompleted)
			s.UpdateStatus(id, StatusQueued)
		case 4:
			s.DeleteJob(id)
		}
	}
	if got, want := s.Counts(), scannedCounts(s); got != want {
		t.Fatalf("running counts %+v drifted from a full scan %+v", got, want)
	}
	s.Close()

	reopened := NewPebbleStore(dir)
	defer reopened.Close()
	if got, want := reopened.Counts(), scannedCounts(reopened); got != want {
		t.Fatalf("after reopen, counts %+v should be re-seeded to %+v", got, want)
	}
}

func TestPebbleCountsStayCheapWithManyRetainedJobs(t *testing.T) {
	s := NewPebbleStore(filepath.Join(t.TempDir(), "p"))
	defer s.Close()
	// Seed retained completed jobs directly, without per-write fsyncs: this
	// test is about Counts() cost, not insert speed.
	for i := 0; i < 5000; i++ {
		req := sampleJobRequest("u", fmt.Sprintf("done-%d", i))
		req.Body = string(make([]byte, 512))
		job := NewJob(&req)
		job.Status = StatusCompleted
		data, _ := json.Marshal(pebbleRecord{Job: job, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()})
		s.db.Set(jobKey(job.ID), data, pebble.NoSync)
	}
	start := time.Now()
	for i := 0; i < 1000; i++ {
		s.Counts()
	}
	if per := time.Since(start) / 1000; per > 50*time.Microsecond {
		t.Fatalf("Counts() took %v per call with 5000 retained jobs; it must not scan", per)
	}
}
