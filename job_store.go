package aquifer

import (
	"log"
	"os"
)

// JobStore is the storage backend behind everything that persists a job:
// idempotency, status transitions, crash recovery, and admission control's
// db-size check. *Store (SQLite/WAL) is the default and only implementation
// until now; *PebbleStore is an opt-in alternative for benchmarking whether
// a memory-first LSM store changes the throughput ceiling the way it did
// for the Elixir/Mnesia sibling of this project. Selected via
// AQUIFER_STORE_BACKEND ("sqlite", the default, or "pebble") — existing
// deployments that don't set it see no change at all.
type JobStore interface {
	Path() string
	Close() error
	CheckOrInsert(job *Job) (string, bool)
	SetQueueKey(jobID, queueKey string)
	DeleteJob(jobID string)
	MarkInFlight(jobID string)
	RecoverInFlight(queueKey string) []*Job
	UpdateStatus(jobID string, status Status)
	Counts() StoreCounts
	GetJob(jobID string) *Job
	GetQueuedJobs() []*Job

	// ListIdempotentKeys and ClearIdempotentKeys back drain mode (see
	// drain.go) -- an opt-in feature, off by default, so calling these on
	// a deployment that never enables it is never reached.
	ListIdempotentKeys() []LedgerEntry
	ClearIdempotentKeys()
}

// NewJobStore constructs whichever backend AQUIFER_STORE_BACKEND names.
// path is the same value that used to go straight to NewStore — for
// SQLite it's a file path, for Pebble it's a directory.
func NewJobStore(path string) JobStore {
	switch os.Getenv("AQUIFER_STORE_BACKEND") {
	case "pebble":
		log.Printf("store: using Pebble backend at %s", path)
		return NewPebbleStore(path)
	default:
		return NewStore(path)
	}
}
