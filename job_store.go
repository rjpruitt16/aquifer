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
// AQUIFER_STORE_BACKEND ("pebble", the default, or "sqlite"). An existing
// SQLite file at DB_PATH keeps an upgraded deployment on SQLite.
type JobStore interface {
	Path() string
	Close() error
	CheckOrInsert(job *Job) (string, bool)
	SetQueueKey(jobID, queueKey string)
	DeleteJob(jobID string)
	// RecoverQueued returns a queue's unfinished jobs (re-enqueued after a
	// queue panic). Jobs are never persisted as in flight: a crash redoes them
	// either way, so that extra synced write per dispatch bought nothing.
	RecoverQueued(queueKey string) []*Job
	UpdateStatus(jobID string, status Status)
	RecordRetry(jobID string, attempts int)
	// PutResult/GetResult keep a job's terminal response so a caller who
	// streamed (and so may get no webhook) can still fetch it later.
	PutResult(jobID string, result JobResult)
	GetResult(jobID string) (JobResult, bool)
	Counts() StoreCounts
	GetJob(jobID string) *Job
	GetQueuedJobs() []*Job

	// ListIdempotentKeys and ClearIdempotentKeys back drain mode (see
	// drain.go) -- an opt-in feature, off by default, so calling these on
	// a deployment that never enables it is never reached.
	ListIdempotentKeys() []LedgerEntry
	ClearIdempotentKeys()
	ListDrainEvents(limit int) []DrainEvent
	AcknowledgeDrainEventsThrough(sequence int64)
}

// NewJobStore constructs whichever backend AQUIFER_STORE_BACKEND names.
// path is the same value that used to go straight to NewStore — for
// SQLite it's a file path, for Pebble it's a directory.
func NewJobStore(path string) JobStore {
	switch os.Getenv("AQUIFER_STORE_BACKEND") {
	case "sqlite":
		return NewStore(path)
	case "pebble":
		log.Printf("store: using Pebble backend at %s", path)
		return NewPebbleStore(path)
	}
	// Pebble is the default. An existing SQLite file at DB_PATH means an
	// upgrade from the old default: stay on it rather than orphan its queued
	// jobs (Pebble needs a directory there and would start empty).
	if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
		log.Printf("store: found an existing SQLite database at %s, staying on SQLite; set AQUIFER_STORE_BACKEND=pebble with a new DB_PATH to switch", path)
		return NewStore(path)
	}
	log.Printf("store: using Pebble backend at %s", path)
	return NewPebbleStore(path)
}
