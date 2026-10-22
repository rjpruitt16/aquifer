// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"log"
	"os"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// Store is the storage backend behind everything that persists a job:
// idempotency, status transitions, crash recovery, and admission control's
// db-size check. *SQLite (SQLite/WAL) is the default and only implementation
// until now; *Pebble is an opt-in alternative for benchmarking whether
// a memory-first LSM store changes the throughput ceiling the way it did
// for the Elixir/Mnesia sibling of this project. Selected via
// AQUIFER_STORE_BACKEND ("pebble", the default, or "sqlite"). An existing
// SQLite file at DB_PATH keeps an upgraded deployment on SQLite.
type Store interface {
	Path() string
	Close() error
	CheckOrInsert(job *jobs.Job) (string, bool)
	SetQueueKey(jobID, queueKey string)
	DeleteJob(jobID string)
	// RecoverQueued returns a queue's unfinished jobs (re-enqueued after a
	// queue panic). Jobs are never persisted as in flight: a crash redoes them
	// either way, so that extra synced write per dispatch bought nothing.
	RecoverQueued(queueKey string) []*jobs.Job
	UpdateStatus(jobID string, status jobs.Status)
	RecordRetry(jobID string, attempts int)
	// PutResult/GetResult keep a job's terminal response so a caller who
	// streamed (and so may get no webhook) can still fetch it later.
	PutResult(jobID string, result jobs.Result)
	GetResult(jobID string) (jobs.Result, bool)
	Counts() Counts
	GetJob(jobID string) *jobs.Job
	GetQueuedJobs() []*jobs.Job

	// ListIdempotentKeys and ClearIdempotentKeys back drain mode (see
	// drain.go) -- an opt-in feature, off by default, so calling these on
	// a deployment that never enables it is never reached.
	ListIdempotentKeys() []jobs.LedgerEntry
	ClearIdempotentKeys()
	ListDrainEvents(limit int) []jobs.DrainEvent
	AcknowledgeDrainEventsThrough(sequence int64)
}

// Open constructs whichever backend AQUIFER_STORE_BACKEND names.
// path is the same value that used to go straight to NewSQLite — for
// SQLite it's a file path, for Pebble it's a directory.
func Open(path string) Store {
	switch os.Getenv("AQUIFER_STORE_BACKEND") {
	case "sqlite":
		return NewSQLite(path)
	case "pebble":
		log.Printf("store: using Pebble backend at %s", path)
		return NewPebble(path)
	}
	// Pebble is the default. An existing SQLite file at DB_PATH means an
	// upgrade from the old default: stay on it rather than orphan its queued
	// jobs (Pebble needs a directory there and would start empty).
	if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
		log.Printf("store: found an existing SQLite database at %s, staying on SQLite; set AQUIFER_STORE_BACKEND=pebble with a new DB_PATH to switch", path)
		return NewSQLite(path)
	}
	log.Printf("store: using Pebble backend at %s", path)
	return NewPebble(path)
}
