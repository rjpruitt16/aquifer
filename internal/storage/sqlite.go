// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"database/sql"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
	_ "modernc.org/sqlite"
)

const (
	ttlQueued    = jobs.QueuedTTL
	ttlCompleted = 30 * time.Minute
	ttlFailed    = 2 * time.Hour
)

type SQLite struct {
	db        *sql.DB
	path      string
	closeOnce sync.Once
	closeErr  error
	stop      chan struct{}
	done      chan struct{}

	// skipDrainEvents is set when drain mode is off: nothing ever reads or
	// acknowledges the events then, so recording them is a wasted write
	// per job and a table that only grows. See SetDrainEventsEnabled.
	skipDrainEvents atomic.Bool
}

// SetDrainEventsEnabled turns drain-event recording on or off. The Registry
// calls it with whether drain mode is enabled; a bare store records them.
func (s *SQLite) SetDrainEventsEnabled(enabled bool) { s.skipDrainEvents.Store(!enabled) }

func NewSQLite(path string) *SQLite {
	// Pragmas belong in the DSN, not a one-off db.Exec after Open. SQLite
	// pragmas are per-connection, and Go's database/sql pool opens new
	// physical connections on demand — an Exec() only configures whichever
	// single connection happens to service that call. With MaxOpenConns(1)
	// this went unnoticed for a long time, since there was only ever one
	// connection to configure. The moment the pool can open more than one,
	// every connection opened after the first would silently run with
	// SQLite's defaults (rollback journal, busy_timeout=0 — i.e. fail
	// instantly on any lock contention instead of retrying), which is
	// exactly what caused "database is locked" errors under concurrent
	// writes even with the Exec-based pragmas already in place.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	// WAL mode lets readers and writers proceed concurrently (only
	// writer-vs-writer serializes) — but that only matters if more than one
	// connection is ever open. A single shared connection would force every
	// request, including plain reads, to queue behind one handle regardless
	// of WAL.
	db.SetMaxOpenConns(25)

	s := &SQLite{db: db, path: path, stop: make(chan struct{}), done: make(chan struct{})}
	s.migrate()
	go s.cleanupLoop()
	return s
}

func (s *SQLite) Path() string {
	return s.path
}

// Close releases the underlying connection pool. In WAL mode, SQLite keeps
// -wal/-shm files alongside the main database file for as long as any
// connection is open; callers that manage a SQLite's lifetime explicitly
// (tests especially, cleaning up a t.TempDir()) should call this before
// their directory is removed.
func (s *SQLite) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func (s *SQLite) migrate() {
	s.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id                  TEXT PRIMARY KEY,
			user_id             TEXT NOT NULL,
			idempotent_key_hash TEXT NOT NULL UNIQUE,
			url                 TEXT NOT NULL,
			method              TEXT NOT NULL,
			body                TEXT NOT NULL DEFAULT '',
			headers             TEXT NOT NULL DEFAULT '{}',
			webhook_url         TEXT NOT NULL DEFAULT '',
			webhook_delivery    INTEGER NOT NULL DEFAULT 0,
			status              TEXT NOT NULL DEFAULT 'queued',
			queue_key           TEXT NOT NULL DEFAULT '',
			created_at          INTEGER NOT NULL,
			execute_before      INTEGER NOT NULL DEFAULT 0,
			expires_at          INTEGER NOT NULL
		)
	`)
	// safe to run on existing tables — ignored if column already exists
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN queue_key TEXT NOT NULL DEFAULT ''`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN execute_before INTEGER NOT NULL DEFAULT 0`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN max_retries INTEGER NOT NULL DEFAULT 4`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`)
	// NULL means the caller didn't set webhook_max_retries.
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN webhook_max_retries INTEGER`)
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN result TEXT NOT NULL DEFAULT ''`)
	if _, err := s.db.Exec(`ALTER TABLE jobs ADD COLUMN webhook_delivery INTEGER NOT NULL DEFAULT 0`); err == nil {
		// Before this column existed, only internal webhook-delivery jobs could
		// have an empty webhook_url. Preserve their identity across migration.
		s.db.Exec(`UPDATE jobs SET webhook_delivery = 1 WHERE webhook_url = ''`)
	}
	// Jobs are no longer marked in_flight; rows left that way by older
	// versions are just unfinished work.
	s.db.Exec(`UPDATE jobs SET status = 'queued' WHERE status = 'in_flight'`)
	s.db.Exec(`
		CREATE TABLE IF NOT EXISTS drain_events (
			sequence             INTEGER PRIMARY KEY AUTOINCREMENT,
			idempotent_key_hash  TEXT NOT NULL,
			job_id               TEXT NOT NULL UNIQUE,
			status               TEXT NOT NULL,
			recorded_at          INTEGER NOT NULL
		)
	`)
}

// CheckOrInsert inserts job unless its (user_id, idempotent_key) pair
// already exists, in which case it reports the existing job's ID as a
// duplicate. The duplicate/fresh decision is read directly off the INSERT's
// own RowsAffected, not a follow-up SELECT — a SELECT-after-INSERT here
// would race under real concurrency (multiple open connections): if the
// INSERT is still in flight relative to another goroutine's read, or a
// transient busy-retry delays it, the SELECT can find nothing and this
// would misreport a brand-new job as an empty-ID "duplicate," silently
// losing it. RowsAffected==1 is authoritative and atomic: it's exactly the
// row this call just wrote, no read-your-own-write race possible.
func (s *SQLite) CheckOrInsert(job *jobs.Job) (string, bool) {
	hashed := job.IdempotencyHash()
	headers, _ := json.Marshal(job.Headers)
	expiresAt := time.Now().Add(ttlQueued).UnixMilli()

	res, err := s.db.Exec(`
		INSERT OR IGNORE INTO jobs
			(id, user_id, idempotent_key_hash, url, method, body, headers, webhook_url, webhook_delivery, status, created_at, execute_before, expires_at, max_retries, attempts, webhook_max_retries)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)
	`, job.ID, job.UserID, hashed, job.URL, job.Method, job.Body, string(headers), job.WebhookURL, job.IsWebhookDelivery(), job.CreatedAt, job.ExecuteBefore, expiresAt, job.MaxRetries, job.Attempts, job.WebhookMaxRetries)

	if err == nil {
		if n, _ := res.RowsAffected(); n == 1 {
			return "", false
		}
	}

	// Either the UNIQUE constraint ignored this insert (a genuine
	// duplicate), or the Exec itself failed — either way the source of
	// truth for "what job actually owns this idempotent key" is now this
	// SELECT, which reads whatever a previous successful insert wrote.
	var existingID string
	if scanErr := s.db.QueryRow(`SELECT id FROM jobs WHERE idempotent_key_hash = ?`, hashed).Scan(&existingID); scanErr != nil {
		log.Printf("store: CheckOrInsert for job %s could not confirm insert or find an existing row (insert err: %v, select err: %v)", job.ID, err, scanErr)
		return "", false
	}
	return existingID, true
}

func (s *SQLite) SetQueueKey(jobID, queueKey string) {
	s.db.Exec(`UPDATE jobs SET queue_key = ? WHERE id = ?`, queueKey, jobID)
}

// DeleteJob removes a job row outright. Used when a freshly-inserted,
// non-duplicate job is rejected by admission control — CheckOrInsert already
// wrote the row before duplicate status was known, so a rejected job must be
// deleted here or it would sit as a ghost "queued" row that never dispatches.
func (s *SQLite) DeleteJob(jobID string) {
	s.db.Exec(`DELETE FROM jobs WHERE id = ?`, jobID)
}

// RecordRetry persists a failed attempt and returns the job to queued, so a
// restart while it waits out its backoff recovers it with its attempt count.
func (s *SQLite) RecordRetry(jobID string, attempts int) {
	s.db.Exec(`UPDATE jobs SET attempts = ?, status = 'queued' WHERE id = ?`, attempts, jobID)
}

func (s *SQLite) PutResult(jobID string, result jobs.Result) {
	data, err := json.Marshal(result)
	if err != nil {
		return
	}
	s.db.Exec(`UPDATE jobs SET result = ? WHERE id = ?`, string(data), jobID)
}

func (s *SQLite) GetResult(jobID string) (jobs.Result, bool) {
	var raw string
	if err := s.db.QueryRow(`SELECT result FROM jobs WHERE id = ? AND expires_at > ?`, jobID, time.Now().UnixMilli()).Scan(&raw); err != nil || raw == "" {
		return jobs.Result{}, false
	}
	var result jobs.Result
	if json.Unmarshal([]byte(raw), &result) != nil {
		return jobs.Result{}, false
	}
	return result, true
}

func (s *SQLite) RecoverQueued(queueKey string) []*jobs.Job {
	rows, err := s.db.Query(`
		SELECT id, user_id, url, method, body, headers, webhook_url, webhook_delivery, status, created_at, execute_before, idempotent_key_hash, max_retries, attempts, webhook_max_retries
		FROM jobs
		WHERE queue_key = ? AND status IN ('queued', 'in_flight') AND expires_at > ?
	`, queueKey, time.Now().UnixMilli())
	if err != nil {
		return nil
	}
	defer rows.Close()

	var found []*jobs.Job
	for rows.Next() {
		if j := scanJob(rows); j != nil {
			j.Status = jobs.StatusQueued
			found = append(found, j)
		}
	}
	return found
}

func (s *SQLite) UpdateStatus(jobID string, status jobs.Status) {
	expiresAt := time.Now().Add(ttlForStatus(status)).UnixMilli()
	s.db.Exec(`UPDATE jobs SET status = ?, expires_at = ? WHERE id = ?`, string(status), expiresAt, jobID)
	if (status == jobs.StatusCompleted || status == jobs.StatusFailed) && !s.skipDrainEvents.Load() {
		s.recordDrainEvent(jobID, status)
	}
}

func (s *SQLite) recordDrainEvent(jobID string, status jobs.Status) {
	if _, err := s.db.Exec(`
		INSERT OR IGNORE INTO drain_events
			(idempotent_key_hash, job_id, status, recorded_at)
		SELECT idempotent_key_hash, id, ?, ?
		FROM jobs
		WHERE id = ? AND webhook_url != ''
	`, string(status), time.Now().UnixMilli(), jobID); err != nil {
		log.Printf("store: record drain event for job %s: %v", jobID, err)
	}
}

type Counts struct {
	TotalJobs  int64
	QueueDepth int64
}

func (s *SQLite) Counts() Counts {
	var c Counts
	s.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status IN ('queued','in_flight')),
			COUNT(*) FILTER (WHERE status = 'queued')
		FROM jobs WHERE expires_at > ?
	`, time.Now().UnixMilli()).Scan(&c.TotalJobs, &c.QueueDepth)
	return c
}

func (s *SQLite) GetJob(jobID string) *jobs.Job {
	row := s.db.QueryRow(`
		SELECT id, user_id, url, method, body, headers, webhook_url, webhook_delivery, status, created_at, execute_before, idempotent_key_hash, max_retries, attempts, webhook_max_retries
		FROM jobs WHERE id = ? AND expires_at > ?
	`, jobID, time.Now().UnixMilli())
	return scanJob(row)
}

func (s *SQLite) GetQueuedJobs() []*jobs.Job {
	rows, err := s.db.Query(`
		SELECT id, user_id, url, method, body, headers, webhook_url, webhook_delivery, status, created_at, execute_before, idempotent_key_hash, max_retries, attempts, webhook_max_retries
		FROM jobs WHERE status IN ('queued', 'in_flight') AND expires_at > ?
	`, time.Now().UnixMilli())
	if err != nil {
		log.Printf("GetQueuedJobs: %v", err)
		return nil
	}
	defer rows.Close()

	var found []*jobs.Job
	for rows.Next() {
		if j := scanJob(rows); j != nil {
			found = append(found, j)
		}
	}
	return found
}

// ListIdempotentKeys backs drain mode's ledger export -- hash-only, matches
// what this table has always stored (the plaintext idempotent key was never
// persisted here, only its hash, see jobs.HashKey and CheckOrInsert). Excludes
// webhook-delivery jobs (webhook_delivery = 1) --
// those are internal delivery bookkeeping, not real user-submitted work, and
// have no business appearing in a ledger meant for tenant-handoff dedup.
func (s *SQLite) ListIdempotentKeys() []jobs.LedgerEntry {
	rows, err := s.db.Query(`SELECT idempotent_key_hash, id, status FROM jobs WHERE expires_at > ? AND webhook_delivery = 0`, time.Now().UnixMilli())
	if err != nil {
		log.Printf("ListIdempotentKeys: %v", err)
		return nil
	}
	defer rows.Close()

	var entries []jobs.LedgerEntry
	for rows.Next() {
		var e jobs.LedgerEntry
		if err := rows.Scan(&e.HashKey, &e.JobID, &e.Status); err != nil {
			log.Printf("ListIdempotentKeys scan: %v", err)
			continue
		}
		entries = append(entries, e)
	}
	return entries
}

// ClearIdempotentKeys wipes the whole table -- only ever called by drain
// mode's watchdog after a successful ledger-flush webhook delivery, never
// on a normal (non-drain-mode) deployment.
func (s *SQLite) ClearIdempotentKeys() {
	if _, err := s.db.Exec(`DELETE FROM drain_events`); err != nil {
		log.Printf("ClearIdempotentKeys drain_events: %v", err)
	}
	if _, err := s.db.Exec(`DELETE FROM jobs`); err != nil {
		log.Printf("ClearIdempotentKeys: %v", err)
	}
}

func (s *SQLite) ListDrainEvents(limit int) []jobs.DrainEvent {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.Query(`
		SELECT sequence, idempotent_key_hash, job_id, status, recorded_at
		FROM drain_events
		ORDER BY sequence ASC
		LIMIT ?
	`, limit)
	if err != nil {
		log.Printf("ListDrainEvents: %v", err)
		return nil
	}
	defer rows.Close()

	var events []jobs.DrainEvent
	for rows.Next() {
		var e jobs.DrainEvent
		if err := rows.Scan(&e.Sequence, &e.HashKey, &e.JobID, &e.Status, &e.RecordedAt); err != nil {
			log.Printf("ListDrainEvents scan: %v", err)
			continue
		}
		events = append(events, e)
	}
	return events
}

func (s *SQLite) AcknowledgeDrainEventsThrough(sequence int64) {
	if sequence <= 0 {
		return
	}
	if _, err := s.db.Exec(`DELETE FROM drain_events WHERE sequence <= ?`, sequence); err != nil {
		log.Printf("AcknowledgeDrainEventsThrough: %v", err)
	}
}

func (s *SQLite) cleanupLoop() {
	defer close(s.done)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			// expire old jobs
			s.db.Exec(`DELETE FROM jobs WHERE expires_at < ?`, now.UnixMilli())
		case <-s.stop:
			return
		}
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) *jobs.Job {
	var j jobs.Job
	var headersJSON string
	var webhookMaxRetries sql.NullInt64
	var webhookDelivery bool
	err := s.Scan(&j.ID, &j.UserID, &j.URL, &j.Method, &j.Body, &headersJSON, &j.WebhookURL, &webhookDelivery, &j.Status, &j.CreatedAt, &j.ExecuteBefore, &j.DedupHash, &j.MaxRetries, &j.Attempts, &webhookMaxRetries)
	if err != nil {
		return nil
	}
	if webhookMaxRetries.Valid {
		n := int(webhookMaxRetries.Int64)
		j.WebhookMaxRetries = &n
	}
	j.Kind = jobs.KindUser
	if webhookDelivery {
		j.Kind = jobs.KindWebhookDelivery
	}
	json.Unmarshal([]byte(headersJSON), &j.Headers)
	return &j
}

func ttlForStatus(s jobs.Status) time.Duration {
	switch s {
	case jobs.StatusCompleted:
		return ttlCompleted
	case jobs.StatusFailed:
		return ttlFailed
	default:
		return ttlQueued
	}
}
