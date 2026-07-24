package aquifer

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	_ "modernc.org/sqlite"
)

const (
	ttlQueued    = 24 * time.Hour
	ttlCompleted = 30 * time.Minute
	ttlFailed    = 2 * time.Hour
	inFlightMax  = 5 * time.Minute // stale in_flight threshold
)

type Store struct {
	db   *sql.DB
	path string
}

func NewStore(path string) *Store {
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

	s := &Store{db: db, path: path}
	s.migrate()
	go s.cleanupLoop()
	return s
}

func (s *Store) Path() string {
	return s.path
}

// Close releases the underlying connection pool. In WAL mode, SQLite keeps
// -wal/-shm files alongside the main database file for as long as any
// connection is open; callers that manage a Store's lifetime explicitly
// (tests especially, cleaning up a t.TempDir()) should call this before
// their directory is removed.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate() {
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
			status              TEXT NOT NULL DEFAULT 'queued',
			queue_key           TEXT NOT NULL DEFAULT '',
			created_at          INTEGER NOT NULL,
			expires_at          INTEGER NOT NULL
		)
	`)
	// safe to run on existing tables — ignored if column already exists
	s.db.Exec(`ALTER TABLE jobs ADD COLUMN queue_key TEXT NOT NULL DEFAULT ''`)
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
func (s *Store) CheckOrInsert(job *Job) (string, bool) {
	hashed := hashKey(job.UserID + ":" + job.IdempotentKey)
	headers, _ := json.Marshal(job.Headers)
	expiresAt := time.Now().Add(ttlQueued).UnixMilli()

	res, err := s.db.Exec(`
		INSERT OR IGNORE INTO jobs
			(id, user_id, idempotent_key_hash, url, method, body, headers, webhook_url, status, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
	`, job.ID, job.UserID, hashed, job.URL, job.Method, job.Body, string(headers), job.WebhookURL, job.CreatedAt, expiresAt)

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

func (s *Store) SetQueueKey(jobID, queueKey string) {
	s.db.Exec(`UPDATE jobs SET queue_key = ? WHERE id = ?`, queueKey, jobID)
}

// DeleteJob removes a job row outright. Used when a freshly-inserted,
// non-duplicate job is rejected by admission control — CheckOrInsert already
// wrote the row before duplicate status was known, so a rejected job must be
// deleted here or it would sit as a ghost "queued" row that never dispatches.
func (s *Store) DeleteJob(jobID string) {
	s.db.Exec(`DELETE FROM jobs WHERE id = ?`, jobID)
}

func (s *Store) MarkInFlight(jobID string) {
	s.db.Exec(`UPDATE jobs SET status = 'in_flight' WHERE id = ?`, jobID)
}

func (s *Store) RecoverInFlight(queueKey string) []*Job {
	rows, err := s.db.Query(`
		SELECT id, user_id, url, method, body, headers, webhook_url, status, created_at
		FROM jobs
		WHERE queue_key = ? AND status = 'in_flight' AND expires_at > ?
	`, queueKey, time.Now().UnixMilli())
	if err != nil {
		return nil
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		if j := scanJob(rows); j != nil {
			jobs = append(jobs, j)
		}
	}

	if len(jobs) > 0 {
		s.db.Exec(`
			UPDATE jobs SET status = 'queued' WHERE queue_key = ? AND status = 'in_flight'
		`, queueKey)
	}
	return jobs
}

func (s *Store) UpdateStatus(jobID string, status Status) {
	expiresAt := time.Now().Add(ttlForStatus(status)).UnixMilli()
	s.db.Exec(`UPDATE jobs SET status = ?, expires_at = ? WHERE id = ?`, string(status), expiresAt, jobID)
}

type StoreCounts struct {
	TotalJobs  int64
	QueueDepth int64
}

func (s *Store) Counts() StoreCounts {
	var c StoreCounts
	s.db.QueryRow(`
		SELECT
			COUNT(*) FILTER (WHERE status IN ('queued','in_flight')),
			COUNT(*) FILTER (WHERE status = 'queued')
		FROM jobs WHERE expires_at > ?
	`, time.Now().UnixMilli()).Scan(&c.TotalJobs, &c.QueueDepth)
	return c
}

func (s *Store) GetJob(jobID string) *Job {
	row := s.db.QueryRow(`
		SELECT id, user_id, url, method, body, headers, webhook_url, status, created_at
		FROM jobs WHERE id = ? AND expires_at > ?
	`, jobID, time.Now().UnixMilli())
	return scanJob(row)
}

func (s *Store) GetQueuedJobs() []*Job {
	rows, err := s.db.Query(`
		SELECT id, user_id, url, method, body, headers, webhook_url, status, created_at
		FROM jobs WHERE status = 'queued' AND expires_at > ?
	`, time.Now().UnixMilli())
	if err != nil {
		log.Printf("GetQueuedJobs: %v", err)
		return nil
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		if j := scanJob(rows); j != nil {
			jobs = append(jobs, j)
		}
	}
	return jobs
}

func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		// expire old jobs
		s.db.Exec(`DELETE FROM jobs WHERE expires_at < ?`, now.UnixMilli())
		// reset stale in_flight jobs back to queued so they get re-dispatched
		s.db.Exec(`
			UPDATE jobs SET status = 'queued'
			WHERE status = 'in_flight' AND created_at < ?
		`, now.Add(-inFlightMax).UnixMilli())
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) *Job {
	var j Job
	var headersJSON string
	err := s.Scan(&j.ID, &j.UserID, &j.URL, &j.Method, &j.Body, &headersJSON, &j.WebhookURL, &j.Status, &j.CreatedAt)
	if err != nil {
		return nil
	}
	json.Unmarshal([]byte(headersJSON), &j.Headers)
	return &j
}

func ttlForStatus(s Status) time.Duration {
	switch s {
	case StatusCompleted:
		return ttlCompleted
	case StatusFailed:
		return ttlFailed
	default:
		return ttlQueued
	}
}

func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
