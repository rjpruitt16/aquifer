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
	db, err := sql.Open("sqlite", path)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")

	s := &Store{db: db, path: path}
	s.migrate()
	go s.cleanupLoop()
	return s
}

func (s *Store) Path() string {
	return s.path
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

func (s *Store) CheckOrInsert(job *Job) (string, bool) {
	hashed := hashKey(job.UserID + ":" + job.IdempotentKey)
	headers, _ := json.Marshal(job.Headers)
	expiresAt := time.Now().Add(ttlQueued).UnixMilli()

	s.db.Exec(`
		INSERT OR IGNORE INTO jobs
			(id, user_id, idempotent_key_hash, url, method, body, headers, webhook_url, status, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)
	`, job.ID, job.UserID, hashed, job.URL, job.Method, job.Body, string(headers), job.WebhookURL, job.CreatedAt, expiresAt)

	var existingID string
	s.db.QueryRow(`SELECT id FROM jobs WHERE idempotent_key_hash = ?`, hashed).Scan(&existingID)

	if existingID != job.ID {
		return existingID, true
	}
	return "", false
}

func (s *Store) SetQueueKey(jobID, queueKey string) {
	s.db.Exec(`UPDATE jobs SET queue_key = ? WHERE id = ?`, queueKey, jobID)
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
