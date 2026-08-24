package aquifer

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStore is a memory-first LSM-tree alternative to the SQLite-backed
// Store, opted into via AQUIFER_STORE_BACKEND=pebble. Pebble is a raw
// key-value engine — no schema, no UNIQUE constraint, no SQL WHERE clause —
// so several things SQLite gave for free are reimplemented explicitly here:
//
//   - Idempotency's check-or-insert atomicity: Pebble has no native
//     insert-if-absent primitive, so a naive Get-then-Set would reproduce
//     the exact lookup-then-write race found and fixed (twice, in two
//     different codebases) elsewhere this session. Guarded here by a
//     lock striped across shardCount buckets, keyed by the idempotent
//     hash — enough to serialize only conflicting keys, not every request.
//   - Secondary indexes: idempotent-key lookup gets a real index
//     (idem:<hash> -> job id), since that is the hot path called on every
//     request. Status/queue-key scans (GetQueuedJobs, RecoverInFlight,
//     Counts, cleanup) iterate the job: prefix and filter in Go — no
//     worse than what the current SQL schema does today, since it has no
//     index on status or queue_key either.
//   - The durability/throughput dial: Pebble's WriteOptions.Sync controls
//     whether a commit forces an fsync (true, matching SQLite's stricter
//     end) or just reaches the OS via write() without waiting for
//     confirmation (false — the same trade Mnesia's batched flush and
//     SQLite's synchronous=NORMAL both make). Defaults to false;
//     AQUIFER_PEBBLE_SYNC_WRITES=true forces the strict mode. Verify
//     crash survival empirically before trusting either setting, the
//     same way that was verified for Mnesia and SQLite this session —
//     don't assume "it's on disk" means "it survives a kill -9."
const shardCount = 256

type pebbleRecord struct {
	Job       *Job
	QueueKey  string
	ExpiresAt int64
}

type PebbleStore struct {
	db       *pebble.DB
	path     string
	syncOpts *pebble.WriteOptions
	locks    [shardCount]sync.Mutex
}

// AQUIFER_PEBBLE_WAL_SYNC_INTERVAL_MS default. Confirmed empirically (a
// real write() + kill -9 + restart, not assumed): unlike SQLite's WAL,
// Pebble's Sync:false does NOT write through to the OS at all — a killed
// process replays "0 keys in 0 batches" on restart, the data never left
// the process. Sync:true is required for any crash-durability guarantee
// at all. WALMinSyncInterval is Pebble's own group-commit mechanism: it
// batches concurrent Sync:true requests into fewer real fsyncs under
// load, but — unlike the hand-timed batching built for the Elixir/Mnesia
// side of this comparison — each caller's own write still blocks until
// it is actually durable, so there's no "acknowledged but silently lost"
// window the way a naive periodic-flush timer would introduce.
const defaultWALSyncIntervalMS = 5

func NewPebbleStore(path string) *PebbleStore {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatalf("pebble: mkdir %s: %v", filepath.Dir(path), err)
	}

	syncIntervalMS := defaultWALSyncIntervalMS
	if v := os.Getenv("AQUIFER_PEBBLE_WAL_SYNC_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			syncIntervalMS = n
		}
	}
	syncInterval := time.Duration(syncIntervalMS) * time.Millisecond

	db, err := pebble.Open(path, &pebble.Options{
		WALMinSyncInterval: func() time.Duration { return syncInterval },
	})
	if err != nil {
		log.Fatalf("pebble: open %s: %v", path, err)
	}

	s := &PebbleStore{
		db: db,
		// Sync is always true — see the comment above this function.
		// Throughput under load comes from WALMinSyncInterval's group
		// commit, not from skipping durability per write.
		path:     path,
		syncOpts: &pebble.WriteOptions{Sync: true},
	}

	go s.cleanupLoop()
	return s
}

func (s *PebbleStore) Path() string {
	return s.path
}

func (s *PebbleStore) Close() error {
	return s.db.Close()
}

func (s *PebbleStore) shardLock(key string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &s.locks[h.Sum32()%shardCount]
}

func jobKey(id string) []byte    { return []byte("job:" + id) }
func idemKey(hash string) []byte { return []byte("idem:" + hash) }

// CheckOrInsert mirrors Store.CheckOrInsert's contract exactly: :ok for a
// fresh job, or the existing job ID if the (user_id, idempotent_key) pair
// was already accepted. The shard lock is what makes this atomic instead
// of a racy Get-then-Set — see the package doc comment above.
func (s *PebbleStore) CheckOrInsert(job *Job) (string, bool) {
	hashed := hashKey(job.UserID + ":" + job.IdempotentKey)
	ik := idemKey(hashed)

	lock := s.shardLock(hashed)
	lock.Lock()
	defer lock.Unlock()

	if val, closer, err := s.db.Get(ik); err == nil {
		existingID := string(val)
		closer.Close()
		return existingID, true
	}

	rec := pebbleRecord{Job: job, ExpiresAt: time.Now().Add(ttlQueued).UnixMilli()}
	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("pebble: marshal job %s: %v", job.ID, err)
		return "", false
	}

	batch := s.db.NewBatch()
	if err := batch.Set(jobKey(job.ID), data, nil); err != nil {
		log.Printf("pebble: batch set job %s: %v", job.ID, err)
		return "", false
	}
	if err := batch.Set(ik, []byte(job.ID), nil); err != nil {
		log.Printf("pebble: batch set idem index for job %s: %v", job.ID, err)
		return "", false
	}
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: commit job %s: %v", job.ID, err)
		return "", false
	}

	return "", false
}

func (s *PebbleStore) getRecord(jobID string) (*pebbleRecord, bool) {
	val, closer, err := s.db.Get(jobKey(jobID))
	if err != nil {
		return nil, false
	}
	defer closer.Close()

	var rec pebbleRecord
	if err := json.Unmarshal(val, &rec); err != nil {
		return nil, false
	}
	return &rec, true
}

func (s *PebbleStore) putRecord(jobID string, rec *pebbleRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("pebble: marshal job %s: %v", jobID, err)
		return
	}
	if err := s.db.Set(jobKey(jobID), data, s.syncOpts); err != nil {
		log.Printf("pebble: set job %s: %v", jobID, err)
	}
}

func (s *PebbleStore) SetQueueKey(jobID, queueKey string) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return
	}
	rec.QueueKey = queueKey
	s.putRecord(jobID, rec)
}

// DeleteJob removes both the job row and its idempotency index entry —
// dropping only the job: key would leave a dangling idem: entry pointing
// at a job that no longer exists, the same ghost-row risk DeleteJob exists
// to prevent on the SQLite side.
func (s *PebbleStore) DeleteJob(jobID string) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		s.db.Delete(jobKey(jobID), s.syncOpts)
		return
	}

	hashed := hashKey(rec.Job.UserID + ":" + rec.Job.IdempotentKey)

	batch := s.db.NewBatch()
	batch.Delete(jobKey(jobID), nil)
	batch.Delete(idemKey(hashed), nil)
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: delete job %s: %v", jobID, err)
	}
}

func (s *PebbleStore) MarkInFlight(jobID string) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return
	}
	rec.Job.Status = StatusInFlight
	s.putRecord(jobID, rec)
}

func (s *PebbleStore) UpdateStatus(jobID string, status Status) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return
	}
	rec.Job.Status = status
	rec.ExpiresAt = time.Now().Add(ttlForStatus(status)).UnixMilli()
	s.putRecord(jobID, rec)
}

// forEachJob iterates every job: record, skipping expired ones. Used only
// by the less-hot-path operations (recovery, counts, cleanup) — no worse
// than the current SQL schema, which has no index on status or queue_key
// either, so these are already full-ish scans there too.
func (s *PebbleStore) forEachJob(fn func(rec *pebbleRecord)) {
	now := time.Now().UnixMilli()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("job:"),
		UpperBound: []byte("job;"), // ';' is ':' + 1 in ASCII, bounds the prefix scan
	})
	if err != nil {
		log.Printf("pebble: iterate jobs: %v", err)
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec pebbleRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		if rec.ExpiresAt <= now {
			continue
		}
		fn(&rec)
	}
}

func (s *PebbleStore) RecoverInFlight(queueKey string) []*Job {
	var jobs []*Job
	var ids []string

	s.forEachJob(func(rec *pebbleRecord) {
		if rec.QueueKey == queueKey && rec.Job.Status == StatusInFlight {
			jobs = append(jobs, rec.Job)
			ids = append(ids, rec.Job.ID)
		}
	})

	for _, id := range ids {
		s.UpdateStatus(id, StatusQueued)
	}

	return jobs
}

func (s *PebbleStore) Counts() StoreCounts {
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

func (s *PebbleStore) GetJob(jobID string) *Job {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return nil
	}
	if rec.ExpiresAt <= time.Now().UnixMilli() {
		return nil
	}
	return rec.Job
}

func (s *PebbleStore) GetQueuedJobs() []*Job {
	var jobs []*Job
	s.forEachJob(func(rec *pebbleRecord) {
		if rec.Job.Status == StatusQueued {
			jobs = append(jobs, rec.Job)
		}
	})
	return jobs
}

// ListIdempotentKeys backs drain mode's ledger export. Unlike SQLite,
// pebbleRecord.Job retains the plaintext IdempotentKey (for unrelated
// reasons -- see the package doc comment), but this must never surface it:
// the hash is recomputed the same way CheckOrInsert derives it, and only
// the hash/job_id/status ever go into the returned LedgerEntry.
func (s *PebbleStore) ListIdempotentKeys() []LedgerEntry {
	var entries []LedgerEntry
	s.forEachJob(func(rec *pebbleRecord) {
		entries = append(entries, LedgerEntry{
			HashKey: hashKey(rec.Job.UserID + ":" + rec.Job.IdempotentKey),
			JobID:   rec.Job.ID,
			Status:  rec.Job.Status,
		})
	})
	return entries
}

// ClearIdempotentKeys wipes both the job: and idem: prefixes -- only ever
// called by drain mode's watchdog after a successful ledger-flush webhook
// delivery, never on a normal (non-drain-mode) deployment. Takes every
// shard lock before wiping so a concurrent CheckOrInsert (which only holds
// one shard's lock) can't race a mid-wipe read/write and reintroduce a row.
func (s *PebbleStore) ClearIdempotentKeys() {
	for i := range s.locks {
		s.locks[i].Lock()
	}
	defer func() {
		for i := range s.locks {
			s.locks[i].Unlock()
		}
	}()

	deletePrefix := func(lower, upper []byte) {
		iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			log.Printf("pebble: iterate for clear: %v", err)
			return
		}
		defer iter.Close()

		var keys [][]byte
		for iter.First(); iter.Valid(); iter.Next() {
			k := make([]byte, len(iter.Key()))
			copy(k, iter.Key())
			keys = append(keys, k)
		}
		for _, k := range keys {
			if err := s.db.Delete(k, s.syncOpts); err != nil {
				log.Printf("pebble: delete %s: %v", k, err)
			}
		}
	}

	deletePrefix([]byte("job:"), []byte("job;"))
	deletePrefix([]byte("idem:"), []byte("idem;"))
}

func (s *PebbleStore) cleanupLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixMilli()
		staleBefore := time.Now().Add(-inFlightMax).UnixMilli()

		var expiredIDs []string
		var staleInFlight []string

		s.forEachJobIncludingExpired(func(rec *pebbleRecord) {
			if rec.ExpiresAt < now {
				expiredIDs = append(expiredIDs, rec.Job.ID)
				return
			}
			if rec.Job.Status == StatusInFlight && rec.Job.CreatedAt < staleBefore {
				staleInFlight = append(staleInFlight, rec.Job.ID)
			}
		})

		for _, id := range expiredIDs {
			s.DeleteJob(id)
		}
		for _, id := range staleInFlight {
			s.UpdateStatus(id, StatusQueued)
		}
	}
}

// forEachJobIncludingExpired is forEachJob without the expiry filter — the
// cleanup loop is the one caller that needs to see expired rows, since its
// whole job is deleting them.
func (s *PebbleStore) forEachJobIncludingExpired(fn func(rec *pebbleRecord)) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("job:"),
		UpperBound: []byte("job;"),
	})
	if err != nil {
		log.Printf("pebble: iterate jobs: %v", err)
		return
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec pebbleRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		fn(&rec)
	}
}
