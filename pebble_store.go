package aquifer

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
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
//     request. Status/queue-key scans (GetQueuedJobs, RecoverQueued,
//     Counts, cleanup) iterate the job: prefix and filter in Go — no
//     worse than what the current SQL schema does today, since it has no
//     index on status or queue_key either.
//   - The durability/throughput dial: by default writes are acknowledged
//     before they are durable and the WAL is synced every 100ms
//     (AQUIFER_PEBBLE_FLUSH_INTERVAL_MS), the trade EZThrottle Local's
//     Mnesia flush makes: a crash can lose up to that much accepted work,
//     for several times the throughput. Setting it to 0 makes every write
//     wait for its own fsync (group-committed, see defaultWALSyncIntervalMS),
//     so an acknowledged job is never lost.
const shardCount = 256

type pebbleRecord struct {
	Job       *Job
	QueueKey  string
	ExpiresAt int64
}

type PebbleStore struct {
	db        *pebble.DB
	path      string
	syncOpts  *pebble.WriteOptions
	locks     [shardCount]sync.Mutex
	drainMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
	stop      chan struct{}
	done      chan struct{}

	// flushInterval > 0 means writes don't wait for their own sync; a
	// background loop syncs the WAL this often (AQUIFER_PEBBLE_FLUSH_INTERVAL_MS).
	flushInterval time.Duration
	flushDone     chan struct{}

	// skipDrainEvents: see Store.skipDrainEvents. Recording an event costs
	// two reads and a write under drainMu, a global lock, so with drain
	// mode off it also serialized every job completion.
	skipDrainEvents atomic.Bool

	// queued and inFlight are running counts kept in step with every status
	// change, so Counts() (called on every dispatch for the load headers) is
	// O(1). Counting by scanning made each dispatch scan and decode every
	// retained job, which collapsed throughput once a few thousand
	// completed jobs had accumulated.
	queued   atomic.Int64
	inFlight atomic.Int64
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

const defaultPebbleCacheMB = 64

// defaultFlushIntervalMS is AQUIFER_PEBBLE_FLUSH_INTERVAL_MS's default.
// 0 means sync every write.
const defaultFlushIntervalMS = 100

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

	// Pebble's default block cache is 8MB, too small to hold the job
	// records each status update reads back, so under load those reads went
	// to sstables and snappy decoding (about a fifth of CPU at 700 jobs/s).
	cacheMB := int64(defaultPebbleCacheMB)
	if v := os.Getenv("AQUIFER_PEBBLE_CACHE_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cacheMB = n
		}
	}
	cache := pebble.NewCache(cacheMB << 20)
	defer cache.Unref()

	db, err := pebble.Open(path, &pebble.Options{
		Cache:              cache,
		WALMinSyncInterval: func() time.Duration { return syncInterval },
	})
	if err != nil {
		log.Fatalf("pebble: open %s: %v", path, err)
	}

	s := &PebbleStore{
		db: db,
		// By default each write waits for its sync — see the comment above
		// this function. Throughput under load comes from
		// WALMinSyncInterval's group commit, not from skipping durability.
		path:      path,
		syncOpts:  &pebble.WriteOptions{Sync: true},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		flushDone: make(chan struct{}),
	}

	flushMS := defaultFlushIntervalMS
	if v := os.Getenv("AQUIFER_PEBBLE_FLUSH_INTERVAL_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			flushMS = n
		}
	}
	if flushMS > 0 {
		s.flushInterval = time.Duration(flushMS) * time.Millisecond
		s.syncOpts = pebble.NoSync
	}

	s.backfillExpiryIndex()
	s.seedCounts()
	go s.cleanupLoop()
	if s.flushInterval > 0 {
		go s.flushLoop()
	} else {
		close(s.flushDone)
	}
	return s
}

// flushLoop syncs the WAL every flushInterval. Pebble's WAL is one ordered
// log, so a synced empty record makes every earlier NoSync write durable.
func (s *PebbleStore) flushLoop() {
	defer close(s.flushDone)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.syncWAL()
		case <-s.stop:
			s.syncWAL()
			return
		}
	}
}

func (s *PebbleStore) syncWAL() {
	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		log.Printf("[pebble] WAL sync failed: %v", err)
	}
}

func (s *PebbleStore) seedCounts() {
	s.queued.Store(0)
	s.inFlight.Store(0)
	s.forEachJob(func(rec *pebbleRecord) { s.countStatus(rec.Job.Status, 1) })
}

func (s *PebbleStore) countStatus(status Status, delta int64) {
	switch status {
	case StatusQueued:
		s.queued.Add(delta)
	case StatusInFlight:
		s.inFlight.Add(delta)
	}
}

func (s *PebbleStore) moveStatus(from, to Status) {
	if from == to {
		return
	}
	s.countStatus(from, -1)
	s.countStatus(to, 1)
}

func (s *PebbleStore) Path() string {
	return s.path
}

func (s *PebbleStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		<-s.flushDone
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

func (s *PebbleStore) shardLock(key string) *sync.Mutex {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &s.locks[h.Sum32()%shardCount]
}

func jobKey(id string) []byte    { return []byte("job:" + id) }
func idemKey(hash string) []byte { return []byte("idem:" + hash) }

// expKey indexes a job by when it expires, so cleanup reads only the jobs
// that are due instead of decoding every retained one. Fixed-width hex keeps
// the keys in time order. A status change that extends a job's expiry adds
// a new key and leaves the old one; cleanup drops stale keys when it finds
// the record still alive (or already gone).
func expKey(expiresAt int64, id string) []byte {
	return []byte(fmt.Sprintf("exp:%016x:%s", expiresAt, id))
}

func expKeyBound(expiresAt int64) []byte { return []byte(fmt.Sprintf("exp:%016x", expiresAt)) }

// expIndexMarker records that every job has an expKey. Stores written by
// older versions are backfilled once on open.
var expIndexMarker = []byte("meta:exp-index-v1")

func drainSeqKey(sequence int64) []byte {
	return []byte("drain:seq:" + strconv.FormatInt(sequence+1000000000000000000, 10))
}
func drainJobKey(jobID string) []byte { return []byte("drain:job:" + jobID) }
func drainSequenceMetaKey() []byte    { return []byte("meta:drain_sequence") }

// CheckOrInsert mirrors Store.CheckOrInsert's contract exactly: :ok for a
// fresh job, or the existing job ID if the (user_id, idempotent_key) pair
// was already accepted. The shard lock is what makes this atomic instead
// of a racy Get-then-Set — see the package doc comment above.
func (s *PebbleStore) CheckOrInsert(job *Job) (string, bool) {
	hashed := job.dedupHash()
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
	if err := batch.Set(expKey(rec.ExpiresAt, job.ID), nil, nil); err != nil {
		log.Printf("pebble: batch set expiry index for job %s: %v", job.ID, err)
		return "", false
	}
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: commit job %s: %v", job.ID, err)
		return "", false
	}
	s.countStatus(StatusQueued, 1)

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

// putRecordWithExpiry writes the record and its expiry index key together.
func (s *PebbleStore) putRecordWithExpiry(jobID string, rec *pebbleRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("pebble: marshal job %s: %v", jobID, err)
		return
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	batch.Set(jobKey(jobID), data, nil)
	batch.Set(expKey(rec.ExpiresAt, jobID), nil, nil)
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: set job %s: %v", jobID, err)
	}
}

// SetQueueKey writes without its own fsync. The queue key is only used to
// re-enqueue a queue's jobs after a panic (normal restart recovery ignores
// it), and Pebble's log is ordered, so the next synced write makes it durable
// anyway. A dedicated sync here cost one of the few durable writes per
// second the WAL can do, twice per job (the job and its webhook).
func (s *PebbleStore) SetQueueKey(jobID, queueKey string) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return
	}
	rec.QueueKey = queueKey
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if err := s.db.Set(jobKey(jobID), data, pebble.NoSync); err != nil {
		log.Printf("pebble: set queue key for job %s: %v", jobID, err)
	}
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

	hashed := rec.Job.dedupHash()

	batch := s.db.NewBatch()
	defer batch.Close()
	batch.Delete(jobKey(jobID), nil)
	batch.Delete(idemKey(hashed), nil)
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: delete job %s: %v", jobID, err)
		return
	}
	s.countStatus(rec.Job.Status, -1)
}

func (s *PebbleStore) UpdateStatus(jobID string, status Status) {
	rec, ok := s.getRecord(jobID)
	if !ok {
		return
	}
	s.moveStatus(rec.Job.Status, status)
	rec.Job.Status = status
	rec.ExpiresAt = time.Now().Add(ttlForStatus(status)).UnixMilli()
	s.putRecordWithExpiry(jobID, rec)
	if (status == StatusCompleted || status == StatusFailed) && !s.skipDrainEvents.Load() {
		s.recordDrainEvent(rec, status)
	}
}

// SetDrainEventsEnabled: see Store.SetDrainEventsEnabled.
func (s *PebbleStore) SetDrainEventsEnabled(enabled bool) { s.skipDrainEvents.Store(!enabled) }

func (s *PebbleStore) recordDrainEvent(rec *pebbleRecord, status Status) {
	if rec == nil || rec.Job == nil || rec.Job.isWebhookDeliveryJob() {
		return
	}

	s.drainMu.Lock()
	defer s.drainMu.Unlock()

	if _, closer, err := s.db.Get(drainJobKey(rec.Job.ID)); err == nil {
		closer.Close()
		return
	}

	var sequence int64
	if val, closer, err := s.db.Get(drainSequenceMetaKey()); err == nil {
		sequence, _ = strconv.ParseInt(string(val), 10, 64)
		closer.Close()
	}
	sequence++

	event := DrainEvent{
		Sequence:   sequence,
		HashKey:    rec.Job.dedupHash(),
		JobID:      rec.Job.ID,
		Status:     status,
		RecordedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("pebble: marshal drain event for job %s: %v", rec.Job.ID, err)
		return
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	batch.Set(drainSeqKey(sequence), data, nil)
	batch.Set(drainJobKey(rec.Job.ID), []byte(strconv.FormatInt(sequence, 10)), nil)
	batch.Set(drainSequenceMetaKey(), []byte(strconv.FormatInt(sequence, 10)), nil)
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: commit drain event for job %s: %v", rec.Job.ID, err)
	}
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

func (s *PebbleStore) RecoverQueued(queueKey string) []*Job {
	var jobs []*Job
	s.forEachJob(func(rec *pebbleRecord) {
		if rec.QueueKey == queueKey && (rec.Job.Status == StatusQueued || rec.Job.Status == StatusInFlight) {
			rec.Job.Status = StatusQueued
			jobs = append(jobs, rec.Job)
		}
	})
	return jobs
}

func (s *PebbleStore) Counts() StoreCounts {
	queued, inFlight := max(s.queued.Load(), 0), max(s.inFlight.Load(), 0)
	return StoreCounts{TotalJobs: queued + inFlight, QueueDepth: queued}
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
		// StatusInFlight only appears on records written by older versions.
		if rec.Job.Status == StatusQueued || rec.Job.Status == StatusInFlight {
			rec.Job.Status = StatusQueued
			jobs = append(jobs, rec.Job)
		}
	})
	return jobs
}

// ListIdempotentKeys backs drain mode's ledger export. Unlike SQLite,
// pebbleRecord.Job retains the plaintext IdempotentKey (for unrelated
// reasons -- see the package doc comment), but this must never surface it:
// the hash is recomputed the same way CheckOrInsert derives it, and only
// the hash/job_id/status ever go into the returned LedgerEntry. Excludes
// webhook-delivery jobs (WebhookURL == "", see Job.isWebhookDeliveryJob) --
// those are internal delivery bookkeeping, not real user-submitted work, and
// have no business appearing in a ledger meant for tenant-handoff dedup.
func (s *PebbleStore) ListIdempotentKeys() []LedgerEntry {
	var entries []LedgerEntry
	s.forEachJob(func(rec *pebbleRecord) {
		if rec.Job.isWebhookDeliveryJob() {
			return
		}
		entries = append(entries, LedgerEntry{
			HashKey: rec.Job.dedupHash(),
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
	deletePrefix([]byte("exp:"), []byte("exp;"))
	deletePrefix([]byte("drain:seq:"), []byte("drain:seq;"))
	deletePrefix([]byte("drain:job:"), []byte("drain:job;"))
	s.db.Delete(drainSequenceMetaKey(), s.syncOpts)
	s.queued.Store(0)
	s.inFlight.Store(0)
}

func (s *PebbleStore) ListDrainEvents(limit int) []DrainEvent {
	if limit <= 0 {
		limit = 1000
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("drain:seq:"),
		UpperBound: []byte("drain:seq;"),
	})
	if err != nil {
		log.Printf("pebble: iterate drain events: %v", err)
		return nil
	}
	defer iter.Close()

	var events []DrainEvent
	for iter.First(); iter.Valid() && len(events) < limit; iter.Next() {
		var e DrainEvent
		if err := json.Unmarshal(iter.Value(), &e); err != nil {
			continue
		}
		events = append(events, e)
	}
	return events
}

func (s *PebbleStore) AcknowledgeDrainEventsThrough(sequence int64) {
	if sequence <= 0 {
		return
	}

	s.drainMu.Lock()
	defer s.drainMu.Unlock()

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("drain:seq:"),
		UpperBound: []byte("drain:seq;"),
	})
	if err != nil {
		log.Printf("pebble: iterate drain events for ack: %v", err)
		return
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var event DrainEvent
		if err := json.Unmarshal(iter.Value(), &event); err != nil {
			continue
		}
		if event.Sequence > sequence {
			break
		}
		batch.Delete(drainSeqKey(event.Sequence), nil)
		batch.Delete(drainJobKey(event.JobID), nil)
	}
	if err := batch.Commit(s.syncOpts); err != nil {
		log.Printf("pebble: acknowledge drain events through %d: %v", sequence, err)
	}
}

func (s *PebbleStore) cleanupLoop() {
	defer close(s.done)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.deleteExpired(time.Now().UnixMilli())
		case <-s.stop:
			return
		}
	}
}

// deleteExpired deletes jobs whose expiry is before now, reading only the
// expiry index keys that are due.
func (s *PebbleStore) deleteExpired(now int64) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("exp:"), UpperBound: expKeyBound(now)})
	if err != nil {
		log.Printf("pebble: iterate expiry index: %v", err)
		return
	}
	var dueKeys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		dueKeys = append(dueKeys, append([]byte(nil), iter.Key()...))
	}
	iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, k := range dueKeys {
		// "exp:" + 16 hex digits + ":" precede the job id.
		if len(k) > 21 {
			id := string(k[21:])
			if rec, ok := s.getRecord(id); ok && rec.ExpiresAt < now {
				s.DeleteJob(id)
			}
		}
		batch.Delete(k, nil)
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Printf("pebble: delete expiry index keys: %v", err)
	}
}

// backfillExpiryIndex gives every job written by an older version an
// expiry index key, once.
func (s *PebbleStore) backfillExpiryIndex() {
	if _, closer, err := s.db.Get(expIndexMarker); err == nil {
		closer.Close()
		return
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	s.forEachJobIncludingExpired(func(rec *pebbleRecord) {
		batch.Set(expKey(rec.ExpiresAt, rec.Job.ID), nil, nil)
	})
	batch.Set(expIndexMarker, nil, nil)
	if err := batch.Commit(pebble.Sync); err != nil {
		log.Printf("pebble: backfill expiry index: %v", err)
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
