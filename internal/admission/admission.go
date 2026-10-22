// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
)

const maxRetryAfterSeconds = 60

// Defaults for LoadLimits. Body and DB-size ceilings default on.
// The DB ceiling is derived from the disk the database lives on (see
// autoDBMaxBytes); defaultDBMaxBytes is only the fallback when the disk
// can't be measured, sized for the 1GB volume this project was first
// benchmarked on. defaultMaxBodyBytes wasn't itself load-tested — it's a
// conservative, standard API-payload ceiling, not a benchmark-derived
// number. Memory is deliberately left disabled by default (see
// LoadLimits) since a safe ceiling depends on the deployment's own
// memory budget, not just its disk.
const (
	defaultMaxBodyBytes = 1 * 1024 * 1024
	defaultDBMaxBytes   = 800 * 1024 * 1024
)

// autoDBMaxBytes marks an unset AQUIFER_DB_MAX_BYTES: NewController
// replaces it with dbShareOfDisk of the space the database can use, so the
// ceiling fits whatever volume Aquifer runs on without configuration. A
// fixed default either wastes a large volume (the week-long soak shed 15-20%
// of jobs at 800MB on a 10GB volume) or overfills a small one.
const (
	autoDBMaxBytes int64 = -1
	dbShareOfDisk        = 0.8

	// autoMemoryLimitMB (AQUIFER_MEMORY_LIMIT_MB unset) sizes the memory
	// ceiling from what the process may use: memoryShareOfMachine of the
	// container's limit, or of the machine's memory without one.
	autoMemoryLimitMB    int64 = -1
	memoryShareOfMachine       = 0.7
)

// Limits are operator-configured ceilings that protect Aquifer itself
// from the traffic it's meant to be absorbing. A zero value disables that
// particular check. All three default on; see LoadLimits.
type Limits struct {
	MemoryLimitMB     int64 // AQUIFER_MEMORY_LIMIT_MB
	MaxBodyBytes      int64 // AQUIFER_MAX_BODY_BYTES
	DBMaxBytes        int64 // AQUIFER_DB_MAX_BYTES
	RetryAfterSeconds int   // AQUIFER_RETRY_AFTER_SECONDS
	MaxSyncLagMS      int64 // AQUIFER_MAX_SYNC_LAG_MS
}

// defaultMaxSyncLagMS: refuse new jobs once accepted ones may have gone a
// second without reaching disk, ten times the default flush interval.
const defaultMaxSyncLagMS = 1000

// LoadLimits reads the AQUIFER_* admission env vars. Body size and
// DB size fall back to sane, non-zero defaults when unset (see the
// default* constants above) rather than being disabled — Aquifer's whole
// purpose is protecting the process from the traffic it absorbs, so it
// protects itself by default rather than requiring that to be opted into.
// An explicit "0" still disables a given check.
//
// Memory defaults to 70% of what the process may use (deriveMemoryLimitMB).
// It used to default to off. In storm test 1 (2GB machine, 1,500 jobs/s for
// two hours) Aquifer never refused a request, the kernel OOM-killed it 25
// times, and 1,440 accepted jobs were lost. The derived value is a guess
// about the machine, not about what else runs on it, so it's logged with a
// warning to set it explicitly.
func LoadLimits() Limits {
	limits := Limits{
		MemoryLimitMB:     env.Int64("AQUIFER_MEMORY_LIMIT_MB", autoMemoryLimitMB),
		MaxBodyBytes:      env.Int64("AQUIFER_MAX_BODY_BYTES", defaultMaxBodyBytes),
		DBMaxBytes:        env.Int64("AQUIFER_DB_MAX_BYTES", autoDBMaxBytes),
		RetryAfterSeconds: int(env.Int64("AQUIFER_RETRY_AFTER_SECONDS", 5)),
		MaxSyncLagMS:      env.Int64("AQUIFER_MAX_SYNC_LAG_MS", defaultMaxSyncLagMS),
	}
	switch {
	case limits.MemoryLimitMB == autoMemoryLimitMB:
		limits.MemoryLimitMB = deriveMemoryLimitMB(memoryLimitSources)
	case limits.MemoryLimitMB == 0:
		log.Printf("admission: AQUIFER_MEMORY_LIMIT_MB=0, so memory is unbounded and nothing is refused before the kernel's OOM killer acts")
	}
	return limits
}

// Where the memory available to this process is read from, most specific
// first. Tests point these at temporary files.
var memoryLimitSources = memorySources{
	cgroupV2: "/sys/fs/cgroup/memory.max",
	cgroupV1: "/sys/fs/cgroup/memory/memory.limit_in_bytes",
	meminfo:  "/proc/meminfo",
}

type memorySources struct{ cgroupV2, cgroupV1, meminfo string }

// deriveMemoryLimitMB returns memoryShareOfMachine of the container's
// memory limit, or of the machine's memory when there is no limit, or 0
// (disabled) when neither can be read, e.g. on macOS.
func deriveMemoryLimitMB(src memorySources) int64 {
	bytes, from := availableMemoryBytes(src)
	if bytes <= 0 {
		log.Printf("admission: AQUIFER_MEMORY_LIMIT_MB is not set and this machine's memory can't be read; memory is unbounded. Set it to about 70%% of the memory Aquifer may use.")
		return 0
	}
	limit := int64(float64(bytes)*memoryShareOfMachine) / (1024 * 1024)
	log.Printf("admission: AQUIFER_MEMORY_LIMIT_MB not set; refusing new jobs above %dMB (%.0f%% of %dMB, %s). If anything else runs on this machine (Valkey, a sidecar), or it is resized, set AQUIFER_MEMORY_LIMIT_MB for it.",
		limit, memoryShareOfMachine*100, bytes/(1024*1024), from)
	return limit
}

func availableMemoryBytes(src memorySources) (int64, string) {
	// cgroup v2 says "max" without a limit; v1 reports a huge number.
	if n, ok := readIntFile(src.cgroupV2); ok && n > 0 && n < 1<<60 {
		return n, "the container's memory limit"
	}
	if n, ok := readIntFile(src.cgroupV1); ok && n > 0 && n < 1<<60 {
		return n, "the container's memory limit"
	}
	if data, err := os.ReadFile(src.meminfo); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemTotal:" {
				if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil && kb > 0 {
					return kb * 1024, "the machine's memory"
				}
			}
		}
	}
	return 0, ""
}

func readIntFile(path string) (int64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	return n, err == nil
}

// Decision is the result of an admission check on a new (non-duplicate) job.
type Decision struct {
	Allowed bool
	Reason  string // "memory", "db_size", "sync_lag", "upstream_queue", or "queue_fair_share"
	Limit   int64
	Current int64
	Queue   *QueueSnapshot
}

// RejectedError is returned when a genuinely new job is rejected by
// process, storage, or queue pressure. The HTTP server turns it into a 429.
type RejectedError struct {
	Decision Decision
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("admission rejected: %s (current %d, limit %d)", e.Decision.Reason, e.Decision.Current, e.Decision.Limit)
}

// Controller evaluates memory and DB size limits at request time.
// Body size is enforced separately at the transport layer via
// http.MaxBytesReader, since that has to happen before the body is even read.
type Controller struct {
	limits Limits
	dbPath string

	// rejectStreak counts consecutive rejections with no allowed request in
	// between. Retry-After grows exponentially with it (capped) so clients
	// that keep retrying into sustained overload back off harder over time
	// instead of hammering the same 5s ceiling forever — that hammering is
	// exactly what stops an overloaded instance from ever catching up.
	rejectStreak atomic.Int64

	// Memory and DB size are sampled at most once per SampleTTL.
	// Check runs on every submission, and a Pebble store's size means
	// stat-ing every file in its directory. Under load that (with the
	// stop-the-world ReadMemStats memory used to come from) was about 9% of
	// CPU, for numbers that barely move in a few hundred milliseconds.
	memSample Sampled
	dbSample  Sampled

	// syncLag reports how long accepted jobs may have gone unsynced (the
	// Pebble store's SyncLag); nil when the store syncs every write.
	syncLag func() time.Duration
}

// syncLagReporter is a store whose acknowledged writes can sit in memory
// before a background sync (PebbleStore with a flush interval).
type syncLagReporter interface{ SyncLag() time.Duration }

// WatchSyncLag makes Check refuse new jobs while store's acknowledged
// writes have gone longer than AQUIFER_MAX_SYNC_LAG_MS without a sync.
func (c *Controller) WatchSyncLag(store any) {
	if c == nil {
		return
	}
	if r, ok := store.(syncLagReporter); ok {
		c.syncLag = r.SyncLag
	}
}

func (c *Controller) syncLagMS() int64 {
	if c.syncLag == nil {
		return 0
	}
	return c.syncLag().Milliseconds()
}

// SampleTTL is how long a Sampled reading is reused before it is read again.
const SampleTTL = 250 * time.Millisecond

// Sampled caches an int64 reading for SampleTTL, for readings that are too
// costly to take on every request.
type Sampled struct {
	mu    sync.Mutex
	value int64
	at    time.Time
}

// Get returns the cached reading, calling read for a fresh one once it is
// older than SampleTTL.
func (s *Sampled) Get(read func() int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.at.IsZero() || time.Since(s.at) >= SampleTTL {
		s.value = read()
		s.at = time.Now()
	}
	return s.value
}

// processMemoryMB is the memory the Go runtime is holding: everything it
// has mapped from the OS, less what it has already handed back. It used to
// be MemStats.Sys, which counts what was handed back too and so never comes
// down. In storm test 2 one spike past the 1,400MB limit left Sys at
// 1,417MB for good while resident memory sat near 750MB, and admission
// refused every job from then on. runtime/metrics reads this without
// stopping the world, unlike ReadMemStats.
func processMemoryMB() int64 {
	total, released := goMemoryBytes()
	return int64((total - released) / (1024 * 1024))
}

// reservedMemoryMB is everything the runtime has mapped, released or not
// (what MemStats.Sys reports). /health shows it beside processMemoryMB.
func reservedMemoryMB() int64 {
	total, _ := goMemoryBytes()
	return int64(total / (1024 * 1024))
}

func goMemoryBytes() (total, released uint64) {
	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
	}
	metrics.Read(samples)
	return samples[0].Value.Uint64(), samples[1].Value.Uint64()
}

func NewController(limits Limits, dbPath string) *Controller {
	if limits.DBMaxBytes < 0 {
		limits.DBMaxBytes = deriveDBMaxBytes(dbPath)
	}
	return &Controller{limits: limits, dbPath: dbPath}
}

// deriveDBMaxBytes sizes the DB ceiling from the disk: dbShareOfDisk of what
// the database can grow into, which is the filesystem's free space plus what
// the database already occupies. Measured from free space rather than the
// disk's total so a database sharing a disk with other data (a laptop, a
// shared host) gets a ceiling the disk can actually reach. Measured once at
// startup, so other data growing on a shared disk later isn't accounted for.
func deriveDBMaxBytes(dbPath string) int64 {
	dir := filepath.Dir(dbPath)
	free, ok := volumeAvailableBytes(dir)
	if !ok {
		log.Printf("admission: AQUIFER_DB_MAX_BYTES not set and the disk at %s can't be measured; using %d bytes", dir, defaultDBMaxBytes)
		return defaultDBMaxBytes
	}
	usable := free + dbSizeBytes(dbPath)
	limit := int64(float64(usable) * dbShareOfDisk)
	log.Printf("admission: AQUIFER_DB_MAX_BYTES not set; capping the database at %d bytes (%.0f%% of the %d bytes it can use on %s)",
		limit, dbShareOfDisk*100, usable, dir)
	return limit
}

func (c *Controller) Check() Decision {
	if c.limits.MemoryLimitMB > 0 {
		currentMB := c.memSample.Get(processMemoryMB)
		if currentMB > c.limits.MemoryLimitMB {
			log.Printf("admission: rejecting job — memory %dMB exceeds limit %dMB", currentMB, c.limits.MemoryLimitMB)
			c.rejectStreak.Add(1)
			return Decision{Allowed: false, Reason: "memory", Limit: c.limits.MemoryLimitMB, Current: currentMB}
		}
	}

	if c.limits.DBMaxBytes > 0 && c.dbPath != "" {
		if size := c.dbSample.Get(func() int64 { return dbSizeBytes(c.dbPath) }); size > c.limits.DBMaxBytes {
			log.Printf("admission: rejecting job — db size %d bytes exceeds limit %d bytes", size, c.limits.DBMaxBytes)
			c.rejectStreak.Add(1)
			return Decision{Allowed: false, Reason: "db_size", Limit: c.limits.DBMaxBytes, Current: size}
		}
	}

	// Accepting a job tells the sender it's safe. While syncs are this far
	// behind, a crash loses everything accepted since the last one, so say
	// "later" instead; fewer writes also lets the sync catch up.
	if c.limits.MaxSyncLagMS > 0 {
		if lag := c.syncLagMS(); lag > c.limits.MaxSyncLagMS {
			log.Printf("admission: rejecting job — last WAL sync %dms ago exceeds limit %dms", lag, c.limits.MaxSyncLagMS)
			c.rejectStreak.Add(1)
			return Decision{Allowed: false, Reason: "sync_lag", Limit: c.limits.MaxSyncLagMS, Current: lag}
		}
	}

	c.rejectStreak.Store(0)
	return Decision{Allowed: true}
}

// Snapshot reports current admission pressure for /health, independent of
// whether any request is currently being rejected.
func (c *Controller) Snapshot() map[string]any {
	currentMB := processMemoryMB()

	var dbBytes int64
	if c.dbPath != "" {
		dbBytes = dbSizeBytes(c.dbPath)
	}

	return map[string]any{
		"memory_mb":           currentMB,
		"memory_reserved_mb":  reservedMemoryMB(),
		"memory_limit_mb":     c.limits.MemoryLimitMB,
		"db_bytes":            dbBytes,
		"db_max_bytes":        c.limits.DBMaxBytes,
		"max_body_bytes":      c.limits.MaxBodyBytes,
		"sync_lag_ms":         c.syncLagMS(),
		"max_sync_lag_ms":     c.limits.MaxSyncLagMS,
		"retry_after_seconds": c.RetryAfterSeconds(),
	}
}

// RetryAfterSeconds returns the configured base value on the first rejection,
// then doubles for each additional consecutive rejection (capped at
// maxRetryAfterSeconds), resetting the moment a request is allowed again.
func (c *Controller) RetryAfterSeconds() int {
	base := c.limits.RetryAfterSeconds
	if base <= 0 {
		base = 5
	}

	streak := c.rejectStreak.Load()
	if streak <= 1 {
		return base
	}

	backoff := base
	for i := int64(1); i < streak && backoff < maxRetryAfterSeconds; i++ {
		backoff *= 2
	}
	if backoff > maxRetryAfterSeconds {
		backoff = maxRetryAfterSeconds
	}
	return backoff
}

func (c *Controller) MaxBodyBytes() int64 {
	return c.limits.MaxBodyBytes
}

// dbSizeBytes handles both storage backends: SQLite's dbPath is a single
// file, so os.Stat's size is exact. Pebble's dbPath is a directory of SST
// and log files — stat-ing the directory entry itself would return a
// near-constant small size regardless of actual data (the same trap
// avoided for Mnesia's directory-based storage on the Elixir side), so a
// directory is summed recursively instead.
func dbSizeBytes(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if !info.IsDir() {
		return info.Size()
	}

	var total int64
	filepath.Walk(path, func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			total += fi.Size()
		}
		return nil
	})
	return total
}

// AnyLimitConfigured reports whether at least one admission limit is
// actually active, as opposed to merely whether a controller instance
// exists (the runtime always constructs one, even with everything at zero).
func (c *Controller) AnyLimitConfigured() bool {
	return c.limits.MemoryLimitMB > 0 || c.limits.MaxBodyBytes > 0 || c.limits.DBMaxBytes > 0
}

// FullnessRatio is the highest of memory use, database size and WAL sync
// lag against their configured limits, reusing the same samples Check uses.
func (c *Controller) FullnessRatio() float64 {
	if c == nil {
		return 0
	}
	var ratio float64
	if c.limits.MemoryLimitMB > 0 {
		ratio = math.Max(ratio, float64(c.memSample.Get(processMemoryMB))/float64(c.limits.MemoryLimitMB))
	}
	if c.limits.DBMaxBytes > 0 && c.dbPath != "" {
		size := c.dbSample.Get(func() int64 { return dbSizeBytes(c.dbPath) })
		ratio = math.Max(ratio, float64(size)/float64(c.limits.DBMaxBytes))
	}
	if c.limits.MaxSyncLagMS > 0 {
		ratio = math.Max(ratio, float64(c.syncLagMS())/float64(c.limits.MaxSyncLagMS))
	}
	return ratio
}
