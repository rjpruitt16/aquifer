package aquifer

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
)

const maxRetryAfterSeconds = 60

// Defaults for LoadAdmissionLimits. Body and DB-size ceilings default on,
// sized off the infrastructure this project is actually benchmarked
// against (a single 512MB Fly.io instance with a 1GB volume — see
// benchmark.md): defaultDBMaxBytes leaves 20% headroom on that volume.
// defaultMaxBodyBytes wasn't itself load-tested — it's a conservative,
// standard API-payload ceiling, not a benchmark-derived number. Memory is
// deliberately left disabled by default (see LoadAdmissionLimits) since a
// safe ceiling depends on the deployment's own memory budget, not just
// its disk.
const (
	defaultMaxBodyBytes = 1 * 1024 * 1024
	defaultDBMaxBytes   = 800 * 1024 * 1024
)

// AdmissionLimits are operator-configured ceilings that protect Aquifer itself
// from the traffic it's meant to be absorbing. A zero value disables that
// particular check. Memory is opt-in (LoadAdmissionLimits defaults it to
// disabled); body size and DB size default on — see LoadAdmissionLimits.
type AdmissionLimits struct {
	MemoryLimitMB     int64 // AQUIFER_MEMORY_LIMIT_MB
	MaxBodyBytes      int64 // AQUIFER_MAX_BODY_BYTES
	DBMaxBytes        int64 // AQUIFER_DB_MAX_BYTES
	RetryAfterSeconds int   // AQUIFER_RETRY_AFTER_SECONDS
}

// LoadAdmissionLimits reads the AQUIFER_* admission env vars. Body size and
// DB size fall back to sane, non-zero defaults when unset (see the
// default* constants above) rather than being disabled — Aquifer's whole
// purpose is protecting the process from the traffic it absorbs, so it
// protects itself by default rather than requiring that to be opted into.
// An explicit "0" still disables a given check. Memory has no safe
// one-size-fits-all default (it depends on the deployment's own memory
// budget, not Aquifer's benchmarked disk usage), so it stays disabled
// unless set explicitly — LoadAdmissionLimits logs a warning when it is.
func LoadAdmissionLimits() AdmissionLimits {
	limits := AdmissionLimits{
		MemoryLimitMB:     envInt64("AQUIFER_MEMORY_LIMIT_MB", 0),
		MaxBodyBytes:      envInt64("AQUIFER_MAX_BODY_BYTES", defaultMaxBodyBytes),
		DBMaxBytes:        envInt64("AQUIFER_DB_MAX_BYTES", defaultDBMaxBytes),
		RetryAfterSeconds: int(envInt64("AQUIFER_RETRY_AFTER_SECONDS", 5)),
	}
	if limits.MemoryLimitMB == 0 {
		log.Printf("admission: AQUIFER_MEMORY_LIMIT_MB is not set — process memory is unbounded. Benchmarked safe at 400MB on a 512MB instance; set it to protect against OOM under burst load.")
	}
	return limits
}

func envInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Printf("admission: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

// AdmissionDecision is the result of an admission check on a new (non-duplicate) job.
type AdmissionDecision struct {
	Allowed bool
	Reason  string // "memory" or "db_size"
	Limit   int64
	Current int64
}

// AdmissionRejectedError is returned by Aquifer.Enqueue when a genuinely new
// job is rejected due to memory or DB size pressure. Callers (the HTTP
// server) type-assert on this to build a 429 with Retry-After.
type AdmissionRejectedError struct {
	Decision AdmissionDecision
}

func (e *AdmissionRejectedError) Error() string {
	return fmt.Sprintf("admission rejected: %s at %d exceeds limit %d", e.Decision.Reason, e.Decision.Current, e.Decision.Limit)
}

// AdmissionController evaluates memory and DB size limits at request time.
// Body size is enforced separately at the transport layer via
// http.MaxBytesReader, since that has to happen before the body is even read.
type AdmissionController struct {
	limits AdmissionLimits
	dbPath string

	// rejectStreak counts consecutive rejections with no allowed request in
	// between. Retry-After grows exponentially with it (capped) so clients
	// that keep retrying into sustained overload back off harder over time
	// instead of hammering the same 5s ceiling forever — that hammering is
	// exactly what stops an overloaded instance from ever catching up.
	rejectStreak atomic.Int64
}

func NewAdmissionController(limits AdmissionLimits, dbPath string) *AdmissionController {
	return &AdmissionController{limits: limits, dbPath: dbPath}
}

func (c *AdmissionController) Check() AdmissionDecision {
	if c.limits.MemoryLimitMB > 0 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		currentMB := int64(m.Sys / (1024 * 1024))
		if currentMB > c.limits.MemoryLimitMB {
			log.Printf("admission: rejecting job — memory %dMB exceeds limit %dMB", currentMB, c.limits.MemoryLimitMB)
			c.rejectStreak.Add(1)
			return AdmissionDecision{Allowed: false, Reason: "memory", Limit: c.limits.MemoryLimitMB, Current: currentMB}
		}
	}

	if c.limits.DBMaxBytes > 0 && c.dbPath != "" {
		if size := dbSizeBytes(c.dbPath); size > c.limits.DBMaxBytes {
			log.Printf("admission: rejecting job — db size %d bytes exceeds limit %d bytes", size, c.limits.DBMaxBytes)
			c.rejectStreak.Add(1)
			return AdmissionDecision{Allowed: false, Reason: "db_size", Limit: c.limits.DBMaxBytes, Current: size}
		}
	}

	c.rejectStreak.Store(0)
	return AdmissionDecision{Allowed: true}
}

// Snapshot reports current admission pressure for /health, independent of
// whether any request is currently being rejected.
func (c *AdmissionController) Snapshot() map[string]any {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMB := int64(m.Sys / (1024 * 1024))

	var dbBytes int64
	if c.dbPath != "" {
		dbBytes = dbSizeBytes(c.dbPath)
	}

	return map[string]any{
		"memory_mb":           currentMB,
		"memory_limit_mb":     c.limits.MemoryLimitMB,
		"db_bytes":            dbBytes,
		"db_max_bytes":        c.limits.DBMaxBytes,
		"max_body_bytes":      c.limits.MaxBodyBytes,
		"retry_after_seconds": c.RetryAfterSeconds(),
	}
}

// RetryAfterSeconds returns the configured base value on the first rejection,
// then doubles for each additional consecutive rejection (capped at
// maxRetryAfterSeconds), resetting the moment a request is allowed again.
func (c *AdmissionController) RetryAfterSeconds() int {
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

func (c *AdmissionController) MaxBodyBytes() int64 {
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
func (c *AdmissionController) AnyLimitConfigured() bool {
	return c.limits.MemoryLimitMB > 0 || c.limits.MaxBodyBytes > 0 || c.limits.DBMaxBytes > 0
}
