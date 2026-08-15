package aquifer

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testAquiferWithLimits(t *testing.T, limits AdmissionLimits) (*Aquifer, *Store) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	t.Cleanup(func() { store.Close() })
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	admission := NewAdmissionController(limits, dbPath)
	return NewAquifer(store, registry, broker, l8, admission, nil), store
}

func sampleJobRequest(userID, idempotentKey string) JobRequest {
	return JobRequest{
		UserID:        userID,
		IdempotentKey: idempotentKey,
		URL:           "https://example.com/webhook",
		Method:        "POST",
		WebhookURL:    "https://example.com/callback",
	}
}

func TestLoadAdmissionLimitsDefaultsOnForSizeChecksOffForMemory(t *testing.T) {
	for _, key := range []string{"AQUIFER_MEMORY_LIMIT_MB", "AQUIFER_MAX_BODY_BYTES", "AQUIFER_DB_MAX_BYTES", "AQUIFER_RETRY_AFTER_SECONDS"} {
		old, had := os.LookupEnv(key)
		os.Unsetenv(key)
		if had {
			t.Cleanup(func() { os.Setenv(key, old) })
		}
	}

	limits := LoadAdmissionLimits()

	if limits.MemoryLimitMB != 0 {
		t.Fatalf("expected memory limit to stay disabled by default, got %d", limits.MemoryLimitMB)
	}
	if limits.MaxBodyBytes != defaultMaxBodyBytes {
		t.Fatalf("expected default max body bytes %d, got %d", defaultMaxBodyBytes, limits.MaxBodyBytes)
	}
	if limits.DBMaxBytes != defaultDBMaxBytes {
		t.Fatalf("expected default db max bytes %d, got %d", defaultDBMaxBytes, limits.DBMaxBytes)
	}
}

func TestLoadAdmissionLimitsExplicitZeroStillDisables(t *testing.T) {
	os.Setenv("AQUIFER_MAX_BODY_BYTES", "0")
	os.Setenv("AQUIFER_DB_MAX_BYTES", "0")
	t.Cleanup(func() {
		os.Unsetenv("AQUIFER_MAX_BODY_BYTES")
		os.Unsetenv("AQUIFER_DB_MAX_BYTES")
	})

	limits := LoadAdmissionLimits()

	if limits.MaxBodyBytes != 0 {
		t.Fatalf("expected explicit AQUIFER_MAX_BODY_BYTES=0 to disable the check, got %d", limits.MaxBodyBytes)
	}
	if limits.DBMaxBytes != 0 {
		t.Fatalf("expected explicit AQUIFER_DB_MAX_BYTES=0 to disable the check, got %d", limits.DBMaxBytes)
	}
}

func TestAdmissionRejectsOverMemoryLimit(t *testing.T) {
	// 1MB is far below what any running Go process actually uses, so this
	// deterministically trips the memory check without needing to allocate
	// anything in the test itself.
	app, _ := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1})

	_, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
	if err == nil {
		t.Fatal("expected admission rejection, got nil error")
	}

	var admissionErr *AdmissionRejectedError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("expected *AdmissionRejectedError, got %T: %v", err, err)
	}
	if admissionErr.Decision.Reason != "memory" {
		t.Fatalf("expected reason=memory, got %q", admissionErr.Decision.Reason)
	}
}

func TestAdmissionAllowsUnderLimit(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1_000_000})

	result, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
	if err != nil {
		t.Fatalf("expected job to be admitted, got error: %v", err)
	}
	if result.Duplicate {
		t.Fatal("expected a fresh job, not a duplicate")
	}
}

func TestAdmissionDisabledByDefault(t *testing.T) {
	// Zero-value limits must mean "unconfigured", not "reject everything" —
	// existing deployments without any AQUIFER_* env vars set must see no
	// behavior change.
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	_, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
	if err != nil {
		t.Fatalf("expected admission control to be a no-op when unconfigured, got: %v", err)
	}
}

func TestAdmissionRejectedJobLeavesNoGhostRow(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1})

	before := store.Counts()
	_, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
	if err == nil {
		t.Fatal("expected admission rejection")
	}
	after := store.Counts()

	if after.TotalJobs != before.TotalJobs {
		t.Fatalf("rejected job left a row behind: before=%d after=%d", before.TotalJobs, after.TotalJobs)
	}
}

func TestAdmissionDuplicateStillSucceedsUnderPressure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	t.Cleanup(func() { store.Close() })
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)

	// First enqueue happens with admission control effectively disabled, so
	// the job is genuinely accepted and durably recorded.
	relaxed := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1_000_000}, dbPath)
	app := NewAquifer(store, registry, broker, l8, relaxed, nil)

	req := sampleJobRequest("user-1", "same-key")
	first, err := app.Enqueue(req)
	if err != nil {
		t.Fatalf("expected first enqueue to succeed, got: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first submission should not be a duplicate")
	}

	// Now swap in a controller that will reject any *new* job, and resubmit
	// the same idempotent key. It must still succeed as a duplicate — the
	// issue's whole point is that retries of already-accepted work aren't
	// punished by admission pressure that started after they were queued.
	app.admission = NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1}, dbPath)

	second, err := app.Enqueue(req)
	if err != nil {
		t.Fatalf("expected duplicate resubmission to succeed under pressure, got: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("expected resubmission to be recognized as a duplicate")
	}
	if second.JobID != first.JobID {
		t.Fatalf("expected same job id, got first=%s second=%s", first.JobID, second.JobID)
	}
}

func TestAdmissionRejectsOverDBSizeLimit(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	t.Cleanup(func() { store.Close() })

	// The DB file already has a schema on disk after migration; a 1-byte
	// ceiling guarantees it reads as over-limit without needing to insert
	// any rows first.
	admission := NewAdmissionController(AdmissionLimits{DBMaxBytes: 1}, dbPath)
	decision := admission.Check()

	if decision.Allowed {
		t.Fatal("expected db size check to reject with a 1-byte ceiling")
	}
	if decision.Reason != "db_size" {
		t.Fatalf("expected reason=db_size, got %q", decision.Reason)
	}
	_ = store // keep store alive so the file exists on disk for the stat check
}

func TestAdmissionSnapshotReportsDisabledWhenUnconfigured(t *testing.T) {
	// A controller instance always exists (the runtime constructs one even
	// with everything at zero), but /health should say "enabled: false"
	// when no actual limit is configured — not just when the Go struct is nil.
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	snap := app.AdmissionSnapshot()

	if snap["enabled"] != false {
		t.Fatalf("expected enabled=false with zero-value limits, got %v", snap["enabled"])
	}
}

func TestAdmissionSnapshotReportsEnabledWhenConfigured(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1_000_000})
	snap := app.AdmissionSnapshot()

	if snap["enabled"] != true {
		t.Fatalf("expected enabled=true with a memory limit set, got %v", snap["enabled"])
	}
}

func TestRetryAfterDoublesOnConsecutiveRejections(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	// 1MB is far below any running process's actual footprint, so every
	// Check() below deterministically rejects.
	c := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1, RetryAfterSeconds: 5}, dbPath)

	want := []int{5, 10, 20, 40, 60, 60, 60}
	for i, w := range want {
		c.Check()
		if got := c.RetryAfterSeconds(); got != w {
			t.Fatalf("rejection #%d: expected Retry-After=%d, got %d", i+1, w, got)
		}
	}
}

func TestRetryAfterResetsAfterAllowedRequest(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	c := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1, RetryAfterSeconds: 5}, dbPath)

	c.Check()
	c.Check()
	c.Check()
	if got := c.RetryAfterSeconds(); got != 20 {
		t.Fatalf("expected backoff to have grown to 20 after 3 rejections, got %d", got)
	}

	// Raise the limit so the next Check() is allowed, then drop it back down.
	c.limits.MemoryLimitMB = 1_000_000
	c.Check()
	c.limits.MemoryLimitMB = 1

	if got := c.RetryAfterSeconds(); got != 5 {
		t.Fatalf("expected Retry-After to reset to base 5 after an allowed request, got %d", got)
	}
}

func TestRetryAfterDefaultsWhenUnconfigured(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	c := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1}, dbPath)

	if got := c.RetryAfterSeconds(); got != 5 {
		t.Fatalf("expected default base Retry-After=5 with no rejections yet, got %d", got)
	}
}
