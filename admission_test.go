package aquifer

import (
	"errors"
	"path/filepath"
	"testing"
)

func testAquiferWithLimits(t *testing.T, limits AdmissionLimits) (*Aquifer, *Store) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{})
	admission := NewAdmissionController(limits, dbPath)
	return NewAquifer(store, registry, broker, l8, admission), store
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
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{})

	// First enqueue happens with admission control effectively disabled, so
	// the job is genuinely accepted and durably recorded.
	relaxed := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1_000_000}, dbPath)
	app := NewAquifer(store, registry, broker, l8, relaxed)

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
