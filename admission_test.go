// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testAquiferWithLimits(t *testing.T, limits AdmissionLimits) (*Aquifer, *Store) {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	admission := NewAdmissionController(limits, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission, nil)
	t.Cleanup(app.Close)
	return app, store
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
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)

	// First enqueue happens with admission control effectively disabled, so
	// the job is genuinely accepted and durably recorded.
	relaxed := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1_000_000}, dbPath)
	app := NewAquifer(store, registry, broker, l8, relaxed, nil)
	t.Cleanup(app.Close)

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

func TestPebbleSyncLagStaysNearTheFlushInterval(t *testing.T) {
	s := NewPebbleStore(filepath.Join(t.TempDir(), "aquifer.db"))
	defer s.Close()
	time.Sleep(350 * time.Millisecond)
	if lag := s.SyncLag(); lag <= 0 || lag > 300*time.Millisecond {
		t.Fatalf("with the default 100ms flush, lag should stay small: %v", lag)
	}

	t.Setenv("AQUIFER_PEBBLE_FLUSH_INTERVAL_MS", "0")
	synced := NewPebbleStore(filepath.Join(t.TempDir(), "aquifer.db"))
	defer synced.Close()
	if lag := synced.SyncLag(); lag != 0 {
		t.Fatalf("syncing every write: lag %v, want 0", lag)
	}
}

// A retry that arrives while the first request for its key is inserted but
// not yet admitted must not be handed that job's ID: admission then rejects
// and deletes the job, and the retry's caller would hold an ID that 404s
// and never finishes (lost accepted jobs in the Canalis soak).
func TestRetryDuringARejectedInsertIsNotGivenTheDeletedJob(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1})
	inserted := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	app.afterInsert = func() {
		once.Do(func() {
			close(inserted)
			<-proceed
		})
	}

	firstErr := make(chan error, 1)
	go func() {
		_, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
		firstErr <- err
	}()
	<-inserted

	type outcome struct {
		result EnqueueResult
		err    error
	}
	retry := make(chan outcome, 1)
	go func() {
		result, err := app.Enqueue(sampleJobRequest("user-1", "key-1"))
		retry <- outcome{result, err}
	}()
	time.Sleep(100 * time.Millisecond)
	close(proceed)

	var admissionErr *AdmissionRejectedError
	if err := <-firstErr; !errors.As(err, &admissionErr) {
		t.Fatalf("expected the first request to be rejected by admission, got %v", err)
	}
	got := <-retry
	if got.err == nil && got.result.Duplicate {
		if store.GetJob(got.result.JobID) == nil {
			t.Fatalf("retry was told job %s exists, but it was deleted", got.result.JobID)
		}
	}
	if !errors.As(got.err, &admissionErr) {
		t.Fatalf("expected the retry to be judged on its own (rejected), got result=%+v err=%v", got.result, got.err)
	}
}

func TestAPanicAfterClaimingAKeyReleasesIt(t *testing.T) {
	app, store := testAquiferWithLimits(t, AdmissionLimits{})
	before := store.Counts().TotalJobs
	app.afterInsert = func() { panic("boom") }
	func() {
		defer func() { _ = recover() }()
		_, _ = app.Enqueue(sampleJobRequest("user-1", "key-panic"))
	}()
	app.afterInsert = nil
	if after := store.Counts().TotalJobs; after != before {
		t.Fatalf("the panicked job's row was left behind: before=%d after=%d", before, after)
	}

	done := make(chan struct{})
	go func() {
		_, _ = app.Enqueue(sampleJobRequest("user-1", "key-panic"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a retry waited on a key whose holder panicked")
	}
}
