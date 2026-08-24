package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// recordingMetrics wraps NoopMetricsAdapter, capturing only the two calls
// these tests care about.
type recordingMetrics struct {
	NoopMetricsAdapter
	succeeded atomic.Int64
	failed    atomic.Int64
}

func (m *recordingMetrics) DrainFlushSucceeded(instanceKey string, ledgerSize int) { m.succeeded.Add(1) }
func (m *recordingMetrics) DrainFlushFailed(instanceKey string, ledgerSize int)    { m.failed.Add(1) }

func drainTestRegistry(t *testing.T, metrics MetricsAdapter) *Registry {
	t.Helper()
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	return NewRegistry(store, cfg, broker, l8, metrics, nil)
}

// skipL8Probe reports whether this request is deliverWebhookSync's own
// preliminary L8-trust-handshake probe (GET /.well-known/l8), writing 404
// and returning true if so, so test handlers below only count the actual
// webhook delivery attempt, not this unrelated preliminary request.
func skipL8Probe(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/.well-known/l8" {
		w.WriteHeader(http.StatusNotFound)
		return true
	}
	return false
}

func seedLedgerEntry(t *testing.T, r *Registry) {
	t.Helper()
	job := &Job{
		ID: generateID(), UserID: "u1", IdempotentKey: "k1",
		URL: "https://example.com", Method: "POST", WebhookURL: "https://example.com",
		Status: StatusQueued, CreatedAt: time.Now().UnixMilli(),
	}
	r.store.CheckOrInsert(job)
}

// TestDrainOptInStartsWatchdogOnlyWhenEnabled is the test proving "opt-in,"
// not just "URL happened to be unset": constructs a disabled Registry and
// an enabled one, each measuring the goroutine-count delta its own
// construction causes, and asserts the enabled path adds strictly more
// goroutines than the disabled path (the watchdog). This is a relative
// comparison rather than an absolute count so it's not flaky against
// whatever constant background-goroutine overhead L8Registry/Store/etc.
// contribute equally either way.
func TestDrainOptInStartsWatchdogOnlyWhenEnabled(t *testing.T) {
	settle := func() { runtime.GC(); time.Sleep(20 * time.Millisecond) }

	t.Setenv("AQUIFER_DRAIN_ENABLED", "")
	t.Setenv("AQUIFER_DRAIN_WEBHOOK_URL", "")
	settle()
	beforeDisabled := runtime.NumGoroutine()
	disabled := drainTestRegistry(t, NoopMetricsAdapter{})
	if disabled.drainCfg.Enabled {
		t.Fatalf("expected drain mode disabled by default")
	}
	settle()
	deltaDisabled := runtime.NumGoroutine() - beforeDisabled

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("AQUIFER_DRAIN_ENABLED", "true")
	t.Setenv("AQUIFER_DRAIN_WEBHOOK_URL", srv.URL)
	t.Setenv("AQUIFER_DRAIN_TIMER_SECONDS", "3600") // long enough the watchdog never fires during this test
	settle()
	beforeEnabled := runtime.NumGoroutine()
	enabled := drainTestRegistry(t, NoopMetricsAdapter{})
	if !enabled.drainCfg.Enabled {
		t.Fatalf("expected drain mode enabled")
	}
	settle()
	deltaEnabled := runtime.NumGoroutine() - beforeEnabled

	if deltaEnabled <= deltaDisabled {
		t.Fatalf("expected enabling drain mode to start a watchdog goroutine: disabled delta=%d, enabled delta=%d", deltaDisabled, deltaEnabled)
	}
}

// TestDrainEnabledButNoWebhookURLIsDisabled is the fail-safe case: enabled
// but misconfigured must behave as disabled, never as "flush with nowhere
// to send it."
func TestDrainEnabledButNoWebhookURLIsDisabled(t *testing.T) {
	t.Setenv("AQUIFER_DRAIN_ENABLED", "true")
	t.Setenv("AQUIFER_DRAIN_WEBHOOK_URL", "")

	cfg := LoadDrainConfig()
	if cfg.Enabled {
		t.Fatalf("expected enabled-but-no-URL to resolve to disabled")
	}
}

// TestAttemptDrainFlushSucceedsAndClears is the success path: webhook
// accepts on the first attempt, store is cleared, success metric fires.
func TestAttemptDrainFlushSucceedsAndClears(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		attempts.Add(1)
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if payload["event"] != "instance_idle" {
			t.Errorf("expected event=instance_idle, got %v", payload["event"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := &recordingMetrics{}
	r := drainTestRegistry(t, metrics)
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, WebhookURL: srv.URL}
	seedLedgerEntry(t, r)

	if !r.attemptDrainFlush() {
		t.Fatalf("expected attemptDrainFlush to succeed")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected exactly 1 delivery attempt, got %d", attempts.Load())
	}
	if entries := r.store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("expected store cleared after successful flush, got %d entries", len(entries))
	}
	if metrics.succeeded.Load() != 1 {
		t.Fatalf("expected DrainFlushSucceeded to fire once, got %d", metrics.succeeded.Load())
	}
	if metrics.failed.Load() != 0 {
		t.Fatalf("expected DrainFlushFailed not to fire, got %d", metrics.failed.Load())
	}
}

// TestAttemptDrainFlushEmptyLedgerSkipsDelivery: nothing to flush means no
// webhook call at all, and is still reported as "handled."
func TestAttemptDrainFlushEmptyLedgerSkipsDelivery(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := drainTestRegistry(t, NoopMetricsAdapter{})
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, WebhookURL: srv.URL}

	if !r.attemptDrainFlush() {
		t.Fatalf("expected attemptDrainFlush to report handled for an empty ledger")
	}
	if attempts.Load() != 0 {
		t.Fatalf("expected no delivery attempt for an empty ledger, got %d", attempts.Load())
	}
}

// TestAttemptDrainFlushFailureDoesNotClear is the correctness-critical
// case: exhausting all retries must leave the store untouched, never clear
// on an unconfirmed delivery.
func TestAttemptDrainFlushFailureDoesNotClear(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	metrics := &recordingMetrics{}
	r := drainTestRegistry(t, metrics)
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, WebhookURL: srv.URL}
	seedLedgerEntry(t, r)

	if r.attemptDrainFlush() {
		t.Fatalf("expected attemptDrainFlush to report failure after exhausting retries")
	}
	if entries := r.store.ListIdempotentKeys(); len(entries) != 1 {
		t.Fatalf("expected ledger untouched after a failed flush, got %d entries", len(entries))
	}
	if metrics.failed.Load() != 1 {
		t.Fatalf("expected DrainFlushFailed to fire once, got %d", metrics.failed.Load())
	}
	if metrics.succeeded.Load() != 0 {
		t.Fatalf("expected DrainFlushSucceeded not to fire, got %d", metrics.succeeded.Load())
	}

	// The next attempt (mirroring the watchdog's next tick) must retry the
	// whole thing from scratch, safely, since nothing was cleared.
	if r.attemptDrainFlush() {
		t.Fatalf("expected retry to still fail against the same failing endpoint")
	}
	if entries := r.store.ListIdempotentKeys(); len(entries) != 1 {
		t.Fatalf("expected ledger still present after a second failed attempt, got %d entries", len(entries))
	}
}

// TestAttemptDrainFlushSucceedsWithinRetryBudget: failing on the first few
// attempts but succeeding before the retry budget is exhausted must still
// clear the store -- success anywhere in the budget counts, not just a
// first-attempt success.
func TestAttemptDrainFlushSucceedsWithinRetryBudget(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := &recordingMetrics{}
	r := drainTestRegistry(t, metrics)
	r.drainCfg = DrainConfig{Enabled: true, TimerSeconds: 1, WebhookURL: srv.URL}
	seedLedgerEntry(t, r)

	if !r.attemptDrainFlush() {
		t.Fatalf("expected attemptDrainFlush to eventually succeed within the retry budget")
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected exactly 3 delivery attempts, got %d", attempts.Load())
	}
	if entries := r.store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("expected store cleared after eventual success, got %d entries", len(entries))
	}
	if metrics.succeeded.Load() != 1 {
		t.Fatalf("expected DrainFlushSucceeded to fire once, got %d", metrics.succeeded.Load())
	}
}
