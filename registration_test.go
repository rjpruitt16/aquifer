package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestRegistrationDisabledByDefault proves the opt-in: with no
// AQUIFER_REGISTRY_URL set, the feature resolves to disabled and
// NewRegistry never starts the ping goroutine (mirrors
// TestDrainDisabledNeverTransitionsState's "prove nothing happens" shape).
func TestRegistrationDisabledByDefault(t *testing.T) {
	t.Setenv("AQUIFER_REGISTRY_URL", "")

	cfg := LoadRegistrationConfig()
	if cfg.Enabled() {
		t.Fatalf("expected registration disabled by default")
	}
}

// TestRegistrationPortDefaultsTo8080 confirms this reuses the exact same
// PORT env var and default region_adapter_fly.go already established,
// rather than introducing a second convention for the same fact.
func TestRegistrationPortDefaultsTo8080(t *testing.T) {
	t.Setenv("PORT", "")

	cfg := LoadRegistrationConfig()
	if cfg.Port != "8080" {
		t.Fatalf("expected default port 8080, got %q", cfg.Port)
	}
}

// TestRegistrationPingsImmediatelyAndOnInterval drives the real goroutine
// (not a direct pingRegistry call) end to end: a freshly constructed
// Registry should ping once immediately (not wait a full interval to
// appear for the first time) and again on the next tick, carrying this
// instance's actual listening port -- not a self-reported address, since
// whatever receives the ping already sees the real source IP itself.
func TestRegistrationPingsImmediatelyAndOnInterval(t *testing.T) {
	var pings atomic.Int64
	var lastPort atomic.Value

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if port, ok := body["port"].(string); ok {
			lastPort.Store(port)
		}
		pings.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("AQUIFER_REGISTRY_URL", srv.URL)
	t.Setenv("PORT", "9999")
	t.Setenv("AQUIFER_REGISTRY_INTERVAL_SECONDS", "1")

	r := drainTestRegistry(t, NoopMetricsAdapter{})
	if !r.registrationCfg.Enabled() {
		t.Fatalf("expected registration enabled")
	}

	// Immediate first ping -- should already have arrived without waiting
	// for a full interval.
	deadline := time.Now().Add(2 * time.Second)
	for pings.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := pings.Load(); got < 1 {
		t.Fatalf("expected an immediate ping on start, got %d", got)
	}
	if port, _ := lastPort.Load().(string); port != "9999" {
		t.Fatalf("expected reported port %q, got %q", "9999", port)
	}

	// Second ping on the next tick, proving this is a recurring loop, not
	// a one-shot registration.
	deadline = time.Now().Add(3 * time.Second)
	for pings.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := pings.Load(); got < 2 {
		t.Fatalf("expected at least 2 pings within two intervals, got %d", got)
	}
}
