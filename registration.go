package aquifer

import (
	"log"
	"os"
	"time"
)

// External registration: opt-in, off by default. When AQUIFER_REGISTRY_URL
// is set, this instance periodically reports its own listening port to that
// URL so an external control plane (Canalis, or anything else someone
// builds against the same contract) can assign tenants to it. Generic on
// purpose, not Canalis-specific: this instance has no idea what's on the
// other end, it's just a webhook target, delivered through the exact same
// deliverWebhookSync path (retries, L8 signing) drain mode's ledger flush
// already uses.
//
// Only the port is reported, not a full address -- whatever's receiving
// this ping already sees the caller's real source IP on the connection
// itself, so a full self-reported address would just be redundant with
// that. This also makes it more portable than region-redirect's
// Fly-specific ".internal" DNS convention (region_adapter_fly.go) -- this
// feature isn't Fly-specific at all.
//
// Disabled by default means exactly that: when AQUIFER_REGISTRY_URL is
// unset, no goroutine is ever started (see NewRegistry) -- not a loop that
// runs and no-ops.
const defaultRegistrationIntervalSeconds = 15

type RegistrationConfig struct {
	URL             string // AQUIFER_REGISTRY_URL -- presence enables this feature
	Port            string // PORT (same env var and "8080" default region_adapter_fly.go already uses)
	IntervalSeconds int64  // AQUIFER_REGISTRY_INTERVAL_SECONDS
}

func (cfg RegistrationConfig) Enabled() bool {
	return cfg.URL != ""
}

// LoadRegistrationConfig reads the AQUIFER_REGISTRY_* env vars (plus the
// already-established PORT var, not a new one of its own).
func LoadRegistrationConfig() RegistrationConfig {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return RegistrationConfig{
		URL:             os.Getenv("AQUIFER_REGISTRY_URL"),
		Port:            port,
		IntervalSeconds: envInt64("AQUIFER_REGISTRY_INTERVAL_SECONDS", defaultRegistrationIntervalSeconds),
	}
}

// registrationLoop pings cfg.URL every IntervalSeconds with this instance's
// listening port. Only ever started when cfg.Enabled() -- see NewRegistry.
// Pings immediately on start rather than waiting a full interval, so a
// freshly-booted instance doesn't sit unregistered for the first tick.
func (r *Registry) registrationLoop() {
	ticker := time.NewTicker(time.Duration(r.registrationCfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	r.pingRegistry()
	for range ticker.C {
		r.pingRegistry()
	}
}

func (r *Registry) pingRegistry() {
	payload := map[string]any{
		"port":        r.registrationCfg.Port,
		"reported_at": time.Now().UTC().Format(time.RFC3339),
	}
	if !deliverWebhookSync(r.registrationCfg.URL, payload, r.l8, r.metrics) {
		log.Printf("registration: failed to report state to %s after retries", r.registrationCfg.URL)
	}
}
