package aquifer

import (
	"log"
	"os"
	"strings"
	"time"
)

// Drain mode: opt-in, off by default. When an Aquifer instance goes
// completely idle (no requests anywhere on the process, not just one
// tenant's queue) for AQUIFER_DRAIN_TIMER_SECONDS, it flushes its
// accumulated idempotency ledger to AQUIFER_DRAIN_WEBHOOK_URL and, only on
// confirmed delivery, clears local state -- making the instance safe to
// hand off to a different tenant. The consuming service on the other end
// (deciding which tenant gets a freed instance next, and retaining the
// ledger long-term) is not part of this project -- see README.md "Drain
// mode" for the webhook contract an operator builds against.
//
// Disabled by default means exactly that: when AQUIFER_DRAIN_ENABLED is
// not set to true, no watchdog goroutine is ever started (see
// NewRegistry/ConfigureDrain) -- not a loop that runs and no-ops.
const (
	defaultDrainTimerSeconds  = 45
	drainWatchdogTickInterval = 5 * time.Second
)

type DrainConfig struct {
	Enabled      bool  // AQUIFER_DRAIN_ENABLED
	TimerSeconds int64 // AQUIFER_DRAIN_TIMER_SECONDS
	WebhookURL   string
}

// LoadDrainConfig reads the AQUIFER_DRAIN_* env vars. Enabled defaults to
// false. If enabled but no webhook URL is configured, that's treated as
// disabled (with a warning) rather than a flush attempt with nowhere to
// send it -- fail-safe toward "do nothing," never toward "clear the
// ledger anyway."
func LoadDrainConfig() DrainConfig {
	cfg := DrainConfig{
		Enabled:      envBool("AQUIFER_DRAIN_ENABLED", false),
		TimerSeconds: envInt64("AQUIFER_DRAIN_TIMER_SECONDS", defaultDrainTimerSeconds),
		WebhookURL:   os.Getenv("AQUIFER_DRAIN_WEBHOOK_URL"),
	}
	if cfg.Enabled && cfg.WebhookURL == "" {
		log.Printf("drain: AQUIFER_DRAIN_ENABLED is true but AQUIFER_DRAIN_WEBHOOK_URL is not set — drain mode disabled, nowhere to send the ledger")
		cfg.Enabled = false
	}
	return cfg
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	default:
		log.Printf("drain: invalid %s=%q, using default %v", key, v, def)
		return def
	}
}

// drainWatchdogLoop polls whether every worker has gone idle (Registry.workers
// empty) and, once that's been true for TimerSeconds, attempts a flush. Only
// ever started when drainCfg.Enabled is true — see NewRegistry.
func (r *Registry) drainWatchdogLoop() {
	ticker := time.NewTicker(drainWatchdogTickInterval)
	defer ticker.Stop()

	var becameIdleAt time.Time
	var handled bool // true once this idle period's flush has succeeded (or had nothing to flush)

	for range ticker.C {
		r.mu.Lock()
		idle := len(r.workers) == 0
		r.mu.Unlock()

		if !idle {
			becameIdleAt = time.Time{}
			handled = false
			continue
		}

		if becameIdleAt.IsZero() {
			becameIdleAt = time.Now()
			continue
		}

		if handled {
			continue // already flushed this idle period, wait for new activity
		}

		if time.Since(becameIdleAt) < time.Duration(r.drainCfg.TimerSeconds)*time.Second {
			continue
		}

		handled = r.attemptDrainFlush()
		// If it failed, handled stays false and the next tick retries the
		// whole thing from scratch -- safe, since nothing was cleared.
	}
}

// attemptDrainFlush enumerates the ledger, delivers it, and only clears
// local state on confirmed delivery. Returns true when this idle period is
// "handled" (either a successful flush, or nothing to flush at all) and
// false when it should be retried on the next watchdog tick.
func (r *Registry) attemptDrainFlush() bool {
	entries := r.store.ListIdempotentKeys()
	if len(entries) == 0 {
		return true
	}

	payload := map[string]any{
		"event":      "instance_idle",
		"flushed_at": time.Now().UTC().Format(time.RFC3339),
		"ledger":     entries,
	}

	if !deliverWebhookSync(r.drainCfg.WebhookURL, payload, r.l8, r.metrics) {
		r.metrics.DrainFlushFailed(r.drainCfg.WebhookURL, len(entries))
		log.Printf("drain: failed to deliver ledger flush (%d entries) after retries — not clearing, will retry", len(entries))
		return false
	}

	r.store.ClearIdempotentKeys()
	r.metrics.DrainFlushSucceeded(r.drainCfg.WebhookURL, len(entries))
	log.Printf("drain: flushed and cleared ledger (%d entries)", len(entries))
	return true
}
