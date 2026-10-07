package aquifer

import (
	"path/filepath"
	"testing"
)

// Only drain mode reads drain events, so a registry with drain off tells
// its store to skip them: no write per finished job, no lock, and no
// table that grows forever with nothing acknowledging it.
func TestDrainEventsSkippedUnlessDrainEnabled(t *testing.T) {
	for _, backend := range []string{"sqlite", "pebble"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			var store JobStore
			if backend == "sqlite" {
				store = NewStore(filepath.Join(dir, "aquifer.db"))
			} else {
				store = NewPebbleStore(filepath.Join(dir, "pebble"))
			}
			l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
			r := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}, NewBroker(), l8, NoopMetricsAdapter{}, nil)
			t.Cleanup(r.Close)

			finish := func(key string) {
				job := NewJob(&JobRequest{UserID: "u", IdempotentKey: key, URL: "http://x", Method: "POST", WebhookURL: "http://w"})
				store.CheckOrInsert(job)
				store.UpdateStatus(job.ID, StatusCompleted)
			}

			finish("off")
			if events := store.ListDrainEvents(10); len(events) != 0 {
				t.Fatalf("drain off: expected no drain events, got %d", len(events))
			}

			r.ConfigureDrain(DrainConfig{Enabled: true, TimerSeconds: 3600, WebhookURL: "http://127.0.0.1:1"})
			finish("on")
			if events := store.ListDrainEvents(10); len(events) != 1 {
				t.Fatalf("drain on: expected 1 drain event, got %d", len(events))
			}
		})
	}
}
