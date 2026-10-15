// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Soak run 2 stalled a queue this way: a short target API outage halved the
// pace down to minRPS, and the run loop then slept out each 2s dispatch
// interval in place. While it slept it read neither completions (the
// successes that would raise the pace again) nor new jobs, including the
// webhook deliveries its own finished jobs were trying to enqueue on the same
// host's queue. It only got back to its channels once maxConcurrent jobs
// were in flight, so the target API's recovery went unnoticed for
// maxConcurrent / minRPS seconds (17 minutes in the soak).
func TestQueueRecoversPaceWhileWaitingToDispatch(t *testing.T) {
	withRetryBackoff(t, 50*time.Millisecond)
	const jobs = 40
	start := time.Now()
	down := func() bool { return time.Since(start) < time.Second }

	var mu sync.Mutex
	delivered := map[string]bool{}
	// One host serves both the work and the webhooks, as the soak driver
	// does, so webhook deliveries share the queue with the jobs.
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		// Like the soak driver, every response advertises a generous rate,
		// so the first success after the outage should restore full pace.
		w.Header().Set("X-Aqueduct-Rps", "100000")
		switch r.URL.Path {
		case "/work":
			if down() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Write([]byte(`{"ok":true}`))
		case "/hook":
			var p map[string]any
			json.NewDecoder(r.Body).Decode(&p)
			if id, ok := p["job_id"].(string); ok {
				mu.Lock()
				delivered[id] = true
				mu.Unlock()
			}
		}
	}))
	t.Cleanup(host.Close)

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 20}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)

	for i := 0; i < jobs; i++ {
		req := retryJob(fmt.Sprintf("stall-%d", i), host.URL+"/work", host.URL+"/hook", intPtr(RetryUntilComplete))
		if _, err := app.Enqueue(req); err != nil {
			t.Fatal(err)
		}
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(delivered)
	}
	// The outage ends after 1s. At minRPS the old loop needed 20 x 2s = 40s
	// before it noticed; a queue that keeps reading its channels recovers in
	// a few seconds.
	if !waitFor(t, 12*time.Second, func() bool { return count() == jobs }) {
		t.Fatalf("only %d of %d jobs finished 11s after the upstream recovered", count(), jobs)
	}
}
