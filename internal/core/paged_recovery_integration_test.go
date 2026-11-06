// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// watchInMemory samples the largest number of jobs any queue holds in
// memory until stop is closed.
func watchInMemory(registry *Registry, stop <-chan struct{}) *atomic.Int64 {
	peak := &atomic.Int64{}
	go func() {
		for {
			for _, target := range registry.TargetStats() {
				for _, q := range target.Queues {
					if q.InMemory > peak.Load() {
						peak.Store(q.InMemory)
					}
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return peak
}

// After a restart with a backlog many times the page size, every job is
// delivered exactly once, jobs submitted during recovery come after the
// recovered ones, and a queue never holds much more than a page in memory
// (issue #88).
func TestRestartRecoversABigBacklogAPageAtATime(t *testing.T) {
	if testing.Short() {
		t.Skip("restarts Aquifer over a 1,200-job backlog; skipped with -short")
	}
	for _, backend := range []string{"sqlite", "pebble"} {
		t.Run(backend, func(t *testing.T) { pagedRecoveryRun(t, backend) })
	}
}

func pagedRecoveryRun(t *testing.T, backend string) {
	const old, fresh, page = 1200, 50, 100
	t.Setenv("AQUIFER_RECOVERY_PAGE_SIZE", fmt.Sprint(page))

	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	seen := map[string]int{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return // a request from the first run, abandoned at shutdown
		}
		key := strings.TrimPrefix(r.URL.Path, "/work/")
		mu.Lock()
		order = append(order, key)
		seen[key]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	open := func() JobStore {
		if backend == "pebble" {
			return NewPebbleStore(dbPath)
		}
		return NewStore(dbPath)
	}
	start := func() (*Aquifer, *Registry, JobStore, http.Handler) {
		store := open()
		broker := NewBroker()
		l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
		cfg := &Config{Defaults: RateConfig{RPS: 5000, MaxConcurrent: 8}}
		registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
		app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
		return app, registry, store, NewServer(app).Routes()
	}
	submit := func(routes http.Handler, key string) {
		job := sampleJobRequest("tenant", key)
		job.URL = target.URL + "/work/" + key
		job.WebhookURL = ""
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("%s: %d %s", key, rec.Code, rec.Body.String())
		}
	}

	// First run: the target hangs, so everything stays queued, then stop.
	app1, _, _, routes1 := start()
	for i := 0; i < old; i++ {
		submit(routes1, fmt.Sprintf("old-%04d", i))
	}
	app1.Close()

	// Second run: recover, then let the target answer.
	app2, registry2, store2, routes2 := start()
	t.Cleanup(app2.Close)
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	peak := watchInMemory(registry2, stopWatch)
	(&Runtime{Aquifer: app2, Store: store2, Registry: registry2}).RecoverQueuedJobs(dbPath)
	time.Sleep(100 * time.Millisecond)
	if got := peak.Load(); got > page*2 {
		t.Fatalf("right after recovery a queue held %d jobs in memory; the page is %d", got, page)
	}
	for i := 0; i < fresh; i++ {
		submit(routes2, fmt.Sprintf("new-%04d", i))
	}
	close(release)

	if !waitUntil(30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == old+fresh
	}) {
		mu.Lock()
		t.Fatalf("delivered %d distinct jobs, want %d", len(seen), old+fresh)
	}
	time.Sleep(200 * time.Millisecond) // let any duplicate arrive

	mu.Lock()
	defer mu.Unlock()
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("%s delivered %d times", key, n)
		}
	}
	firstNew := -1
	for i, key := range order {
		if strings.HasPrefix(key, "new-") {
			firstNew = i
			break
		}
	}
	// Up to 8 run at once and the first run's in-flight jobs are redone, so
	// allow some overlap; new jobs mustn't jump the recovered backlog.
	if firstNew < old-2*8 {
		t.Fatalf("a new job was delivered at position %d, ahead of most of the %d recovered jobs", firstNew, old)
	}
	if got := peak.Load(); got > page*2 {
		t.Fatalf("a queue held up to %d jobs in memory; with a page of %d it should stay near one page", got, page)
	}
	t.Logf("%s: %d jobs delivered once each; first new job at position %d; max in-memory queue %d", backend, len(seen), firstNew, peak.Load())
}
