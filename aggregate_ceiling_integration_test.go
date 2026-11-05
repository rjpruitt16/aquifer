// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTargetRPSCeilingHoldsAcrossTenantQueues checks that a target's YAML rps
// is a ceiling on the total it receives, not on each tenant's queue. Three
// tenants flood one fast target configured at 30 rps; the target must see
// about 30 requests a second, not about 3 x 30 (issue #146).
func TestTargetRPSCeilingHoldsAcrossTenantQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test; skipped with -short")
	}
	const ceiling = 30.0

	var hits atomic.Int64
	var mu sync.Mutex
	perTenant := map[string]int{}
	counting := atomic.Bool{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		hits.Add(1)
		if counting.Load() {
			mu.Lock()
			perTenant[strings.TrimPrefix(r.URL.Path, "/work/")]++
			mu.Unlock()
		}
		w.Header().Set("X-Aqueduct-Account-Queue", "enabled")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	host := mustHost(t, upstream.URL)

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{
		Defaults:  RateConfig{RPS: 2, MaxConcurrent: 1},
		Upstreams: map[string]RateConfig{host: {RPS: ceiling, MaxConcurrent: 4}},
	}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)
	routes := NewServer(app).Routes()

	submit := func(user string, n int) {
		job := sampleJobRequest(user, fmt.Sprintf("%s-%d", user, n))
		job.URL = upstream.URL + "/work/" + user
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("%s job %d: expected 201, got %d: %s", user, n, rec.Code, rec.Body.String())
		}
	}

	// Teach Aquifer that this target wants a queue per tenant, then flood it
	// from three tenants with more work than the window can drain.
	submit("warm", 0)
	if !waitUntil(5*time.Second, func() bool { return hits.Load() >= 1 }) {
		t.Fatal("the target never received the first job")
	}
	for n := 0; n < 600; n++ {
		for _, user := range []string{"a", "b", "c"} {
			submit(user, n)
		}
	}

	// Let every queue get going, then measure a steady window.
	time.Sleep(2 * time.Second)
	start := hits.Load()
	counting.Store(true)
	const window = 6 * time.Second
	time.Sleep(window)
	counting.Store(false)
	delivered := float64(hits.Load()-start) / window.Seconds()

	t.Logf("delivered %.1f/s against a %.0f rps ceiling with 3 tenant queues", delivered, ceiling)
	if delivered > ceiling*1.2 {
		t.Fatalf("target received %.1f requests/s; the %.0f rps ceiling should hold across tenant queues", delivered, ceiling)
	}
	if delivered < ceiling*0.6 {
		t.Fatalf("target received only %.1f requests/s; the ceiling should still be usable", delivered)
	}

	// The shared ceiling must not let one tenant take the slots: with equal
	// backlogs each of the three should get a fair part of the window.
	mu.Lock()
	defer mu.Unlock()
	total := perTenant["a"] + perTenant["b"] + perTenant["c"]
	t.Logf("per tenant in the window: %v", perTenant)
	for _, user := range []string{"a", "b", "c"} {
		if share := float64(perTenant[user]) / float64(total); share < 0.2 {
			t.Fatalf("tenant %s got %.0f%% of the window (%v); each should get about a third", user, share*100, perTenant)
		}
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
