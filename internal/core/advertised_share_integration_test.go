// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAdvertisedRatesSetEachTenantsShare checks that the target, not
// Aquifer's configuration, decides its total rate and how it is split
// between tenants, through POST /jobs. The target is configured at only
// 2 rps; three tenants stay backlogged, and the target advertises wildly
// different rates to each (X-Aqueduct-Rps 60, 6 and 0.6). The total must
// rise to the highest advertised, 60/s, split 90.0%, 9.0% and 0.9%. Halfway
// through it swaps the first and last, and the shares must follow.
func TestAdvertisedRatesSetEachTenantsShare(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test (about 25 s); skipped with -short")
	}
	const (
		configured = 2.0
		top        = 60.0 // the highest rate advertised
		phase      = 12.0 // seconds per phase
		settle     = 2.0  // seconds at the start of each phase left out
	)
	advertise := map[string]float64{"vip": 60, "standard": 6, "low": 0.6}
	swapped := map[string]float64{"vip": 0.6, "standard": 6, "low": 60}

	var mu sync.Mutex
	arrivals := map[string][]float64{} // tenant -> seconds since start
	var start time.Time
	var started atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		tenant := strings.TrimPrefix(r.URL.Path, "/work/")
		rates := advertise
		el := 0.0
		if started.Load() {
			el = time.Since(start).Seconds()
			if el >= phase {
				rates = swapped
			}
			mu.Lock()
			arrivals[tenant] = append(arrivals[tenant], el)
			mu.Unlock()
		}
		w.Header().Set("X-Aqueduct-Account-Queue", "enabled")
		if rps, ok := rates[tenant]; ok {
			w.Header().Set("X-Aqueduct-Rps", fmt.Sprint(rps))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{
		Defaults:  RateConfig{RPS: 2, MaxConcurrent: 1},
		Upstreams: map[string]RateConfig{mustHost(t, upstream.URL): {RPS: configured, MaxConcurrent: 4}},
	}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)
	routes := NewServer(app).Routes()

	var seq atomic.Int64
	submit := func(tenant string) {
		job := sampleJobRequest(tenant, fmt.Sprintf("%s-%d", tenant, seq.Add(1)))
		job.URL = upstream.URL + "/work/" + tenant
		job.WebhookURL = ""
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated && rec.Code != http.StatusTooManyRequests {
			t.Errorf("%s: unexpected %d: %s", tenant, rec.Code, rec.Body.String())
		}
	}

	// Every tenant offers far more than the total (open loop), so all three
	// stay backlogged and the split is decided only by the shared pace.
	start = time.Now()
	started.Store(true)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for tenant := range advertise {
		wg.Add(1)
		go func(tenant string) {
			defer wg.Done()
			tick := time.NewTicker(5 * time.Millisecond) // 200/s each
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					submit(tenant)
				}
			}
		}(tenant)
	}
	time.Sleep(time.Duration(2 * phase * float64(time.Second)))
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	count := func(tenant string, from, to float64) int {
		n := 0
		for _, at := range arrivals[tenant] {
			if at >= from && at < to {
				n++
			}
		}
		return n
	}
	for i, rates := range []map[string]float64{advertise, swapped} {
		from, to := float64(i)*phase+settle, float64(i+1)*phase
		sum := 0.0
		for _, r := range rates {
			sum += r
		}
		got := map[string]int{}
		total := 0
		for tenant := range rates {
			got[tenant] = count(tenant, from, to)
			total += got[tenant]
		}
		rate := float64(total) / (to - from)
		t.Logf("phase %d: %.1f/s delivered, %v (advertised %v)", i+1, rate, got, rates)
		if rate > top*1.1 || rate < top*0.85 {
			t.Errorf("phase %d: target received %.1f/s; want about the %.0f rps it advertised", i+1, rate, top)
		}
		for tenant, r := range rates {
			want := float64(total) * r / sum
			if math.Abs(float64(got[tenant])-want) > math.Max(want*0.15, 4) {
				t.Errorf("phase %d: %s got %d of %d, want about %.0f (advertised %.1f of %.1f)", i+1, tenant, got[tenant], total, want, r, sum)
			}
		}
	}
}
