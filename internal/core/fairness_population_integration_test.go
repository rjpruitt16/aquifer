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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// populationTenant describes one tenant's offered load over the test: a rate
// in jobs per second, active only while on(t) is true for t seconds since
// the start.
type populationTenant struct {
	name  string
	class string // "heavy", "medium" or "small"
	rate  float64
	on    func(t float64) bool
}

// TestFairnessAcrossAChangingPopulation checks fairness the way a shared
// target sees it: many tenants with very different demand, whose rates change
// while the test runs, through POST /jobs against one target with an rps
// ceiling.
//
// The population (ceiling 60 rps, max_concurrent 4 per queue):
//   - 3 heavy tenants offering 60/s each. One stays on throughout, one joins
//     for the middle of the run, one switches on and off every 3 s.
//   - 7 medium tenants offering 3/s each.
//   - 30 small tenants offering 0.5/s each. 10 of them join at 8 s, and 10
//     others go quiet at 14 s.
//
// The light tenants (medium and small) ask for far less than a fair share, so
// max-min fairness says they get everything they ask for, promptly, and the
// heavy tenants split what's left. The test checks, per 2 s window after a
// warm-up: the target's total stays within the ceiling and capacity isn't
// wasted while anyone is backlogged; light tenants' jobs arrive within 2 s;
// every backlogged tenant gets served; and the heavy tenants that are on
// share evenly.
func TestFairnessAcrossAChangingPopulation(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test (about 25 s); skipped with -short")
	}
	const (
		ceiling  = 60.0
		duration = 20.0 // seconds of offered load
		window   = 2.0  // seconds per measurement window
		warmup   = 2.0
	)

	tenants := []populationTenant{
		{name: "heavy-steady", class: "heavy", rate: 60, on: func(float64) bool { return true }},
		{name: "heavy-middle", class: "heavy", rate: 60, on: func(t float64) bool { return t >= 6 && t < 14 }},
		{name: "heavy-bursty", class: "heavy", rate: 60, on: func(t float64) bool { return int(t/3)%2 == 0 }},
	}
	for i := 0; i < 7; i++ {
		tenants = append(tenants, populationTenant{name: fmt.Sprintf("medium-%d", i), class: "medium", rate: 3, on: func(float64) bool { return true }})
	}
	for i := 0; i < 30; i++ {
		on := func(float64) bool { return true }
		switch {
		case i < 10:
			on = func(t float64) bool { return t >= 8 } // join late
		case i < 20:
			on = func(t float64) bool { return t < 14 } // go quiet
		}
		tenants = append(tenants, populationTenant{name: fmt.Sprintf("small-%d", i), class: "small", rate: 0.5, on: on})
	}
	class := map[string]string{}
	for _, tn := range tenants {
		class[tn.name] = tn.class
	}

	// The target records when each job arrives. Each job carries its tenant
	// in the path and its submit time in a header.
	type arrival struct {
		tenant    string
		at, delay float64 // seconds since start; seconds from submit to arrival
	}
	var mu sync.Mutex
	var arrivals []arrival
	var start time.Time
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		now := time.Since(start).Seconds()
		sent, _ := strconv.ParseFloat(r.Header.Get("X-Test-Submitted"), 64)
		mu.Lock()
		arrivals = append(arrivals, arrival{tenant: strings.TrimPrefix(r.URL.Path, "/work/"), at: now, delay: now - sent})
		mu.Unlock()
		w.Header().Set("X-Aqueduct-Account-Queue", "enabled")
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
		Upstreams: map[string]RateConfig{mustHost(t, upstream.URL): {RPS: ceiling, MaxConcurrent: 4}},
	}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)
	routes := NewServer(app).Routes()

	var refused atomic.Int64
	var seq atomic.Int64
	submit := func(tenant string) {
		submitted := time.Since(start).Seconds()
		job := sampleJobRequest(tenant, fmt.Sprintf("%s-%d", tenant, seq.Add(1)))
		job.URL = upstream.URL + "/work/" + tenant
		job.WebhookURL = ""
		job.Headers = map[string]string{"X-Test-Submitted": strconv.FormatFloat(submitted, 'f', 6, 64)}
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		switch rec.Code {
		case http.StatusCreated:
		case http.StatusTooManyRequests:
			refused.Add(1)
		default:
			t.Errorf("%s: unexpected %d: %s", tenant, rec.Code, rec.Body.String())
		}
	}

	// Learn the target's account-queue mode before the population starts.
	start = time.Now()
	submit("warm")
	if !waitUntil(5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(arrivals) > 0 }) {
		t.Fatal("the target never received the first job")
	}
	mu.Lock()
	arrivals = nil
	mu.Unlock()

	// Each tenant submits on its own schedule (open loop: it doesn't wait for
	// results), only while it's on.
	start = time.Now()
	var wg sync.WaitGroup
	for _, tn := range tenants {
		wg.Add(1)
		go func(tn populationTenant) {
			defer wg.Done()
			interval := time.Duration(float64(time.Second) / tn.rate)
			next := start.Add(time.Duration(float64(interval) * float64(len(tn.name)%7) / 7)) // stagger
			for {
				now := time.Now()
				el := now.Sub(start).Seconds()
				if el >= duration {
					return
				}
				if now.Before(next) {
					time.Sleep(next.Sub(now))
					continue
				}
				if tn.on(el) {
					submit(tn.name)
				}
				next = next.Add(interval)
			}
		}(tn)
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond) // let in-flight requests land

	mu.Lock()
	defer mu.Unlock()

	// Per-window counts.
	nWindows := int(duration / window)
	perWindow := make([]map[string]int, nWindows)
	for i := range perWindow {
		perWindow[i] = map[string]int{}
	}
	var lightDelays []float64
	lightLate := 0
	for _, a := range arrivals {
		if w := int(a.at / window); w >= 0 && w < nWindows {
			perWindow[w][a.tenant]++
		}
		if c := class[a.tenant]; c != "heavy" && a.at >= warmup {
			lightDelays = append(lightDelays, a.delay)
			if a.delay > 2 {
				lightLate++
			}
		}
	}

	heavyOn := func(w int, name string) bool {
		for _, tn := range tenants {
			if tn.name == name {
				// on for the whole window
				return tn.on(float64(w)*window) && tn.on(float64(w)*window+window-0.01)
			}
		}
		return false
	}
	for w := int(warmup / window); w < nWindows; w++ {
		total := 0
		for _, n := range perWindow[w] {
			total += n
		}
		rate := float64(total) / window
		if rate > ceiling*1.15 {
			t.Errorf("window %d: target received %.1f/s, over the %.0f rps ceiling", w, rate, ceiling)
		}
		// Heavy tenants that are on for the whole window are backlogged, so
		// capacity should be used and split evenly between them.
		var on []string
		for _, tn := range tenants {
			if tn.class == "heavy" && heavyOn(w, tn.name) {
				on = append(on, tn.name)
			}
		}
		if len(on) == 0 {
			continue
		}
		if rate < ceiling*0.8 {
			t.Errorf("window %d: only %.1f/s delivered while %v were backlogged", w, rate, on)
		}
		if len(on) >= 2 {
			var shares []float64
			for _, name := range on {
				shares = append(shares, float64(perWindow[w][name]))
			}
			lo, hi := minMax(shares)
			if lo == 0 || hi/lo > 1.6 {
				t.Errorf("window %d: heavy tenants %v got %v; backlogged tenants should share evenly", w, on, shares)
			}
		}
	}

	sort.Float64s(lightDelays)
	p99 := 0.0
	if len(lightDelays) > 0 {
		p99 = lightDelays[int(math.Min(float64(len(lightDelays)-1), 0.99*float64(len(lightDelays))))]
	}
	t.Logf("arrivals %d, light-tenant jobs %d, light p99 %.2fs, light late (>2s) %d, refused at intake %d",
		len(arrivals), len(lightDelays), p99, lightLate, refused.Load())
	for w := int(warmup / window); w < nWindows; w++ {
		heavy := map[string]int{}
		light := 0
		for name, n := range perWindow[w] {
			if class[name] == "heavy" {
				heavy[name] = n
			} else {
				light += n
			}
		}
		t.Logf("window %2d (%2.0f-%2.0fs): heavy %v, light total %d", w, float64(w)*window, float64(w+1)*window, heavy, light)
	}
	if len(lightDelays) < 400 {
		t.Fatalf("only %d light-tenant jobs arrived; expected about %d", len(lightDelays), int((duration-warmup)*(7*3+20*0.5)))
	}
	if p99 > 2 {
		t.Errorf("light tenants' p99 was %.2fs; tenants under their fair share should not wait behind heavy ones", p99)
	}
	if late := float64(lightLate) / float64(len(lightDelays)); late > 0.02 {
		t.Errorf("%.1f%% of light-tenant jobs took over 2s", late*100)
	}
}

func minMax(v []float64) (float64, float64) {
	lo, hi := v[0], v[0]
	for _, x := range v[1:] {
		lo, hi = math.Min(lo, x), math.Max(hi, x)
	}
	return lo, hi
}
