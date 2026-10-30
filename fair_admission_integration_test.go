// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestFairAdmissionKeepsSmallTenantsInWhenOneFillsTheBudget reproduces the
// Hatchet pilot's lockout through the HTTP API. The target API sets a
// backlog budget of 10 with X-Aqueduct-Max-Backlog on its first response
// and then stops answering, so jobs pile up. One tenant fills the budget
// until it is refused; small tenants must still get in, up to the 1.25x
// ceiling, past which everyone is refused.
func TestFairAdmissionKeepsSmallTenantsInWhenOneFillsTheBudget(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if hits.Add(1) == 1 {
			w.Header().Set("X-Aqueduct-Max-Backlog", "10")
			w.WriteHeader(http.StatusOK)
			return
		}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	defer close(release)

	routes := testServer(t, AdmissionLimits{}).Routes()
	submit := func(user, key string) *httptest.ResponseRecorder {
		job := sampleJobRequest(user, key)
		job.URL = upstream.URL + "/work"
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		return rec
	}

	// The first job's response teaches Aquifer the budget of 10. The second
	// is dispatched after that response is handled, then hangs.
	for i := 0; i < 2; i++ {
		if rec := submit("big", fmt.Sprintf("big-learn-%d", i)); rec.Code != http.StatusCreated {
			t.Fatalf("job %d: expected 201, got %d: %s", i, rec.Code, rec.Body.String())
		}
	}
	if !waitUntil(5*time.Second, func() bool { return hits.Load() >= 2 }) {
		t.Fatal("the target API never received the queued jobs")
	}

	// The big tenant fills the budget on its own and is refused.
	refused := false
	for i := 1; i <= 20 && !refused; i++ {
		rec := submit("big", fmt.Sprintf("big-%d", i))
		switch rec.Code {
		case http.StatusCreated:
		case http.StatusTooManyRequests:
			refused = true
		default:
			t.Fatalf("big job %d: unexpected %d: %s", i, rec.Code, rec.Body.String())
		}
	}
	if !refused {
		t.Fatal("the big tenant was never refused; the learned budget of 10 did not apply")
	}

	// A small tenant with nothing queued still enters past the budget.
	rec := submit("small-1", "small-1-0")
	if rec.Code != http.StatusCreated {
		t.Fatalf("small tenant refused after the big tenant filled the budget: %d %s", rec.Code, rec.Body.String())
	}
	if rec := submit("big", "big-again"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("big tenant should stay refused past the budget, got %d", rec.Code)
	}

	// More small tenants keep entering until the backlog reaches the
	// ceiling (12.5 for a budget of 10); then everyone is refused.
	for i := 2; i <= 20; i++ {
		rec := submit(fmt.Sprintf("small-%d", i), "only")
		if rec.Code == http.StatusCreated {
			continue
		}
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("small-%d: unexpected %d: %s", i, rec.Code, rec.Body.String())
		}
		backlog, err := strconv.Atoi(rec.Header().Get("X-Aqueduct-Upstream-Backlog"))
		if err != nil {
			t.Fatalf("small-%d: refusal without a valid X-Aqueduct-Upstream-Backlog: %v", i, err)
		}
		if backlog > 13 {
			t.Fatalf("refused only at backlog %d; the ceiling should stop it at 13", backlog)
		}
		if i == 2 {
			t.Fatal("the ceiling refused the second small tenant; it should allow some room past the budget")
		}
		return
	}
	t.Fatal("small tenants were never refused; the ceiling did not apply")
}

// TestFairAdmissionKeepsSmallTenantsInWhenTheBudgetShrinks reproduces the
// Hatchet outage benchmark's lockout. The node's own budget is 20. One tenant
// queues 15 jobs while the target API hangs, then the target API answers with
// X-Aqueduct-Max-Backlog: 4, so the backlog already sits past 1.25x the new
// budget. A small tenant must still get in; the big one stays refused.
func TestFairAdmissionKeepsSmallTenantsInWhenTheBudgetShrinks(t *testing.T) {
	t.Setenv("AQUIFER_MAX_PENDING_PER_UPSTREAM", "20")
	first := make(chan struct{})
	release := make(chan struct{})
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if hits.Add(1) == 1 {
			<-first
			w.Header().Set("X-Aqueduct-Max-Backlog", "4")
			w.WriteHeader(http.StatusOK)
			return
		}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	defer close(release)

	routes := testServer(t, AdmissionLimits{}).Routes()
	submit := func(user, key string) *httptest.ResponseRecorder {
		job := sampleJobRequest(user, key)
		job.URL = upstream.URL + "/work"
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		return rec
	}

	for i := 0; i < 15; i++ {
		if rec := submit("big", fmt.Sprintf("big-%d", i)); rec.Code != http.StatusCreated {
			t.Fatalf("big job %d under the node's budget: expected 201, got %d: %s", i, rec.Code, rec.Body.String())
		}
	}
	// The first answer shrinks the budget to 4; the next dispatch hangs.
	close(first)
	if !waitUntil(5*time.Second, func() bool { return hits.Load() >= 2 }) {
		t.Fatal("the target API never received the second job")
	}

	if rec := submit("big", "big-again"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("big tenant should be refused past the shrunken budget, got %d", rec.Code)
	}
	if rec := submit("small-1", "small-1-0"); rec.Code != http.StatusCreated {
		t.Fatalf("small tenant refused after the target API shrank its budget: %d %s", rec.Code, rec.Body.String())
	}
}
