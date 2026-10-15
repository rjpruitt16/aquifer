// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Who decides whether a destination gets one queue per tenant or one shared
// queue: the destination itself (X-Aqueduct-Account-Queue on its
// responses), then the node owner (AQUIFER_ACCOUNT_QUEUE, on by default),
// then a sender, who can only ask for it on.

func okServer(t *testing.T, header string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if header != "" {
			w.Header().Set("X-Aqueduct-Account-Queue", header)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func localJob(userID, url string) *Job {
	return &Job{ID: generateID(), UserID: userID, URL: url, Method: "POST", Status: StatusQueued}
}

func workerFor(t *testing.T, r *Registry, url string) *URLWorker {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[domainKey(url)]
	if !ok {
		t.Fatalf("expected a worker for %s", url)
	}
	return w
}

func accountQueueMode(w *URLWorker) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accountQueueMode
}

func TestParseAccountQueueValue(t *testing.T) {
	for _, tc := range []struct {
		in     string
		on, ok bool
	}{
		{"enabled", true, true}, {"true", true, true}, {" ON ", true, true}, {"1", true, true},
		{"disabled", false, true}, {"false", false, true}, {"off", false, true}, {"0", false, true},
		{"", false, false}, {"maybe", false, false},
	} {
		on, ok := parseAccountQueueValue(tc.in)
		if on != tc.on || ok != tc.ok {
			t.Errorf("parseAccountQueueValue(%q) = %v, %v; want %v, %v", tc.in, on, ok, tc.on, tc.ok)
		}
	}
}

func TestAccountQueuesAreOnByDefault(t *testing.T) {
	r := testRegistry(t)
	srv := okServer(t, "")
	r.Enqueue(localJob("tenant-a", srv.URL), "")

	if !accountQueueMode(workerFor(t, r, srv.URL)) {
		t.Fatal("expected one queue per tenant with no header and no AQUIFER_ACCOUNT_QUEUE")
	}
}

// One sender switching isolation off would take it away from every other
// tenant sending to the same destination.
func TestASenderCannotTurnAccountQueuesOff(t *testing.T) {
	r := testRegistry(t)
	srv := okServer(t, "")
	r.Enqueue(localJob("tenant-a", srv.URL), "disabled")
	r.Enqueue(localJob("tenant-b", srv.URL), "false")

	if !accountQueueMode(workerFor(t, r, srv.URL)) {
		t.Fatal("a sender's header turned account queues off")
	}
}

// Inlet sends "true"; that has to mean on, not off.
func TestASenderCanTurnAccountQueuesOnWhenTheNodeDefaultIsOff(t *testing.T) {
	t.Setenv("AQUIFER_ACCOUNT_QUEUE", "off")
	r := testRegistry(t)
	srv := okServer(t, "")
	r.Enqueue(localJob("tenant-a", srv.URL), "")
	if accountQueueMode(workerFor(t, r, srv.URL)) {
		t.Fatal("expected a shared queue with AQUIFER_ACCOUNT_QUEUE=off")
	}

	r.Enqueue(localJob("tenant-a", srv.URL), "true")
	if !accountQueueMode(workerFor(t, r, srv.URL)) {
		t.Fatal("a sender's X-Aqueduct-Account-Queue: true didn't turn account queues on")
	}
}

func waitForMode(t *testing.T, r *Registry, url string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		w, ok := r.workers[domainKey(url)]
		r.mu.Unlock()
		if ok && accountQueueMode(w) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("account-queue mode for %s never became %v", url, want)
}

// The destination has the last say, over the node owner's default and over
// senders, and its choice outlives its worker being retired.
func TestTheDestinationHasTheLastSay(t *testing.T) {
	r := testRegistry(t)
	srv := okServer(t, "disabled")
	r.Enqueue(localJob("tenant-a", srv.URL), "")
	waitForMode(t, r, srv.URL, false)

	// A sender asking for isolation doesn't override the destination.
	r.Enqueue(localJob("tenant-b", srv.URL), "enabled")
	if accountQueueMode(workerFor(t, r, srv.URL)) {
		t.Fatal("a sender overrode the destination's choice")
	}

	// A fresh worker for the same destination starts from its choice.
	r.mu.Lock()
	delete(r.workers, domainKey(srv.URL))
	_, fresh := r.resolveWorkerLocked(localJob("tenant-c", srv.URL))
	r.mu.Unlock()
	if accountQueueMode(fresh) {
		t.Fatal("the destination's choice was forgotten when its worker was recreated")
	}
}

func TestTheDestinationCanTurnAccountQueuesOn(t *testing.T) {
	t.Setenv("AQUIFER_ACCOUNT_QUEUE", "off")
	r := testRegistry(t)
	srv := okServer(t, "enabled")
	r.Enqueue(localJob("tenant-a", srv.URL), "")
	waitForMode(t, r, srv.URL, true)
}

// The scenario from the question "where do retries go in the fair queue?":
// one tenant's endpoint is broken and bursting retries while another
// tenant sends steady traffic to the same host. The broken tenant's
// retries wait in its own queue's backoff, off the concurrency slots, so
// the healthy tenant's jobs still go straight through.
func TestABrokenTenantDoesNotStallAHealthyOneOnTheSameHost(t *testing.T) {
	withRetryBackoff(t, 200*time.Millisecond)
	var brokenHits, healthyDone atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/broken") {
			brokenHits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		healthyDone.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	hook := okServer(t, "")                               // webhookSink's buffer of 4 would block 60 deliveries
	app, _ := testAquiferWithLimits(t, AdmissionLimits{}) // max_concurrent 1 per queue

	for i := 0; i < 40; i++ {
		req := retryJob(fmt.Sprintf("broken-%d", i), srv.URL+"/broken", hook.URL, intPtr(20))
		req.UserID = "broken-tenant"
		if _, err := app.Enqueue(req); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	const healthy = 20
	for i := 0; i < healthy; i++ {
		req := retryJob(fmt.Sprintf("healthy-%d", i), srv.URL+"/ok", hook.URL, nil)
		req.UserID = "healthy-tenant"
		if _, err := app.Enqueue(req); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for healthyDone.Load() < healthy && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := healthyDone.Load(); got < healthy {
		t.Fatalf("healthy tenant finished %d/%d jobs in %s while the broken tenant retried", got, healthy, time.Since(start))
	}
	if brokenHits.Load() == 0 {
		t.Fatal("test setup: the broken tenant never ran")
	}
}

// The same with the failure on a different host: one sender, one broken
// destination and one healthy one. Each destination has its own queues.
func TestABrokenHostDoesNotStallAHealthyHost(t *testing.T) {
	withRetryBackoff(t, 200*time.Millisecond)
	var brokenHits, healthyDone atomic.Int64
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		brokenHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer broken.Close()
	healthySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		healthyDone.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer healthySrv.Close()
	hook := okServer(t, "") // webhookSink's buffer of 4 would block 60 deliveries
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	for i := 0; i < 40; i++ {
		if _, err := app.Enqueue(retryJob(fmt.Sprintf("broken-%d", i), broken.URL, hook.URL, intPtr(20))); err != nil {
			t.Fatal(err)
		}
	}
	const healthy = 20
	for i := 0; i < healthy; i++ {
		if _, err := app.Enqueue(retryJob(fmt.Sprintf("healthy-%d", i), healthySrv.URL, hook.URL, nil)); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for healthyDone.Load() < healthy && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := healthyDone.Load(); got < healthy {
		t.Fatalf("healthy host got %d/%d jobs while the broken host was retrying", got, healthy)
	}
	if brokenHits.Load() == 0 {
		t.Fatal("test setup: the broken host never ran")
	}
}

// A direct /proxy answer carries the destination's choice too, not only a
// queued dispatch's.
func TestTheDestinationIsHeardOnDirectProxyResponses(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	srv := NewServer(app)
	upstream := okServer(t, "disabled")

	body, _ := json.Marshal(proxyJobRequest("user-1", "proxy-key-1", upstream.URL))
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/proxy", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected the direct answer relayed, got %d: %s", rec.Code, rec.Body.String())
	}
	waitForMode(t, app.registry, upstream.URL, false)
}
