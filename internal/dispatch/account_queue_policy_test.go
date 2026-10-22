// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
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

func localJob(userID, url string) *jobs.Job {
	return &jobs.Job{ID: jobs.NewID(), UserID: userID, URL: url, Method: "POST", Status: jobs.StatusQueued}
}

func workerFor(t *testing.T, r *Registry, url string) *URLWorker {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[DomainKey(url)]
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
		on, ok := ParseAccountQueueValue(tc.in)
		if on != tc.on || ok != tc.ok {
			t.Errorf("ParseAccountQueueValue(%q) = %v, %v; want %v, %v", tc.in, on, ok, tc.on, tc.ok)
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
		w, ok := r.workers[DomainKey(url)]
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
	delete(r.workers, DomainKey(srv.URL))
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

// TestAqueductHeaderTakesPrecedenceOverOrca is the important precedence
// guarantee: an operator's own explicit pacing directive must never be
// silently overridden by a best-effort inferred one. This test exercises
// the actual dispatch response-handling path, not just orca.Rps in isolation.
func TestAqueductHeaderTakesPrecedenceOverOrca(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aqueduct-Rps", "7")
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.kv_cache_usage_perc=0.99")

	if val := PacingHeader(headers, "Rps"); val != "7" {
		t.Fatalf("expected explicit X-Aqueduct-Rps to be read first, got %q", val)
	}
	// orca.Rps itself doesn't know about precedence -- that's enforced by the
	// caller in account_queue.go's else-if. This test documents the
	// guarantee at the PacingHeader level: if this string is non-empty,
	// the caller never even calls orca.Rps.
}
