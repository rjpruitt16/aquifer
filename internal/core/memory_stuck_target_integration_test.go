// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Over the memory limit, a target whose backlog isn't draining is refused
// while a healthy target keeps getting jobs. Before, one dead target's
// backlog filled memory and every target was refused (issue #87).
func TestMemoryPressureRefusesOnlyTheStuckTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for two drain samples; skipped with -short")
	}
	release := make(chan struct{})
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		<-release // never answers during the test
	}))
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		close(release)
		dead.Close()
		healthy.Close()
	})

	// Always over the 1 MB limit, never over the hard limit: every decision
	// is the soft one.
	app, _ := testAquiferWithLimits(t, AdmissionLimits{MemoryLimitMB: 1, MemoryHardLimitPercent: 1_000_000_000})
	routes := NewServer(app).Routes()
	submit := func(target, key string) int {
		job := sampleJobRequest("tenant", key)
		job.URL = target + "/work"
		job.WebhookURL = ""
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		return rec.Code
	}

	// A target not yet measured isn't stuck, so its first jobs get in.
	for i := 0; i < 20; i++ {
		if code := submit(dead.URL, fmt.Sprintf("dead-%d", i)); code != http.StatusCreated {
			t.Fatalf("dead target job %d before any measurement: got %d, want 201", i, code)
		}
	}
	// Two 3 s samples: nothing finished, so its backlog isn't draining.
	time.Sleep(7 * time.Second)

	if code := submit(dead.URL, "dead-after"); code != http.StatusTooManyRequests {
		t.Fatalf("stuck target under memory pressure: got %d, want 429", code)
	}
	if code := submit(healthy.URL, "healthy-after"); code != http.StatusCreated {
		t.Fatalf("healthy target under memory pressure: got %d, want 201", code)
	}
}
