// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// deadTargetRun submits retry-until-done jobs to a target that only answers
// 503 and returns the webhooks received within wait.
func deadTargetRun(t *testing.T, wait time.Duration) []map[string]any {
	t.Helper()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	hooks := make(chan map[string]any, 16)
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["job_id"]; ok {
			hooks <- payload
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		dead.Close()
		hook.Close()
	})

	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	routes := NewServer(app).Routes()
	for i := 0; i < 3; i++ {
		retries := -1
		job := sampleJobRequest("tenant", fmt.Sprintf("dead-%d", i))
		job.URL = dead.URL + "/work"
		job.WebhookURL = hook.URL
		job.MaxRetries = &retries
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("job %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}

	var got []map[string]any
	deadline := time.After(wait)
	for {
		select {
		case payload := <-hooks:
			got = append(got, payload)
			if len(got) == 3 {
				return got
			}
		case <-deadline:
			return got
		}
	}
}

func TestDeadTargetFailsWaitingJobsWhenEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the target to count as dead; skipped with -short")
	}
	t.Setenv("AQUIFER_DEAD_TARGET_FAIL_AFTER_SECONDS", "1")
	got := deadTargetRun(t, 8*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d webhooks, want all 3 jobs failed for a dead target: %v", len(got), got)
	}
	for _, payload := range got {
		reason, _ := payload["reason"].(string)
		if payload["status"] != "failed" || !strings.HasPrefix(reason, "target_unavailable") {
			t.Fatalf("webhook = %v, want failed with reason target_unavailable", payload)
		}
	}
}

func TestDeadTargetKeepsJobsWaitingByDefault(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test; skipped with -short")
	}
	if got := deadTargetRun(t, 4*time.Second); len(got) != 0 {
		t.Fatalf("with the setting off, retry-until-done jobs must keep waiting; got webhooks %v", got)
	}
}
