// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookBacklogRejectsOnlyWhenTheInstanceIsShared(t *testing.T) {
	t.Setenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER", "1")
	release := make(chan struct{})

	// A receiver that never answers, so "busy"'s webhooks pile up undelivered.
	stuckHook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		<-release
	}))
	t.Cleanup(stuckHook.Close)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		if r.URL.Path == "/slow" {
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)

	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	// Registered last so it runs first: the servers and app above can only
	// close once their blocked requests are released.
	t.Cleanup(func() { close(release) })
	handler := NewServer(app).Routes()
	submit := func(user, key, path string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(JobRequest{UserID: user, IdempotentKey: key, URL: upstream.URL + path, Method: "POST", WebhookURL: stuckHook.URL})
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		return rec
	}

	for i, key := range []string{"b1", "b2", "b3"} {
		if rec := submit("busy", key, "/ok"); rec.Code != http.StatusCreated {
			t.Fatalf("job %d: expected 201 while alone, got %d: %s", i, rec.Code, rec.Body.String())
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jobs, webhooks := app.registry.UserLoad("busy")
		pending := webhooks >= 2 && jobs == 0
		if pending {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if rec := submit("busy", "b4", "/ok"); rec.Code != http.StatusCreated {
		t.Fatalf("alone on the instance, a webhook backlog must not cause a 429, got %d: %s", rec.Code, rec.Body.String())
	}

	if rec := submit("neighbor", "n1", "/slow"); rec.Code != http.StatusCreated {
		t.Fatalf("neighbor's job should be accepted, got %d", rec.Code)
	}
	rec := submit("busy", "b5", "/ok")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("with another user active, busy's webhook backlog must cause a 429, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["limit_reason"] != "webhook_backlog" {
		t.Fatalf("expected limit_reason webhook_backlog, got %v", resp)
	}
}
