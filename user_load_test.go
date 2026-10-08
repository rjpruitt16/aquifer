package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func webhookJobFor(userID string) *Job {
	return &Job{ID: generateID(), UserID: userID, WebhookURL: ""}
}

func userJob(userID string) *Job {
	return &Job{ID: generateID(), UserID: userID, WebhookURL: "https://example.com/hook"}
}

func TestWebhookBacklogDecision(t *testing.T) {
	t.Setenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER", "1000")
	tracker := newUserLoadTracker()
	for i := 0; i < 1500; i++ {
		tracker.add(webhookJobFor("busy"))
	}

	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("a user alone on the instance must never be rejected for webhook backlog")
	}

	tracker.add(userJob("neighbor"))
	if rejected, limit, backlog := tracker.webhookBacklogDecision("busy", 0.4); !rejected || limit != 1000 || backlog != 1500 {
		t.Fatalf("1500/1000 gives p=0.5, so a 0.4 draw must reject (got rejected=%v limit=%d backlog=%d)", rejected, limit, backlog)
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0.6); rejected {
		t.Fatal("1500/1000 gives p=0.5, so a 0.6 draw must be admitted")
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("neighbor", 0); rejected {
		t.Fatal("a user with no webhook backlog must never be rejected")
	}

	for i := 0; i < 600; i++ {
		tracker.done(webhookJobFor("busy"))
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("at or under the limit, the user is admitted again")
	}

	t.Setenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER", "0")
	for i := 0; i < 5000; i++ {
		tracker.add(webhookJobFor("busy"))
	}
	if rejected, _, _ := tracker.webhookBacklogDecision("busy", 0); rejected {
		t.Fatal("a limit of 0 disables the rule")
	}
}

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
		app.registry.users.mu.Lock()
		load := app.registry.users.users["busy"]
		pending := load != nil && load.webhooks >= 2 && load.jobs == 0
		app.registry.users.mu.Unlock()
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
