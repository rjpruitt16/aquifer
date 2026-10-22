// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// Through a real server: jobs dispatched to a real backend show up on
// /metrics, and the backend sees the node's id.
func TestMetricsEndpointThroughARealServer(t *testing.T) {
	var mu sync.Mutex
	var nodeIDs []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		nodeIDs = append(nodeIDs, r.URL.Path+"="+r.Header.Get("X-Aqueduct-Node-Id"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	metrics := NewPrometheusMetrics()
	registry := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 4}}, broker, l8, metrics, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)
	srv := NewServer(app).Routes()

	for i, user := range []string{"alice", "bob", "alice"} {
		body, _ := json.Marshal(JobRequest{UserID: user, IdempotentKey: "k" + string(rune('0'+i)), URL: backend.URL + "/work", Method: "POST", WebhookURL: backend.URL + "/hook"})
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("submit: %d %s", rec.Code, rec.Body.String())
		}
	}

	var out string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("/metrics: %d", rec.Code)
		}
		body, _ := io.ReadAll(rec.Body)
		out = string(body)
		if strings.Contains(out, `aquifer_webhooks_total{outcome="delivered"} 3`) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	target := domainKey(backend.URL)
	for _, want := range []string{
		`aquifer_jobs_total{target="` + target + `",outcome="completed"} 3`,
		`aquifer_dispatch_attempts_total{target="` + target + `",status_class="2xx"} 3`,
		`aquifer_queue_wait_seconds_count{target="` + target + `"} 3`,
		`aquifer_job_duration_seconds_count{target="` + target + `",outcome="completed"} 3`,
		`aquifer_active_queues{target="` + target + `"} 0`,
		`aquifer_node_info{node_id="` + NodeID() + `"} 1`,
		// The three completion webhooks go to the same host, as internal
		// jobs of Aquifer's own: they're webhooks, not jobs or attempts.
		`aquifer_webhooks_total{outcome="delivered"} 3`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// Every job and webhook carries the node's id. (Aquifer's own L8
	// discovery fetch of /.well-known/l8 isn't a dispatch.)
	dispatched := 0
	for _, seen := range nodeIDs {
		path, id, _ := strings.Cut(seen, "=")
		if path != "/work" && path != "/hook" {
			continue
		}
		dispatched++
		if id != NodeID() {
			t.Fatalf("%s arrived with node id %q, want %q", path, id, NodeID())
		}
	}
	if dispatched != 6 {
		t.Fatalf("want 3 jobs and 3 webhooks, saw %v", nodeIDs)
	}
}

func TestMetricsIs404WithACustomAdapter(t *testing.T) {
	srv := testServer(t, AdmissionLimits{}) // NoopMetricsAdapter
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rec.Code)
	}
}

// A receiver that turns a webhook down (401) ends its delivery job without
// a retry, and that's a failed webhook, not a delivered one.
func TestARejectedWebhookIsNotDelivered(t *testing.T) {
	m := NewPrometheusMetrics()
	hook := &Job{Kind: jobs.KindWebhookDelivery}
	observeJobFinished(m, hook, "https://app.example", "completed", 200)
	observeJobFinished(m, hook, "https://app.example", "completed", 401)
	observeJobFinished(m, hook, "https://app.example", "failed", 503)
	var b strings.Builder
	m.Write(&b, "node-a", nil)
	out := b.String()
	for _, want := range []string{
		`aquifer_webhooks_total{outcome="delivered"} 1`,
		`aquifer_webhooks_total{outcome="failed"} 2`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "aquifer_jobs_total{") {
		t.Errorf("webhook jobs aren't jobs:\n%s", out)
	}
}
