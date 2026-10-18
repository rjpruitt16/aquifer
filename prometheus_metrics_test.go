// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func render(m *PrometheusMetrics, targets []TargetStats) string {
	var b strings.Builder
	m.Write(&b, "node-a", targets)
	return b.String()
}

func TestMetricsCountJobsAttemptsAndRejectionsWithoutTenantLabels(t *testing.T) {
	m := newPrometheusMetrics(false, maxTenantLabels)
	m.JobCompleted("alice", "api.example.com", 12)
	m.JobCompleted("bob", "api.example.com", 12)
	m.JobFailed("alice", "api.example.com", "upstream returned 503")
	m.JobFailed("alice", "api.example.com", "execution_deadline_exceeded")
	m.DispatchAttempt("api.example.com", 200)
	m.DispatchAttempt("api.example.com", 429)
	m.DispatchAttempt("api.example.com", 503)
	m.DispatchAttempt("api.example.com", 0)
	m.AdmissionRejected("queue_fair_share")
	m.JobWaited("alice", "api.example.com", 300*time.Millisecond)

	out := render(m, nil)
	for _, want := range []string{
		`aquifer_node_info{node_id="node-a"} 1`,
		`aquifer_jobs_total{target="api.example.com",outcome="completed"} 2`,
		`aquifer_jobs_total{target="api.example.com",outcome="failed"} 1`,
		`aquifer_jobs_total{target="api.example.com",outcome="expired"} 1`,
		`aquifer_dispatch_attempts_total{target="api.example.com",status_class="2xx"} 1`,
		`aquifer_dispatch_attempts_total{target="api.example.com",status_class="429"} 1`,
		`aquifer_dispatch_attempts_total{target="api.example.com",status_class="5xx"} 1`,
		`aquifer_dispatch_attempts_total{target="api.example.com",status_class="error"} 1`,
		`aquifer_admission_rejected_total{reason="queue_fair_share"} 1`,
		`aquifer_queue_wait_seconds_bucket{target="api.example.com",le="0.25"} 0`,
		`aquifer_queue_wait_seconds_bucket{target="api.example.com",le="0.5"} 1`,
		`aquifer_queue_wait_seconds_bucket{target="api.example.com",le="+Inf"} 1`,
		`aquifer_queue_wait_seconds_count{target="api.example.com"} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "alice") || strings.Contains(out, "tenant=") {
		t.Errorf("tenant IDs must stay out of /metrics unless tenant labels are on:\n%s", out)
	}
}

func TestMetricsEachFamilyIsContiguous(t *testing.T) {
	m := newPrometheusMetrics(true, maxTenantLabels)
	m.JobCompleted("alice", "a.example.com", 1)
	m.JobCompleted("bob", "b.example.com", 1)
	m.JobFinished("alice", "a.example.com", "completed", time.Second)
	// Prometheus rejects a family whose lines are split up by another one.
	seen := map[string]bool{}
	last := ""
	for _, line := range strings.Split(strings.TrimSpace(render(m, nil)), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		name := line[:strings.IndexAny(line, "{ ")]
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			name = strings.TrimSuffix(name, suffix)
		}
		if name != last && seen[name] {
			t.Fatalf("%s appears in two separate places", name)
		}
		seen[name], last = true, name
	}
}

func TestMetricsTenantLabelsAreOptInAndCapped(t *testing.T) {
	m := newPrometheusMetrics(true, 2)
	for _, user := range []string{"alice", "bob", "carol", "dave"} {
		m.JobCompleted(user, "api.example.com", 1)
	}
	out := render(m, nil)
	for _, want := range []string{
		`aquifer_tenant_jobs_total{target="api.example.com",outcome="completed",tenant="alice"} 1`,
		`aquifer_tenant_jobs_total{target="api.example.com",outcome="completed",tenant="bob"} 1`,
		`aquifer_tenant_jobs_total{target="api.example.com",outcome="completed",tenant="other"} 2`,
		`aquifer_jobs_total{target="api.example.com",outcome="completed"} 4`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "carol") {
		t.Errorf("tenants past the cap should be counted as other:\n%s", out)
	}
}

func TestMetricsLabelValuesAreEscaped(t *testing.T) {
	if got := quote("a\"b\\c\nd"); got != `"a\"b\\c\nd"` {
		t.Fatalf("quote = %s", got)
	}
}

func queues(served map[string]int64, backlog map[string]int64) TargetStats {
	stats := TargetStats{Target: "api.example.com"}
	for key, s := range served {
		stats.Queues = append(stats.Queues, QueueStat{Key: key, Served: s, Backlog: backlog[key]})
	}
	return stats
}

func TestFairnessIsOneWhenWaitingQueuesAreServedEvenly(t *testing.T) {
	m := newPrometheusMetrics(false, maxTenantLabels)
	start := time.Now()
	busy := map[string]int64{"q1": 5, "q2": 5}
	if _, ok := m.fairnessLocked(queues(map[string]int64{"q1": 0, "q2": 0}, busy), start); ok {
		t.Fatal("no fairness without an earlier sample to compare with")
	}
	index, ok := m.fairnessLocked(queues(map[string]int64{"q1": 30, "q2": 30}, busy), start.Add(time.Minute))
	if !ok || index != 1 {
		t.Fatalf("even service should be 1, got %v (ok=%v)", index, ok)
	}
}

func TestFairnessDropsWhenOneQueueIsStarved(t *testing.T) {
	m := newPrometheusMetrics(false, maxTenantLabels)
	start := time.Now()
	busy := map[string]int64{"q1": 5, "q2": 5}
	m.fairnessLocked(queues(map[string]int64{"q1": 0, "q2": 0}, busy), start)
	index, ok := m.fairnessLocked(queues(map[string]int64{"q1": 60, "q2": 0}, busy), start.Add(time.Minute))
	// (60)^2 / (2 * 60^2) = 0.5: all the service went to one of two.
	if !ok || math.Abs(index-0.5) > 1e-9 {
		t.Fatalf("one of two starved should be 0.5, got %v", index)
	}
}

func TestFairnessIgnoresIdleQueuesAndNewcomers(t *testing.T) {
	m := newPrometheusMetrics(false, maxTenantLabels)
	start := time.Now()
	m.fairnessLocked(queues(map[string]int64{"q1": 0, "q2": 0, "idle": 0}, map[string]int64{"q1": 1, "q2": 1}), start)
	// idle has nothing waiting, and new arrived within the window: neither
	// should drag the index down.
	index, ok := m.fairnessLocked(queues(
		map[string]int64{"q1": 20, "q2": 20, "idle": 0, "new": 1},
		map[string]int64{"q1": 1, "q2": 1, "new": 9},
	), start.Add(time.Minute))
	if !ok || index != 1 {
		t.Fatalf("got %v, want 1", index)
	}
}

func TestFairnessComparesAgainstAboutAMinuteAgo(t *testing.T) {
	m := newPrometheusMetrics(false, maxTenantLabels)
	start := time.Now()
	busy := map[string]int64{"q1": 1, "q2": 1}
	// Uneven long ago, even in the last minute: the gauge should say even.
	m.fairnessLocked(queues(map[string]int64{"q1": 0, "q2": 0}, busy), start)
	m.fairnessLocked(queues(map[string]int64{"q1": 100, "q2": 0}, busy), start.Add(time.Minute))
	index, _ := m.fairnessLocked(queues(map[string]int64{"q1": 110, "q2": 10}, busy), start.Add(2*time.Minute))
	if index != 1 {
		t.Fatalf("got %v, want 1 (only the last minute counts)", index)
	}
	if n := len(m.samples["api.example.com"]); n > 2 {
		t.Fatalf("old samples should be dropped, %d kept", n)
	}
}

func TestNodeIDPrefersExplicitThenClusterThenFlyThenHostname(t *testing.T) {
	env := map[string]string{}
	getenv := func(k string) string { return env[k] }
	host := func() (string, error) { return "box-1", nil }
	if got := resolveNodeID(getenv, host); got != "box-1" {
		t.Fatalf("got %q, want the hostname", got)
	}
	env["FLY_MACHINE_ID"] = "fly-1"
	if got := resolveNodeID(getenv, host); got != "fly-1" {
		t.Fatalf("got %q", got)
	}
	env["AQUIFER_CLUSTER_SELF_ID"] = "a"
	if got := resolveNodeID(getenv, host); got != "a" {
		t.Fatalf("got %q", got)
	}
	env["AQUIFER_NODE_ID"] = "explicit"
	if got := resolveNodeID(getenv, host); got != "explicit" {
		t.Fatalf("got %q", got)
	}
	if got := resolveNodeID(func(string) string { return "" }, func() (string, error) { return "", errors.New("no") }); got != "unknown" {
		t.Fatalf("got %q", got)
	}
}

// Through a real server: jobs dispatched to a real backend show up on
// /metrics, and the backend sees the node's id.
func TestMetricsEndpointThroughARealServer(t *testing.T) {
	var mu sync.Mutex
	var nodeIDs []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		nodeIDs = append(nodeIDs, r.Header.Get("X-Aqueduct-Node-Id"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	metrics := newPrometheusMetrics(false, maxTenantLabels)
	registry := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 4}}, broker, l8, metrics, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, dbPath), nil)
	t.Cleanup(app.Close)
	srv := NewServer(app).Routes()

	for i, user := range []string{"alice", "bob", "alice"} {
		body, _ := json.Marshal(JobRequest{UserID: user, IdempotentKey: "k" + string(rune('0'+i)), URL: backend.URL + "/work", Method: "POST"})
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
		if strings.Contains(out, `outcome="completed"} 3`) {
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
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(nodeIDs) != 3 || nodeIDs[0] == "" || nodeIDs[0] != NodeID() {
		t.Fatalf("backend should see this node's id on every dispatch, got %v", nodeIDs)
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
