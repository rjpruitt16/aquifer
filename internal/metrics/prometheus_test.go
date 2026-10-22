// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func render(m *Prometheus, targets []TargetStats) string {
	var b strings.Builder
	m.Write(&b, "node-a", targets)
	return b.String()
}

func TestMetricsCountJobsAttemptsAndRejectionsWithoutTenantLabels(t *testing.T) {
	m := newPrometheus(false, maxTenantLabels)
	m.JobFinished("alice", "api.example.com", "completed", time.Second)
	m.JobFinished("bob", "api.example.com", "completed", time.Second)
	m.JobFinished("alice", "api.example.com", "failed", time.Second)
	m.JobFinished("alice", "api.example.com", "expired", time.Second)
	// These fire for internal webhook jobs and registration pings too, so
	// they mustn't move the job or webhook counts.
	m.JobCompleted("alice", "api.example.com", 12)
	m.WebhookDelivered("http://registry/register", 1)
	m.WebhookJobFinished("delivered")
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
		`aquifer_webhooks_total{outcome="delivered"} 1`,
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
	m := newPrometheus(true, maxTenantLabels)
	m.JobFinished("alice", "a.example.com", "completed", time.Second)
	m.JobFinished("bob", "b.example.com", "completed", time.Second)
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
	m := newPrometheus(true, 2)
	for _, user := range []string{"alice", "bob", "carol", "dave"} {
		m.JobFinished(user, "api.example.com", "completed", time.Second)
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
	m := newPrometheus(false, maxTenantLabels)
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
	m := newPrometheus(false, maxTenantLabels)
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
	m := newPrometheus(false, maxTenantLabels)
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
	m := newPrometheus(false, maxTenantLabels)
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

// URL jobs can name any host, so past the cap new targets share "other"
// instead of adding series (and map entries) forever.
func TestMetricsTargetLabelsAreCapped(t *testing.T) {
	m := newPrometheus(false, maxTenantLabels)
	m.maxTargets = 2
	for i := range 50 {
		target := fmt.Sprintf("https://host-%d.example", i)
		m.JobWaited("u", target, time.Second)
		m.DispatchAttempt(target, 200)
		m.JobFinished("u", target, "completed", time.Second)
	}
	if len(m.wait) != 3 || len(m.attempts) != 3 || len(m.jobs) != 3 || len(m.duration) != 3 {
		t.Fatalf("want 2 targets plus other in every map, got wait=%d attempts=%d jobs=%d duration=%d",
			len(m.wait), len(m.attempts), len(m.jobs), len(m.duration))
	}
	out := render(m, nil)
	if want := `aquifer_jobs_total{target="other",outcome="completed"} 48`; !strings.Contains(out, want+"\n") {
		t.Errorf("missing %q in:\n%s", want, out)
	}
	if strings.Contains(out, "host-2.example") {
		t.Errorf("targets past the cap should be counted as other:\n%s", out)
	}
}

// Fairness samples are kept only for targets that still have queues.
func TestFairnessSamplesAreDroppedForGoneTargets(t *testing.T) {
	m := newPrometheus(false, maxTenantLabels)
	render(m, []TargetStats{queues(map[string]int64{"a": 1}, map[string]int64{"a": 1})})
	if len(m.samples) != 1 {
		t.Fatalf("want one target sampled, got %d", len(m.samples))
	}
	render(m, nil)
	if len(m.samples) != 0 {
		t.Fatalf("a target with no queues should be dropped, got %d", len(m.samples))
	}
	render(m, []TargetStats{queues(map[string]int64{"a": 1}, map[string]int64{"a": 1})})
	render(m, []TargetStats{{Target: "api.example.com"}})
	if len(m.samples) != 0 {
		t.Fatalf("a live target with no queues should keep no samples, got %d", len(m.samples))
	}
}
