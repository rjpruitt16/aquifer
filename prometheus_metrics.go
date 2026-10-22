// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PrometheusMetrics is the built-in MetricsAdapter behind GET /metrics, in
// Prometheus text format. The aquifer binary uses it by default; a program
// embedding Aquifer opts in by passing NewPrometheusMetrics() as
// RuntimeOptions.Metrics.
//
// Labels are by target API (scheme and host, or pool), never by tenant, unless
// AQUIFER_METRICS_TENANT_LABELS=true: tenant IDs are unbounded and often
// customer data. With it on, the first maxTenantLabels tenants seen get
// their own label and the rest share "other".
type PrometheusMetrics struct {
	tenantLabels bool
	maxTenants   int

	mu         sync.Mutex
	jobs       map[[3]string]float64 // target, outcome, tenant ("" when off)
	attempts   map[[2]string]float64 // target, status class
	rejected   map[string]float64    // reason
	webhooks   map[string]float64    // delivered, failed
	wait       map[string]*histogram // target
	duration   map[[2]string]*histogram
	tenantDur  map[[3]string]*histogram // target, outcome, tenant (tenant labels only)
	tenantSeen map[string]bool
	samples    map[string][]servedSample // target -> recent per-queue served counts
}

const maxTenantLabels = 1000

// fairnessWindow is how far back the fairness gauge compares service.
const fairnessWindow = time.Minute

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

type histogram struct {
	counts []float64 // per bucket, not cumulative
	sum    float64
	count  float64
}

func newHistogram() *histogram { return &histogram{counts: make([]float64, len(durationBuckets))} }

func (h *histogram) observe(seconds float64) {
	for i, le := range durationBuckets {
		if seconds <= le {
			h.counts[i]++
			break
		}
	}
	h.sum += seconds
	h.count++
}

type servedSample struct {
	at     time.Time
	served map[string]int64
}

// NewPrometheusMetrics reads AQUIFER_METRICS_TENANT_LABELS.
func NewPrometheusMetrics() *PrometheusMetrics {
	on, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv("AQUIFER_METRICS_TENANT_LABELS")))
	return newPrometheusMetrics(on, maxTenantLabels)
}

func newPrometheusMetrics(tenantLabels bool, maxTenants int) *PrometheusMetrics {
	return &PrometheusMetrics{
		tenantLabels: tenantLabels,
		maxTenants:   maxTenants,
		jobs:         map[[3]string]float64{},
		attempts:     map[[2]string]float64{},
		rejected:     map[string]float64{},
		webhooks:     map[string]float64{},
		wait:         map[string]*histogram{},
		duration:     map[[2]string]*histogram{},
		tenantDur:    map[[3]string]*histogram{},
		tenantSeen:   map[string]bool{},
		samples:      map[string][]servedSample{},
	}
}

// tenantLocked is the tenant label for userID: "" with tenant labels off.
func (m *PrometheusMetrics) tenantLocked(userID string) string {
	if !m.tenantLabels {
		return ""
	}
	if m.tenantSeen[userID] {
		return userID
	}
	if len(m.tenantSeen) < m.maxTenants {
		m.tenantSeen[userID] = true
		return userID
	}
	return "other"
}

// Jobs are counted from JobFinished, which leaves out Aquifer's internal
// completion-webhook jobs (JobCompleted and JobFailed fire for those too).
// WebhookDelivered and WebhookFailed are registration pings and drain-mode
// ledger flushes, not completion webhooks; those come from
// WebhookJobFinished.
func (m *PrometheusMetrics) JobQueued(userID, upstream string)                      {}
func (m *PrometheusMetrics) JobDispatched(userID, upstream string)                  {}
func (m *PrometheusMetrics) JobCompleted(userID, upstream string, durationMs int64) {}
func (m *PrometheusMetrics) JobFailed(userID, upstream string, reason string)       {}
func (m *PrometheusMetrics) WebhookDelivered(url string, attempt int)               {}
func (m *PrometheusMetrics) WebhookFailed(url string, attempts int)                 {}

func (m *PrometheusMetrics) WebhookJobFinished(outcome string) {
	m.mu.Lock()
	m.webhooks[outcome]++
	m.mu.Unlock()
}

// Queue depth and flow rate are read from the live queues at scrape time
// instead: these events come from many queues under one target label, so
// the last one to report would win.
func (m *PrometheusMetrics) QueueDepth(upstream string, depth int)                  {}
func (m *PrometheusMetrics) FlowRate(upstream string, rps float64)                  {}
func (m *PrometheusMetrics) DrainFlushSucceeded(instanceKey string, ledgerSize int) {}
func (m *PrometheusMetrics) DrainFlushFailed(instanceKey string, ledgerSize int)    {}

func (m *PrometheusMetrics) JobWaited(userID, upstream string, wait time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.wait[upstream]
	if h == nil {
		h = newHistogram()
		m.wait[upstream] = h
	}
	h.observe(wait.Seconds())
}

func (m *PrometheusMetrics) JobFinished(userID, upstream, outcome string, total time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[[3]string{upstream, outcome, m.tenantLocked(userID)}]++
	key := [2]string{upstream, outcome}
	h := m.duration[key]
	if h == nil {
		h = newHistogram()
		m.duration[key] = h
	}
	h.observe(total.Seconds())
	if m.tenantLabels {
		tkey := [3]string{upstream, outcome, m.tenantLocked(userID)}
		th := m.tenantDur[tkey]
		if th == nil {
			th = newHistogram()
			m.tenantDur[tkey] = th
		}
		th.observe(total.Seconds())
	}
}

func (m *PrometheusMetrics) DispatchAttempt(upstream string, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempts[[2]string{upstream, statusClass(status)}]++
}

func (m *PrometheusMetrics) AdmissionRejected(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejected[reason]++
}

// statusClass groups attempt results: 429 on its own, since it's the
// backend saying "slow down" rather than failing.
func statusClass(status int) string {
	switch {
	case status == 0:
		return "error"
	case status == 429:
		return "429"
	case status >= 100 && status < 600:
		return fmt.Sprintf("%dxx", status/100)
	default:
		return "other"
	}
}

// fairnessLocked is Jain's fairness index over the queues that have work
// waiting now, by how many jobs each finished over about the last minute:
// (Σx)² / (n·Σx²), 1.0 when they were all served equally. Queues created
// within the window are left out, since a newcomer's short history would
// look like unfairness. ok is false when there's nothing to compare: no
// queue waiting, or none served.
func (m *PrometheusMetrics) fairnessLocked(stats TargetStats, now time.Time) (index float64, ok bool) {
	current := servedSample{at: now, served: make(map[string]int64, len(stats.Queues))}
	for _, q := range stats.Queues {
		current.served[q.Key] = q.Served
	}
	// Samples are in time order. Compare against the newest one at least a
	// window old (or the oldest, early on), and drop the ones before it.
	samples := m.samples[stats.Target]
	from := -1
	for i := range samples {
		if now.Sub(samples[i].at) >= fairnessWindow {
			from = i
		}
	}
	if from < 0 && len(samples) > 0 {
		from = 0
	}
	var then *servedSample
	if from >= 0 {
		then = &servedSample{at: samples[from].at, served: samples[from].served}
		samples = samples[from:]
	}
	m.samples[stats.Target] = append(samples, current)

	if then == nil {
		return 0, false
	}
	var sum, sumSquares float64
	n := 0
	for _, q := range stats.Queues {
		before, existed := then.served[q.Key]
		if q.Backlog <= 0 || !existed {
			continue
		}
		x := float64(q.Served - before)
		sum += x
		sumSquares += x * x
		n++
	}
	if n == 0 || sumSquares == 0 {
		return 0, false
	}
	return sum * sum / (float64(n) * sumSquares), true
}

// Write renders every metric. targets is the live queue state.
func (m *PrometheusMetrics) Write(w io.Writer, nodeID string, targets []TargetStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	sort.Slice(targets, func(i, j int) bool { return targets[i].Target < targets[j].Target })

	fmt.Fprintf(w, "# HELP aquifer_node_info This node's id, as sent to backends in X-Aqueduct-Node-Id.\n# TYPE aquifer_node_info gauge\n")
	fmt.Fprintf(w, "aquifer_node_info{node_id=%s} 1\n", quote(nodeID))

	// aquifer_jobs_total sums over tenants when tenant labels are on.
	totals := map[[2]string]float64{}
	for k, v := range m.jobs {
		totals[[2]string{k[0], k[1]}] += v
	}
	help(w, "aquifer_jobs_total", "counter", "Jobs that reached a terminal state, by outcome (completed, failed, expired).")
	for _, k := range sortedKeys2(totals) {
		fmt.Fprintf(w, "aquifer_jobs_total{target=%s,outcome=%s} %g\n", quote(k[0]), quote(k[1]), totals[k])
	}
	if m.tenantLabels {
		help(w, "aquifer_tenant_jobs_total", "counter", "Jobs that reached a terminal state, per tenant (AQUIFER_METRICS_TENANT_LABELS).")
		for _, k := range sortedKeys3(m.jobs) {
			fmt.Fprintf(w, "aquifer_tenant_jobs_total{target=%s,outcome=%s,tenant=%s} %g\n", quote(k[0]), quote(k[1]), quote(k[2]), m.jobs[k])
		}
	}

	help(w, "aquifer_dispatch_attempts_total", "counter", "Attempts at a target API, retries included, by result (2xx, 4xx, 429, 5xx, error = no response).")
	for _, k := range sortedKeys2(m.attempts) {
		fmt.Fprintf(w, "aquifer_dispatch_attempts_total{target=%s,status_class=%s} %g\n", quote(k[0]), quote(k[1]), m.attempts[k])
	}

	help(w, "aquifer_admission_rejected_total", "counter", "Jobs refused with 429, by reason.")
	for _, reason := range sortedKeys(m.rejected) {
		fmt.Fprintf(w, "aquifer_admission_rejected_total{reason=%s} %g\n", quote(reason), m.rejected[reason])
	}

	help(w, "aquifer_webhooks_total", "counter", "Completion webhooks that finished, by outcome (delivered, or failed after its retries).")
	for _, outcome := range sortedKeys(m.webhooks) {
		fmt.Fprintf(w, "aquifer_webhooks_total{outcome=%s} %g\n", quote(outcome), m.webhooks[outcome])
	}

	help(w, "aquifer_queue_wait_seconds", "histogram", "Time from submission to a job's first dispatch.")
	for _, target := range sortedKeys(m.wait) {
		writeHistogram(w, "aquifer_queue_wait_seconds", "target="+quote(target), m.wait[target])
	}

	help(w, "aquifer_job_duration_seconds", "histogram", "Time from submission to a terminal state, by outcome.")
	for _, k := range sortedKeys2(m.duration) {
		writeHistogram(w, "aquifer_job_duration_seconds", "target="+quote(k[0])+",outcome="+quote(k[1]), m.duration[k])
	}
	if m.tenantLabels {
		help(w, "aquifer_tenant_job_duration_seconds", "histogram", "Time from submission to a terminal state, per tenant (AQUIFER_METRICS_TENANT_LABELS).")
		for _, k := range sortedKeys3(m.tenantDur) {
			writeHistogram(w, "aquifer_tenant_job_duration_seconds", "target="+quote(k[0])+",outcome="+quote(k[1])+",tenant="+quote(k[2]), m.tenantDur[k])
		}
	}

	help(w, "aquifer_active_queues", "gauge", "Queues on this node with work waiting for the target API (one per tenant with account queues on).")
	for _, t := range targets {
		fmt.Fprintf(w, "aquifer_active_queues{target=%s} %d\n", quote(t.Target), t.ActiveQueues)
	}
	help(w, "aquifer_backlog_jobs", "gauge", "Jobs on this node queued or in flight for the target API.")
	for _, t := range targets {
		fmt.Fprintf(w, "aquifer_backlog_jobs{target=%s} %d\n", quote(t.Target), t.Backlog)
	}
	help(w, "aquifer_flow_rate_rps", "gauge", "Sum of the active queues' current dispatch rates, after the target API's pacing headers.")
	for _, t := range targets {
		fmt.Fprintf(w, "aquifer_flow_rate_rps{target=%s} %g\n", quote(t.Target), round2(t.FlowRate))
	}
	help(w, "aquifer_fairness_jain", "gauge", "Jain's index (0-1) over queues with work waiting, by jobs each finished in about the last minute. 1 is perfectly even.")
	seen := map[string]bool{}
	for _, t := range targets {
		seen[t.Target] = true
		if index, ok := m.fairnessLocked(t, now); ok {
			fmt.Fprintf(w, "aquifer_fairness_jain{target=%s} %g\n", quote(t.Target), round2(index))
		}
	}
	for target := range m.samples {
		if !seen[target] {
			delete(m.samples, target) // its worker is gone
		}
	}
}

func help(w io.Writer, name, kind, text string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, text, name, kind)
}

func writeHistogram(w io.Writer, name, labels string, h *histogram) {
	cumulative := 0.0
	for i, le := range durationBuckets {
		cumulative += h.counts[i]
		fmt.Fprintf(w, "%s_bucket{%s,le=\"%g\"} %g\n", name, labels, le, cumulative)
	}
	fmt.Fprintf(w, "%s_bucket{%s,le=\"+Inf\"} %g\n", name, labels, h.count)
	fmt.Fprintf(w, "%s_sum{%s} %g\n", name, labels, h.sum)
	fmt.Fprintf(w, "%s_count{%s} %g\n", name, labels, h.count)
}

// quote is a Prometheus label value: quoted, with \, " and newlines escaped.
func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeys2[V any](m map[[2]string]V) [][2]string {
	keys := make([][2]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+"\x00"+keys[i][1] < keys[j][0]+"\x00"+keys[j][1] })
	return keys
}

func sortedKeys3[V any](m map[[3]string]V) [][3]string {
	keys := make([][3]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.Join(keys[i][:], "\x00") < strings.Join(keys[j][:], "\x00")
	})
	return keys
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
