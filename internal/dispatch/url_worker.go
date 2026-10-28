// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/admission"
	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/metrics"
	"github.com/rjpruitt16/aquifer/internal/storage"
	"github.com/rjpruitt16/aquifer/l8"
)

type URLWorker struct {
	mu      sync.Mutex
	domain  string
	rps     float64
	maxConc int
	pool    *Pool // nil unless this worker dispatches into a registered pool instead of a fixed domain
	// accountQueueMode is the effective mode -- one queue per tenant, or one
	// shared queue -- recomputed by applyAccountQueueModeLocked from, in
	// order: the destination's own say (destinationMode, from its
	// X-Aqueduct-Account-Queue responses), the node owner's default
	// (AQUIFER_ACCOUNT_QUEUE), and a sender asking for it (senderOptIn,
	// which can only turn it on).
	accountQueueMode bool
	nodeDefault      bool
	senderOptIn      bool
	destinationMode  *bool
	// onDestinationMode lets the registry remember the destination's choice
	// across this worker being retired and recreated.
	onDestinationMode func(bool)
	slowStart         bool // set by a target API's X-Aqueduct-Slow-Start response header; applies to the *next* new queue created for this domain, not retroactively
	maxBacklog        atomic.Int64
	activeQueues      atomic.Int64
	backlog           atomic.Int64
	queues            map[string]*AccountQueue
	store             storage.Store
	broker            *Broker
	l8                *l8.Registry
	metrics           MetricsAdapter
	enqueueWebhook    WebhookEnqueuer
	resultRecorder    storage.ResultRecorder
	onJobDone         func(*jobs.Job)
	onIdle            func(string, *URLWorker)
	breakerUntil      time.Time // zero value means the breaker is closed
	breakerKind       string    // "queue" or "reroute" — which kind of signal tripped it, see classifyOverload
	// gate holds the destination's advertised totals, shared by every
	// account queue here and by proxy mode's direct attempts. nil unless
	// AQUIFER_SHARED_DESTINATION_LIMITS is on; then done is used instead,
	// by the proportional budget loop.
	gate      *Gate
	closeOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// SharedDestinationLimits reports whether a destination's advertised
// X-Aqueduct-Max-Concurrent and X-Aqueduct-Rps are totals shared by all its
// account queues (one Gate per destination) rather than each queue's own,
// with the proportional budget loop bounding the sum.
func SharedDestinationLimits() bool {
	return env.Bool("AQUIFER_SHARED_DESTINATION_LIMITS", false)
}

// BreakerOpen reports whether proxy mode should skip a direct dispatch
// attempt to this worker's domain entirely and fall straight back to the
// durable queue — set by TripBreaker after an overload signal, cleared
// automatically once the cooldown elapses.
func (w *URLWorker) BreakerOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return !w.breakerUntil.IsZero() && time.Now().Before(w.breakerUntil)
}

// BreakerKind reports which kind of signal tripped the breaker last —
// "queue" or "reroute" (see classifyOverload) — only meaningful while
// BreakerOpen is true. A subsequent request arriving while the breaker is
// still open has no fresh response of its own to classify, so it reuses
// whichever kind actually tripped it: a domain breaker-tripped by a 429
// stays queue-only on every retry during that cooldown, not
// reroute-eligible just because SOME overload happened.
func (w *URLWorker) BreakerKind() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.breakerKind
}

// TripBreaker opens the breaker for cooldown, recording which kind of
// signal caused it (see BreakerKind). No separate half-open state is
// needed: once breakerUntil passes, BreakerOpen naturally returns false
// again, so the next request after the cooldown is itself a real probe
// against the live target API — success leaves the breaker closed, a repeat
// overload signal re-trips it via another TripBreaker call.
func (w *URLWorker) TripBreaker(cooldown time.Duration, kind string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.breakerUntil = time.Now().Add(cooldown)
	w.breakerKind = kind
}

func (w *URLWorker) HandleMaxBacklogHeader(value string) {
	maxBacklog, err := strconv.ParseInt(value, 10, 64)
	if err != nil || maxBacklog < 0 {
		return
	}
	w.maxBacklog.Store(maxBacklog)
}

// QueueActive reports whether any of this domain's account queues currently
// has real backlog (queued or in-flight work). Distinct from BreakerOpen: a
// breaker cooldown is a fixed clock that can expire while a real backlog is
// still draining, letting proxy mode resume direct dispatch against an
// target API that's still catching up from the very overload that tripped the
// breaker. QueueActive self-corrects instead — it stays true for exactly as
// long as there's real work in flight, independent of any timer, and goes
// false the instant the backlog is actually empty.
func (w *URLWorker) QueueActive() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, q := range w.queues {
		if q.Active() {
			return true
		}
	}
	return false
}

func NewURLWorker(domain string, rps float64, maxConc int, pool *Pool, store storage.Store, broker *Broker, l8 *l8.Registry, metrics MetricsAdapter, enqueueWebhook WebhookEnqueuer, resultRecorder storage.ResultRecorder, onJobDone func(*jobs.Job), onIdle func(string, *URLWorker)) *URLWorker {
	w := &URLWorker{
		domain:         domain,
		rps:            rps,
		maxConc:        maxConc,
		pool:           pool,
		queues:         make(map[string]*AccountQueue),
		store:          store,
		broker:         broker,
		l8:             l8,
		metrics:        EnsureMetrics(metrics),
		enqueueWebhook: enqueueWebhook,
		resultRecorder: resultRecorder,
		onJobDone:      onJobDone,
		onIdle:         onIdle,
		stop:           make(chan struct{}),
	}
	w.maxBacklog.Store(admission.MaxPendingPerUpstream())
	if SharedDestinationLimits() {
		w.gate = NewGate(w.budgetCeiling)
	} else {
		w.done = make(chan struct{})
		go w.enforceAggregateBudget()
	}
	return w
}

func (w *URLWorker) Stop() {
	w.closeOnce.Do(func() {
		w.signalStop()

		w.mu.Lock()
		queues := make([]*AccountQueue, 0, len(w.queues))
		for _, q := range w.queues {
			queues = append(queues, q)
		}
		w.mu.Unlock()

		for _, q := range queues {
			q.Stop()
		}
		if w.done != nil {
			<-w.done
		}
	})
}

func (w *URLWorker) signalStop() {
	w.stopOnce.Do(func() {
		close(w.stop)
	})
}

// enforceAggregateBudget periodically checks whether the sum of every
// active account queue's current rate exceeds this worker's actual
// budget (static config, or live pool capacity), and if so, throttles
// each queue proportionally. Without this, account-queue mode isolates
// tenants from each other but doesn't bound them collectively — N
// simultaneously active tenant queues could each independently believe
// they own the full ceiling, multiplying real load on the target API by N.
// It runs only while shared destination limits are off; the Gate bounds
// the sum when they are on.
func (w *URLWorker) enforceAggregateBudget() {
	defer close(w.done)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.checkAndThrottle()
		case <-w.stop:
			return
		}
	}
}

// checkAndThrottle is the synchronous core of enforceAggregateBudget,
// split out so it can be called directly and deterministically in tests
// instead of waiting on the real ticker.
func (w *URLWorker) checkAndThrottle() {
	w.mu.Lock()
	queues := make([]*AccountQueue, 0, len(w.queues))
	for _, q := range w.queues {
		if q.Active() {
			queues = append(queues, q)
		}
	}
	w.mu.Unlock()

	if len(queues) < 2 {
		// A single active queue (or none) can't exceed an aggregate
		// budget by definition — nothing to throttle.
		return
	}

	ceiling := w.budgetCeiling()
	if ceiling <= 0 {
		return
	}

	var total float64
	for _, q := range queues {
		total += q.RPS()
	}
	if total <= ceiling {
		return
	}

	scale := ceiling / total
	for _, q := range queues {
		q.Throttle(math.Max(q.RPS()*scale, MinRPS))
	}
}

// TryDirect takes a turn at this destination's in-flight total for a proxy
// mode direct attempt. It returns false when the destination is at its
// total or queues are waiting for a turn; the job should then be queued.
// release ends the turn and may be called more than once; call it once the
// response body has been read or closed. Without shared destination limits
// every direct attempt may go, as before.
func (w *URLWorker) TryDirect() (release func(), ok bool) {
	if w.gate == nil {
		return func() {}, true
	}
	if !w.gate.TryDirect() {
		return nil, false
	}
	var once sync.Once
	return func() { once.Do(w.gate.Release) }, true
}

// DirectAdvertised applies the totals the destination advertised on a
// direct attempt's response.
func (w *URLWorker) DirectAdvertised(resp *http.Response) {
	if w.gate == nil || resp == nil {
		return
	}
	header := resp.Header
	var maxConc *int
	if v, err := strconv.Atoi(PacingHeader(header, "Max-Concurrent")); err == nil {
		maxConc = &v
	}
	var rps *float64
	if v, err := strconv.ParseFloat(PacingHeader(header, "Rps"), 64); err == nil {
		rps = &v
	}
	failed := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	w.gate.Advertised(maxConc, rps, failed)
}

// aggregateRPS is the live sum of every active child queue's current
// rate — this is what actually answers "how many account queues are
// firing at what total capacity," a question isolated per-queue pacing
// alone couldn't answer, since each queue was independently capped at
// the full configured ceiling with no shared budget.
func (w *URLWorker) aggregateRPS() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total float64
	for _, q := range w.queues {
		if q.Active() {
			total += q.RPS()
		}
	}
	return total
}

// budgetCeiling is the total rate all of this worker's account queues
// combined should never exceed: the pool's live aggregate capacity if
// pool-backed, otherwise the statically configured RPS.
func (w *URLWorker) budgetCeiling() float64 {
	if w.pool != nil {
		return w.pool.TotalCapacity()
	}
	return w.rps
}

func (w *URLWorker) Enqueue(job *jobs.Job) bool {
	_, _, enqueued := w.enqueue(job, false)
	return enqueued
}

func (w *URLWorker) AdmitAndEnqueue(job *jobs.Job) (admission.QueueSnapshot, *admission.RejectedError, bool) {
	return w.enqueue(job, true)
}

func (w *URLWorker) enqueue(job *jobs.Job, enforceAdmission bool) (admission.QueueSnapshot, *admission.RejectedError, bool) {
	q, snapshot, rejected, ok := w.reserve(job, enforceAdmission)
	if rejected != nil || !ok {
		return snapshot, rejected, ok
	}
	if !q.handoff(job) {
		return admission.QueueSnapshot{}, nil, false
	}
	return snapshot, nil, true
}

// reserve makes the admission decision and reserves the job's backlog slot
// under the worker lock, so concurrent submissions see each other in the
// fair-admission math. The caller hands the job over after the lock is
// released.
func (w *URLWorker) reserve(job *jobs.Job, enforceAdmission bool) (*AccountQueue, admission.QueueSnapshot, *admission.RejectedError, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.stop:
		return nil, admission.QueueSnapshot{}, nil, false
	default:
	}

	key := SharedQueueKey
	if w.accountQueueMode {
		key = JobQueueKey(job)
	}

	q, ok := w.queues[key]
	if enforceAdmission {
		var totalPending, queuePending int64
		activeQueues := 0
		for queueKey, candidate := range w.queues {
			pending := candidate.Backlog()
			if pending <= 0 {
				continue
			}
			totalPending += pending
			activeQueues++
			if queueKey == key {
				queuePending = pending
			}
		}
		if !ok || queuePending == 0 {
			activeQueues++
		}
		if !w.accountQueueMode {
			activeQueues = 1
			queuePending = totalPending
		}

		maxBacklog := w.maxBacklog.Load()
		decision := admission.DecideFair(queuePending, totalPending, activeQueues, maxBacklog, admission.RandomDraw())
		if !decision.Allowed {
			snapshot := decision.Snapshot
			return nil, snapshot, &admission.RejectedError{Decision: admission.Decision{
				Allowed: false,
				Reason:  decision.Reason,
				Limit:   maxBacklog,
				Current: totalPending,
				Queue:   &snapshot,
			}}, true
		}
	}

	if !ok {
		q = NewAccountQueue(key, w.domain, w.rps, w.maxConc, w.pool, w.store, w.broker, w.l8, w.metrics, w.enqueueWebhook, w.resultRecorder, &w.activeQueues, &w.backlog, w.onJobDone, func(k string) {
			w.mu.Lock()
			// Retire only this queue, and only if nothing was reserved on it
			// or buffered for it since its run loop went idle.
			if w.queues[k] != q || q.Backlog() > 0 || len(q.cmds) > 0 {
				w.mu.Unlock()
				return
			}
			delete(w.queues, k)
			empty := len(w.queues) == 0
			if empty {
				w.signalStop()
			}
			w.mu.Unlock()
			// Propagate upward so Registry can observe an instance-wide
			// idle state -- w.onIdle was previously wired but never
			// called, leaking this worker in Registry.workers forever
			// once its last queue went idle.
			if empty && w.onIdle != nil {
				w.onIdle(w.domain, w)
			}
		}, w.slowStart, func(v bool) {
			w.mu.Lock()
			w.slowStart = v
			w.mu.Unlock()
		}, func(v int64) {
			w.maxBacklog.Store(v)
		})
		destinationSignal := w.HandleDestinationAccountQueue
		q.accountQueueSignal.Store(&destinationSignal)
		q.gate.Store(w.gate)
		w.queues[key] = q
	}

	q.reserveBacklog()
	return q, w.snapshotForQueueLocked(key), nil, true
}

func (w *URLWorker) snapshotForQueueLocked(key string) admission.QueueSnapshot {
	snapshot := admission.QueueSnapshot{MaxBacklog: w.maxBacklog.Load()}
	for queueKey, q := range w.queues {
		pending := q.Backlog()
		if pending <= 0 {
			continue
		}
		snapshot.ActiveQueues++
		snapshot.UpstreamBacklog += pending
		if queueKey == key {
			snapshot.QueueBacklog = pending
		}
	}
	if !w.accountQueueMode && snapshot.UpstreamBacklog > 0 {
		snapshot.ActiveQueues = 1
		snapshot.QueueBacklog = snapshot.UpstreamBacklog
	}
	if snapshot.MaxBacklog > 0 {
		start := admission.FairStart * float64(snapshot.MaxBacklog)
		snapshot.AdmissionPressure = admission.Clamp((float64(snapshot.UpstreamBacklog)-start)/(float64(snapshot.MaxBacklog)-start), 0, 1)
	}
	return snapshot
}

func (w *URLWorker) Snapshot() admission.QueueSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotForQueueLocked("")
}

// handleAccountQueueHeader applies a sender's X-Aqueduct-Account-Queue
// request header. A sender can ask for one queue per tenant but can't turn
// that off: the mode is per destination, so one sender switching it off
// would take isolation away from every other tenant sending there. Only
// the node owner (AQUIFER_ACCOUNT_QUEUE) and the destination can.
func (w *URLWorker) handleAccountQueueHeader(val string) {
	on, ok := ParseAccountQueueValue(val)
	if !ok || !on {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.senderOptIn = true
	w.applyAccountQueueModeLocked()
}

// setAccountQueuePolicy sets the node owner's default and, if the registry
// remembers one, the destination's earlier choice. Called once, right
// after the worker is created.
func (w *URLWorker) setAccountQueuePolicy(nodeDefault bool, destination *bool, onDestinationMode func(bool)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nodeDefault = nodeDefault
	w.destinationMode = destination
	w.onDestinationMode = onDestinationMode
	w.applyAccountQueueModeLocked()
}

// HandleDestinationAccountQueue applies the destination's own
// X-Aqueduct-Account-Queue answer, which overrides everything else.
func (w *URLWorker) HandleDestinationAccountQueue(on bool) {
	w.mu.Lock()
	changed := w.destinationMode == nil || *w.destinationMode != on
	w.destinationMode = &on
	w.applyAccountQueueModeLocked()
	remember := w.onDestinationMode
	w.mu.Unlock()
	if changed && remember != nil {
		remember(on)
	}
}

func (w *URLWorker) applyAccountQueueModeLocked() {
	if w.destinationMode != nil {
		w.accountQueueMode = *w.destinationMode
		return
	}
	w.accountQueueMode = w.nodeDefault || w.senderOptIn
}

// ParseAccountQueueValue reads an X-Aqueduct-Account-Queue value: enabled,
// true, on, yes or 1 for one queue per tenant; disabled, false, off, no or
// 0 for one shared queue. ok is false for anything else.
func ParseAccountQueueValue(val string) (on bool, ok bool) {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "enabled", "true", "on", "yes", "1":
		return true, true
	case "disabled", "false", "off", "no", "0":
		return false, true
	}
	return false, false
}

// accountQueueDefault is the node owner's AQUIFER_ACCOUNT_QUEUE: on (the
// default) gives each tenant its own queue at every destination unless the
// destination says otherwise; off shares one queue per destination unless
// a sender or the destination asks for isolation.
func accountQueueDefault() bool {
	val := os.Getenv("AQUIFER_ACCOUNT_QUEUE")
	if val == "" {
		return true
	}
	on, ok := ParseAccountQueueValue(val)
	if !ok {
		log.Printf("AQUIFER_ACCOUNT_QUEUE=%q isn't on or off; using on", val)
		return true
	}
	return on
}

const SharedQueueKey = "__shared__"

func JobQueueKey(job *jobs.Job) string {
	apiKey := job.Headers["Authorization"]
	if apiKey == "" {
		apiKey = job.Headers["x-api-key"]
	}
	if apiKey == "" {
		apiKey = job.Headers["api-key"]
	}
	raw := fmt.Sprintf("%s:%s", job.UserID, apiKey)
	return sha256String(raw)
}

func DomainKey(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}

func sha256String(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func (w *URLWorker) Stats() metrics.TargetStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	stats := metrics.TargetStats{Target: w.domain, Queues: make([]metrics.QueueStat, 0, len(w.queues))}
	for key, q := range w.queues {
		qs := metrics.QueueStat{Key: key, Backlog: q.Backlog(), RPS: q.RPS(), Served: q.Served()}
		if qs.Backlog > 0 {
			stats.ActiveQueues++
			stats.Backlog += qs.Backlog
			stats.FlowRate += qs.RPS
		}
		stats.Queues = append(stats.Queues, qs)
	}
	return stats
}

// AccountQueueMode reports whether this worker queues each tenant
// separately (X-Aqueduct-Account-Queue).
func (w *URLWorker) AccountQueueMode() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.accountQueueMode
}
