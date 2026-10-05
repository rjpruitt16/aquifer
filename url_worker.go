package aquifer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type URLWorker struct {
	mu               sync.Mutex
	domain           string
	rps              float64
	maxConc          int
	pool             *Pool // nil unless this worker dispatches into a registered pool instead of a fixed domain
	accountQueueMode bool
	slowStart        bool // set by an upstream's X-Aqueduct-Slow-Start response header; applies to the *next* new queue created for this domain, not retroactively
	maxBacklog       atomic.Int64
	activeQueues     atomic.Int64
	backlog          atomic.Int64
	queues           map[string]*AccountQueue
	store            JobStore
	broker           *Broker
	l8               *L8Registry
	metrics          MetricsAdapter
	enqueueWebhook   webhookEnqueuer
	resultRecorder   JobResultRecorder
	onJobDone        func(string)
	onIdle           func(string, *URLWorker)
	breakerUntil     time.Time // zero value means the breaker is closed
	breakerKind      string    // "queue" or "reroute" — which kind of signal tripped it, see classifyOverload
	closeOnce        sync.Once
	stopOnce         sync.Once
	stop             chan struct{}
	done             chan struct{}
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
// against the live upstream — success leaves the breaker closed, a repeat
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
// upstream that's still catching up from the very overload that tripped the
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

func NewURLWorker(domain string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook webhookEnqueuer, resultRecorder JobResultRecorder, onJobDone func(string), onIdle func(string, *URLWorker)) *URLWorker {
	w := &URLWorker{
		domain:         domain,
		rps:            rps,
		maxConc:        maxConc,
		pool:           pool,
		queues:         make(map[string]*AccountQueue),
		store:          store,
		broker:         broker,
		l8:             l8,
		metrics:        ensureMetrics(metrics),
		enqueueWebhook: enqueueWebhook,
		resultRecorder: resultRecorder,
		onJobDone:      onJobDone,
		onIdle:         onIdle,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	w.maxBacklog.Store(configuredMaxPendingPerUpstream())
	go w.enforceAggregateBudget()
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
		<-w.done
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
// they own the full ceiling, multiplying real load on the upstream by N.
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
		q.Throttle(math.Max(q.RPS()*scale, minRPS))
	}
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

func (w *URLWorker) Enqueue(job *Job) bool {
	_, _, enqueued := w.enqueue(job, false)
	return enqueued
}

func (w *URLWorker) AdmitAndEnqueue(job *Job) (QueueSnapshot, *AdmissionRejectedError, bool) {
	return w.enqueue(job, true)
}

func (w *URLWorker) enqueue(job *Job, enforceAdmission bool) (QueueSnapshot, *AdmissionRejectedError, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.stop:
		return QueueSnapshot{}, nil, false
	default:
	}

	key := sharedKey
	if w.accountQueueMode {
		key = jobQueueKey(job)
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
		decision := decideFairAdmission(queuePending, totalPending, activeQueues, maxBacklog, randomAdmissionDraw())
		if !decision.Allowed {
			snapshot := decision.Snapshot
			return snapshot, &AdmissionRejectedError{Decision: AdmissionDecision{
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
		w.queues[key] = q
	}

	if !q.Enqueue(job) {
		return QueueSnapshot{}, nil, false
	}
	return w.snapshotForQueueLocked(key), nil, true
}

func (w *URLWorker) snapshotForQueueLocked(key string) QueueSnapshot {
	snapshot := QueueSnapshot{MaxBacklog: w.maxBacklog.Load()}
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
		start := fairAdmissionStart * float64(snapshot.MaxBacklog)
		snapshot.AdmissionPressure = clampFloat((float64(snapshot.UpstreamBacklog)-start)/(float64(snapshot.MaxBacklog)-start), 0, 1)
	}
	return snapshot
}

func (w *URLWorker) Snapshot() QueueSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotForQueueLocked("")
}

func (w *URLWorker) handleAccountQueueHeader(val string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accountQueueMode = val == "enabled"
}

const sharedKey = "__shared__"

func jobQueueKey(job *Job) string {
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

func domainKey(rawURL string) string {
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
