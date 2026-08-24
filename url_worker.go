package aquifer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"
)

type URLWorker struct {
	mu               sync.Mutex
	domain           string
	rps              float64
	maxConc          int
	pool             *Pool // nil unless this worker dispatches into a registered pool instead of a fixed domain
	accountQueueMode bool
	queues           map[string]*AccountQueue
	store            JobStore
	broker           *Broker
	l8               *L8Registry
	metrics          MetricsAdapter
	onIdle           func(string)
}

func NewURLWorker(domain string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, onIdle func(string)) *URLWorker {
	w := &URLWorker{
		domain:  domain,
		rps:     rps,
		maxConc: maxConc,
		pool:    pool,
		queues:  make(map[string]*AccountQueue),
		store:   store,
		broker:  broker,
		l8:      l8,
		metrics: ensureMetrics(metrics),
		onIdle:  onIdle,
	}
	go w.enforceAggregateBudget()
	return w
}

// enforceAggregateBudget periodically checks whether the sum of every
// active account queue's current rate exceeds this worker's actual
// budget (static config, or live pool capacity), and if so, throttles
// each queue proportionally. Without this, account-queue mode isolates
// tenants from each other but doesn't bound them collectively — N
// simultaneously active tenant queues could each independently believe
// they own the full ceiling, multiplying real load on the upstream by N.
func (w *URLWorker) enforceAggregateBudget() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		w.checkAndThrottle()
	}
}

// checkAndThrottle is the synchronous core of enforceAggregateBudget,
// split out so it can be called directly and deterministically in tests
// instead of waiting on the real ticker.
func (w *URLWorker) checkAndThrottle() {
	w.mu.Lock()
	queues := make([]*AccountQueue, 0, len(w.queues))
	for _, q := range w.queues {
		queues = append(queues, q)
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
		total += q.RPS()
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

func (w *URLWorker) Enqueue(job *Job) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key := sharedKey
	if w.accountQueueMode {
		key = jobQueueKey(job)
	}

	q, ok := w.queues[key]
	if !ok {
		q = NewAccountQueue(key, w.domain, w.rps, w.maxConc, w.pool, w.store, w.broker, w.l8, w.metrics, func(k string) {
			w.mu.Lock()
			delete(w.queues, k)
			empty := len(w.queues) == 0
			w.mu.Unlock()
			// Propagate upward so Registry can observe an instance-wide
			// idle state -- w.onIdle was previously wired but never
			// called, leaking this worker in Registry.workers forever
			// once its last queue went idle.
			if empty && w.onIdle != nil {
				w.onIdle(w.domain)
			}
		})
		w.queues[key] = q
	}

	q.Enqueue(job)
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
