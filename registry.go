package aquifer

import (
	"encoding/json"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu              sync.Mutex
	workers         map[string]*URLWorker
	store           JobStore
	cfg             *Config
	broker          *Broker
	l8              *L8Registry
	metrics         MetricsAdapter
	pools           *PoolRegistry
	totalJobs       atomic.Int64
	queueDepth      atomic.Int64
	drainCfg        DrainConfig
	drainState      atomic.Value // DrainState, read from Health() concurrently with the watchdog goroutine writing it
	registrationCfg RegistrationConfig
}

// NewRegistry reads drain mode's config from AQUIFER_DRAIN_* env vars
// (LoadDrainConfig) — disabled unless AQUIFER_DRAIN_ENABLED is explicitly
// set, matching NewPebbleStore's existing precedent of reading its own
// opt-in env vars internally. Callers wanting a programmatic override
// (RuntimeOptions.DrainConfig) call ConfigureDrain after construction.
func NewRegistry(store JobStore, cfg *Config, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pools *PoolRegistry) *Registry {
	r := &Registry{
		workers:         make(map[string]*URLWorker),
		store:           store,
		cfg:             cfg,
		broker:          broker,
		l8:              l8,
		metrics:         ensureMetrics(metrics),
		pools:           pools,
		drainCfg:        LoadDrainConfig(),
		registrationCfg: LoadRegistrationConfig(),
	}
	counts := store.Counts()
	r.totalJobs.Store(counts.TotalJobs)
	r.queueDepth.Store(counts.QueueDepth)
	r.drainState.Store(DrainStateActive)
	if r.drainCfg.Enabled {
		go r.drainWatchdogLoop()
	}
	if r.registrationCfg.Enabled() {
		go r.registrationLoop()
	}
	return r
}

// DrainState is the instance's current position in drain mode's lifecycle
// (active/draining/unassigned) — meaningful only when drain mode is
// enabled, but always safe to call (returns DrainStateActive otherwise,
// since the watchdog that would ever move it elsewhere never runs).
func (r *Registry) DrainState() DrainState {
	return r.drainState.Load().(DrainState)
}

func (r *Registry) setDrainState(s DrainState) {
	r.drainState.Store(s)
}

// DrainSnapshot reports drain mode's current state for GET /health, or nil
// when drain mode isn't enabled — an instance that never turned this on
// shouldn't see a new key appear in its health output.
func (r *Registry) DrainSnapshot() map[string]any {
	if !r.drainCfg.Enabled {
		return nil
	}
	return map[string]any{"state": string(r.DrainState())}
}

// ConfigureDrain overrides drain mode's config after construction (used by
// RuntimeOptions.DrainConfig) and starts the watchdog if the override
// enables it and the constructor-time env-based config hadn't already
// started one. There is no supported path to stop an already-running
// watchdog at runtime — disabling drain mode requires a restart, same as
// every other env-var-driven config in this codebase.
func (r *Registry) ConfigureDrain(cfg DrainConfig) {
	wasEnabled := r.drainCfg.Enabled
	r.drainCfg = cfg
	if cfg.Enabled && !wasEnabled {
		go r.drainWatchdogLoop()
	}
}

// Enqueue queues a job on the URLWorker for its upstream domain, or for
// its target pool if the job carries a PoolID instead of a URL.
// accountQueueHeader is the raw X-Aqueduct-Account-Queue/X-Aquifer-Account-Queue
// value from the originating HTTP request, or "" if this job has no live
// request behind it (e.g. recovered from disk at startup). An empty value
// leaves the worker's current account-queue mode unchanged rather than
// forcing it off — the mode is shared per upstream domain, so one request
// that doesn't care about it shouldn't be able to flip it off for every
// other concurrent tenant relying on it being on.
func (r *Registry) Enqueue(job *Job, accountQueueHeader string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalJobs.Add(1)
	r.queueDepth.Add(1)

	key, w := r.resolveWorkerLocked(job)

	r.metrics.JobQueued(job.UserID, key)

	if accountQueueHeader != "" {
		w.handleAccountQueueHeader(accountQueueHeader)
	}

	w.Enqueue(job)
	r.metrics.QueueDepth(key, int(r.queueDepth.Load()))
}

// workerFor resolves (creating if necessary) the URLWorker that would
// handle this job's dispatch, without enqueueing anything onto it or
// touching the queue-depth/metrics counters Enqueue updates. Used by proxy
// mode's circuit breaker to check/trip breaker state before a job is ever
// actually queued.
func (r *Registry) workerFor(job *Job) *URLWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, w := r.resolveWorkerLocked(job)
	return w
}

// resolveWorkerLocked must be called with r.mu held. Returns the job's
// routing key and its (possibly newly created) URLWorker — the exact
// key/worker resolution Enqueue already did inline, now shared with
// workerFor.
func (r *Registry) resolveWorkerLocked(job *Job) (string, *URLWorker) {
	var key string
	var pool *Pool
	var rc RateConfig
	if job.PoolID != "" {
		key = "pool:" + job.PoolID
		if r.pools != nil {
			pool = r.pools.Get(job.PoolID)
		}
		rc = r.cfg.Defaults
	} else {
		key = domainKey(job.URL)
		rc = r.cfg.ForURL(job.URL)
	}

	w, ok := r.workers[key]
	if !ok {
		w = NewURLWorker(key, rc.RPS, rc.MaxConcurrent, pool, r.store, r.broker, r.l8, r.metrics, r.EnqueueWebhook, func(k string) {
			r.mu.Lock()
			delete(r.workers, k)
			r.mu.Unlock()
		})
		r.workers[key] = w
	}
	return key, w
}

// EnqueueWebhook queues a webhook delivery through the same domain-keyed
// account-queue pacing and backpressure machinery as forward dispatch
// (RPS/concurrency limits, X-Aqueduct-* response-header throttling) instead
// of firing immediately with a fixed retry schedule — a slow or
// rate-limited webhook receiver can now shed load exactly the way an
// upstream API already can, and delivery is durable across a restart the
// same way a real job is (the underlying webhook-delivery Job is persisted
// via CheckOrInsert, not just an in-memory retry loop).
//
// originalJobID scopes the idempotent key (see Job.isWebhookDeliveryJob) so
// a given job's webhook is enqueued at most once even if this were somehow
// called twice for it.
func (r *Registry) EnqueueWebhook(originalJobID, userID, webhookURL string, payload map[string]any) {
	if webhookURL == "" {
		return
	}

	body, _ := json.Marshal(payload)
	job := NewJob(&JobRequest{
		UserID:        userID,
		IdempotentKey: "webhook:" + originalJobID,
		URL:           webhookURL,
		Method:        "POST",
		Headers:       map[string]string{"Content-Type": "application/json"},
		Body:          string(body),
	})

	if _, duplicate := r.store.CheckOrInsert(job); duplicate {
		return
	}

	r.Enqueue(job, "")
}

func (r *Registry) JobDispatched() {
	r.queueDepth.Add(-1)
}

func (r *Registry) JobDone() {
	r.totalJobs.Add(-1)
}
