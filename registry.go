package aquifer

import (
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu         sync.Mutex
	workers    map[string]*URLWorker
	store      JobStore
	cfg        *Config
	broker     *Broker
	l8         *L8Registry
	metrics    MetricsAdapter
	pools      *PoolRegistry
	totalJobs  atomic.Int64
	queueDepth atomic.Int64
	drainCfg   DrainConfig
}

// NewRegistry reads drain mode's config from AQUIFER_DRAIN_* env vars
// (LoadDrainConfig) — disabled unless AQUIFER_DRAIN_ENABLED is explicitly
// set, matching NewPebbleStore's existing precedent of reading its own
// opt-in env vars internally. Callers wanting a programmatic override
// (RuntimeOptions.DrainConfig) call ConfigureDrain after construction.
func NewRegistry(store JobStore, cfg *Config, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pools *PoolRegistry) *Registry {
	r := &Registry{
		workers:  make(map[string]*URLWorker),
		store:    store,
		cfg:      cfg,
		broker:   broker,
		l8:       l8,
		metrics:  ensureMetrics(metrics),
		pools:    pools,
		drainCfg: LoadDrainConfig(),
	}
	counts := store.Counts()
	r.totalJobs.Store(counts.TotalJobs)
	r.queueDepth.Store(counts.QueueDepth)
	if r.drainCfg.Enabled {
		go r.drainWatchdogLoop()
	}
	return r
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

	r.metrics.JobQueued(job.UserID, key)
	w, ok := r.workers[key]
	if !ok {
		w = NewURLWorker(key, rc.RPS, rc.MaxConcurrent, pool, r.store, r.broker, r.l8, r.metrics, func(k string) {
			r.mu.Lock()
			delete(r.workers, k)
			r.mu.Unlock()
		})
		r.workers[key] = w
	}

	if accountQueueHeader != "" {
		w.handleAccountQueueHeader(accountQueueHeader)
	}

	w.Enqueue(job)
	r.metrics.QueueDepth(key, int(r.queueDepth.Load()))
}

func (r *Registry) JobDispatched() {
	r.queueDepth.Add(-1)
}

func (r *Registry) JobDone() {
	r.totalJobs.Add(-1)
}
