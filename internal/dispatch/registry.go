// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/admission"
	"github.com/rjpruitt16/aquifer/internal/cluster"
	"github.com/rjpruitt16/aquifer/internal/httpx"
	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/metrics"
	"github.com/rjpruitt16/aquifer/internal/storage"
	"github.com/rjpruitt16/aquifer/l8"
)

type Registry struct {
	mu        sync.Mutex
	enqueuing sync.WaitGroup // enqueues past the stopping check; Close waits for them
	workers   map[string]*URLWorker
	// accountQueueDefault is the node owner's AQUIFER_ACCOUNT_QUEUE.
	// destinationAccountQueue remembers each destination's own
	// X-Aqueduct-Account-Queue answer, so it outlives the destination's
	// worker being retired when idle (guarded by mu).
	accountQueueDefault     bool
	destinationAccountQueue map[string]bool
	store                   storage.Store
	cfg                     *Config
	broker                  *Broker
	l8                      *l8.Registry
	metrics                 MetricsAdapter
	pools                   *PoolRegistry
	clusterRouter           *cluster.Router
	drainRemote             storage.Remote
	resultRecorder          storage.ResultRecorder
	users                   *userLoadTracker
	totalJobs               atomic.Int64
	queueDepth              atomic.Int64
	drainCfg                DrainConfig
	drainFlushMu            sync.Mutex
	drainState              atomic.Value // DrainState, read from Health() concurrently with the watchdog goroutine writing it
	drainWatchdog           bool
	drainBatch              bool
	registrationCfg         RegistrationConfig
	registrationState       atomic.Value // cluster.State
	// fullnessSource reports the whole instance's fullness (Aquifer.Fullness,
	// which needs the admission limits the registry doesn't own) for the
	// registration ping. Set after construction, so read atomically.
	fullnessSource atomic.Value // func() int
	// loadSource is Aquifer.Load, for the same reason.
	loadSource atomic.Value // func() Load
	closeOnce  sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
	stop       chan struct{}
	wg         sync.WaitGroup
}

func (r *Registry) SetClusterRouter(router *cluster.Router) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.clusterRouter = router
	r.mu.Unlock()
}

// NewRegistry reads drain mode's config from AQUIFER_DRAIN_* env vars
// (LoadDrainConfig) — disabled unless AQUIFER_DRAIN_ENABLED is explicitly
// set, matching NewPebbleStore's existing precedent of reading its own
// opt-in env vars internally. Callers wanting a programmatic override
// (RuntimeOptions.DrainConfig) call ConfigureDrain after construction.
func NewRegistry(store storage.Store, cfg *Config, broker *Broker, l8 *l8.Registry, metrics MetricsAdapter, pools *PoolRegistry) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registry{
		users:                   newUserLoadTracker(),
		workers:                 make(map[string]*URLWorker),
		accountQueueDefault:     accountQueueDefault(),
		destinationAccountQueue: make(map[string]bool),
		store:                   store,
		cfg:                     cfg,
		broker:                  broker,
		l8:                      l8,
		metrics:                 EnsureMetrics(metrics),
		pools:                   pools,
		drainCfg:                LoadDrainConfig(),
		registrationCfg:         LoadRegistrationConfig(),
		ctx:                     ctx,
		cancel:                  cancel,
		stop:                    make(chan struct{}),
	}
	r.applyDrainEvents()
	counts := store.Counts()
	r.totalJobs.Store(counts.TotalJobs)
	r.queueDepth.Store(counts.QueueDepth)
	r.drainState.Store(DrainStateActive)
	r.registrationState.Store(cluster.StateActive)
	if r.drainCfg.Enabled {
		r.startDrainLoops()
	}
	if r.registrationCfg.Enabled() {
		r.startRegistrationLoop()
	}
	return r
}

func (r *Registry) BeginDrain() {
	if r == nil {
		return
	}
	r.registrationState.Store(cluster.StateDraining)
	if r.registrationCfg.Enabled() {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.pingRegistry()
		}()
	}
}

func (r *Registry) WaitIdle(ctx context.Context) error {
	if r == nil {
		return nil
	}
	ticker := time.NewTicker(idlePollInterval)
	defer ticker.Stop()
	for {
		if r.idleForShutdown() {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Registry) idleForShutdown() bool {
	if r.store.Counts().TotalJobs != 0 {
		return false
	}
	r.mu.Lock()
	workers := make([]*URLWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	r.mu.Unlock()
	for _, worker := range workers {
		if worker.QueueActive() {
			return false
		}
	}
	return true
}

func (r *Registry) FlushForShutdown(ctx context.Context) bool {
	if r == nil || !r.drainCfg.Enabled {
		return true
	}
	r.setDrainState(DrainStateDraining)
	return r.attemptDrainFlushContext(ctx)
}

func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		r.cancel()
		close(r.stop)

		// enqueue registers under r.mu after checking r.stop, so once this
		// lock is taken no new enqueue can start; wait for the ones already
		// writing to the store or handing jobs to queues.
		r.mu.Lock()
		r.mu.Unlock()
		r.enqueuing.Wait()

		r.mu.Lock()
		workers := make([]*URLWorker, 0, len(r.workers))
		for _, w := range r.workers {
			workers = append(workers, w)
		}
		r.mu.Unlock()

		for _, w := range workers {
			w.Stop()
		}
		r.wg.Wait()

		if r.pools != nil {
			r.pools.Stop()
		}
		if r.l8 != nil {
			r.l8.Close()
		}
		if r.store != nil {
			r.store.Close()
		}
	})
}

func (r *Registry) stopping() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

// DrainState is the instance's current position in drain mode's lifecycle
// (active/draining/unassigned) — meaningful only when drain mode is
// enabled, but always safe to call (returns DrainStateActive otherwise,
// since the watchdog that would ever move it elsewhere never runs).
func (r *Registry) DrainState() DrainState {
	return r.drainState.Load().(DrainState)
}

func (r *Registry) setDrainState(s DrainState) {
	prev, _ := r.drainState.Swap(s).(DrainState)
	// Becoming free is the one transition a control plane is waiting on,
	// so report it now instead of on the next heartbeat.
	if s == DrainStateUnassigned && prev != DrainStateUnassigned && r.registrationCfg.Enabled() {
		go r.pingRegistry()
	}
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
	r.drainCfg = cfg
	r.applyDrainEvents()
	if cfg.Enabled {
		r.startDrainLoops()
	}
}

func (r *Registry) SetDrainRemote(remote storage.Remote) {
	r.mu.Lock()
	r.drainRemote = remote
	if recorder, ok := remote.(storage.ResultRecorder); ok {
		r.resultRecorder = recorder
	} else {
		r.resultRecorder = nil
	}
	r.mu.Unlock()
}

func (r *Registry) drainRemoteOrNil() storage.Remote {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drainRemote
}

func (r *Registry) startDrainLoops() {
	r.mu.Lock()
	startWatchdog := !r.drainWatchdog
	startBatch := r.drainCfg.BatchEnabled && !r.drainBatch
	if startWatchdog {
		r.drainWatchdog = true
	}
	if startBatch {
		r.drainBatch = true
	}
	r.mu.Unlock()

	if startWatchdog {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.drainWatchdogLoop()
		}()
	}
	if startBatch {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.drainBatchLoop()
		}()
	}
}

func (r *Registry) startRegistrationLoop() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.registrationLoop()
	}()
}

// Enqueue queues a job on the URLWorker for its target API domain, or for
// its target pool if the job carries a PoolID instead of a URL.
// accountQueueHeader is the raw X-Aqueduct-Account-Queue/X-Aquifer-Account-Queue
// value from the originating HTTP request, or "" if this job has no live
// request behind it (e.g. recovered from disk at startup). An empty value
// leaves the worker's current account-queue mode unchanged rather than
// forcing it off — the mode is shared per target API domain, so one request
// that doesn't care about it shouldn't be able to flip it off for every
// other concurrent tenant relying on it being on.
func (r *Registry) Enqueue(job *jobs.Job, accountQueueHeader string) {
	_, _ = r.enqueue(job, accountQueueHeader, false)
}

func (r *Registry) AdmitAndEnqueue(job *jobs.Job, accountQueueHeader string) (admission.QueueSnapshot, error) {
	return r.enqueue(job, accountQueueHeader, true)
}

func (r *Registry) enqueue(job *jobs.Job, accountQueueHeader string, enforceAdmission bool) (admission.QueueSnapshot, error) {
	if r.stopping() {
		return admission.QueueSnapshot{}, httpx.ErrDraining
	}

	// userLoadTracker has its own lock, so this runs before the registry
	// lock (which now covers only resolving the worker).
	if enforceAdmission && !job.IsWebhookDelivery() {
		if rejected, limit, backlog := r.users.webhookBacklogDecision(job.UserID, admission.RandomDraw()); rejected {
			return admission.QueueSnapshot{}, &admission.RejectedError{Decision: admission.Decision{
				Allowed: false,
				Reason:  "webhook_backlog",
				Limit:   limit,
				Current: backlog,
			}}
		}
	}

	for {
		// The registry lock covers only finding the worker. Admission, the
		// store write and the handoff to the queue happen outside it, so
		// submissions and webhook enqueues no longer run one at a time.
		r.mu.Lock()
		if r.stopping() {
			r.mu.Unlock()
			return admission.QueueSnapshot{}, httpx.ErrDraining
		}
		key, w := r.resolveWorkerLocked(job)
		r.enqueuing.Add(1)
		r.mu.Unlock()
		snapshot, err, done := r.enqueueOnWorker(key, w, job, accountQueueHeader, enforceAdmission)
		r.enqueuing.Done()
		if done {
			return snapshot, err
		}

		// The worker can self-stop after its last AccountQueue idles out.
		// If Enqueue races that transition, drop the stale registry entry
		// and resolve a fresh worker instead of silently losing the job.
		r.mu.Lock()
		if current := r.workers[key]; current == w {
			delete(r.workers, key)
		}
		r.mu.Unlock()
	}
}

// enqueueOnWorker runs one attempt against w. done is false when the worker
// had stopped and the caller should resolve a fresh one.
func (r *Registry) enqueueOnWorker(key string, w *URLWorker, job *jobs.Job, accountQueueHeader string, enforceAdmission bool) (admission.QueueSnapshot, error, bool) {
	{
		if accountQueueHeader != "" {
			w.handleAccountQueueHeader(accountQueueHeader)
		}

		if r.clusterRouter != nil {
			r.clusterRouter.TrackLocalJob(job.UserID)
		}
		var snapshot admission.QueueSnapshot
		var admissionErr *admission.RejectedError
		var workerLive bool
		if enforceAdmission {
			snapshot, admissionErr, workerLive = w.AdmitAndEnqueue(job)
		} else {
			workerLive = w.Enqueue(job)
			if workerLive {
				snapshot = w.Snapshot()
			}
		}
		if admissionErr != nil {
			if r.clusterRouter != nil {
				r.clusterRouter.CompleteLocalJob(job.UserID)
			}
			return snapshot, admissionErr, true
		}
		if workerLive {
			r.users.add(job)
			r.totalJobs.Add(1)
			r.queueDepth.Add(1)
			r.metrics.JobQueued(job.UserID, key)
			r.metrics.QueueDepth(key, int(r.queueDepth.Load()))
			return snapshot, nil, true
		}
		if r.clusterRouter != nil {
			r.clusterRouter.CompleteLocalJob(job.UserID)
		}
		return admission.QueueSnapshot{}, nil, false
	}
}

func (r *Registry) QueueSnapshot() admission.QueueSnapshot {
	r.mu.Lock()
	workers := make([]*URLWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	r.mu.Unlock()

	var snapshot admission.QueueSnapshot
	for _, worker := range workers {
		workerSnapshot := worker.Snapshot()
		snapshot.ActiveQueues += workerSnapshot.ActiveQueues
		snapshot.UpstreamBacklog += workerSnapshot.UpstreamBacklog
	}
	return snapshot
}

// WorkerFor resolves (creating if necessary) the URLWorker that would
// handle this job's dispatch, without enqueueing anything onto it or
// touching the queue-depth/metrics counters Enqueue updates. Used by proxy
// mode's circuit breaker to check/trip breaker state before a job is ever
// actually queued.
func (r *Registry) WorkerFor(job *jobs.Job) *URLWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, w := r.resolveWorkerLocked(job)
	return w
}

// resolveWorkerLocked must be called with r.mu held. Returns the job's
// routing key and its (possibly newly created) URLWorker — the exact
// key/worker resolution Enqueue already did inline, now shared with
// WorkerFor.
func (r *Registry) resolveWorkerLocked(job *jobs.Job) (string, *URLWorker) {
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
		key = DomainKey(job.URL)
		rc = r.cfg.ForURL(job.URL)
	}

	w, ok := r.workers[key]
	if !ok {
		w = NewURLWorker(key, rc.RPS, rc.MaxConcurrent, pool, r.store, r.broker, r.l8, r.metrics, r.EnqueueWebhook, r.resultRecorder, func(job *jobs.Job) {
			if r.clusterRouter != nil {
				r.clusterRouter.CompleteLocalJob(job.UserID)
			}
			r.users.done(job)
		}, func(k string, idleWorker *URLWorker) {
			r.mu.Lock()
			if current := r.workers[k]; current == idleWorker {
				delete(r.workers, k)
			}
			r.mu.Unlock()
		})
		var remembered *bool
		if on, ok := r.destinationAccountQueue[key]; ok {
			remembered = &on
		}
		w.setAccountQueuePolicy(r.accountQueueDefault, remembered, func(on bool) {
			r.mu.Lock()
			r.destinationAccountQueue[key] = on
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
// target API already can, and delivery is durable across a restart the
// same way a real job is (the underlying webhook-delivery Job is persisted
// via CheckOrInsert, not just an in-memory retry loop).
//
// originalJobID scopes the idempotent key (see Job.IsWebhookDelivery) so
// a given job's webhook is enqueued at most once even if this were somehow
// called twice for it. maxRetries is the original job's webhook_max_retries.
//
// Delivery skips admission control: the job it reports on was already
// admitted and its target API call already made, so refusing the delivery
// would throw away finished work rather than shed new load.
func (r *Registry) EnqueueWebhook(originalJobID, userID, webhookURL string, maxRetries int, payload map[string]any) {
	if webhookURL == "" || r.stopping() {
		return
	}

	body, _ := json.Marshal(payload)
	job := jobs.New(&jobs.Request{
		UserID:        userID,
		IdempotentKey: "webhook:" + originalJobID,
		URL:           webhookURL,
		Method:        "POST",
		Headers:       map[string]string{"Content-Type": "application/json"},
		Body:          string(body),
		MaxRetries:    &maxRetries,
	})
	job.Kind = jobs.KindWebhookDelivery

	if _, duplicate := r.store.CheckOrInsert(job); duplicate || r.stopping() {
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

// applyDrainEvents tells the store whether to record drain events: only
// drain mode reads them, so with it off they're skipped.
func (r *Registry) applyDrainEvents() {
	if s, ok := r.store.(interface{ SetDrainEventsEnabled(bool) }); ok {
		s.SetDrainEventsEnabled(r.drainCfg.Enabled)
	}
}

// SetFullnessSource gives the registration ping the instance's fullness
// (see fullness.go). Wired by NewAquifer.
func (r *Registry) SetFullnessSource(source func() int) {
	if r != nil && source != nil {
		r.fullnessSource.Store(source)
	}
}

// SetLoadSource gives the registration ping the instance's load (see
// load.go). Wired by NewAquifer.
func (r *Registry) SetLoadSource(source func() Load) {
	if r != nil && source != nil {
		r.loadSource.Store(source)
	}
}

// AcceptedJobs is how many jobs and webhook deliveries this instance has
// accepted since start.
func (r *Registry) AcceptedJobs() int64 {
	if r == nil {
		return 0
	}
	return r.users.accepted.Load()
}

// ActiveUsers is how many users have work still owed on this instance.
func (r *Registry) ActiveUsers() int64 {
	if r == nil {
		return 0
	}
	return r.users.active()
}

// metrics.TargetStats is every target API's live queue state, for /metrics.
func (r *Registry) TargetStats() []metrics.TargetStats {
	r.mu.Lock()
	workers := make([]*URLWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	r.mu.Unlock()
	stats := make([]metrics.TargetStats, 0, len(workers))
	for _, worker := range workers {
		stats = append(stats, worker.Stats())
	}
	return stats
}

// idlePollInterval is how often WaitIdle checks for in-flight work.
const idlePollInterval = 25 * time.Millisecond

// BacklogFullnessRatio is the fullest target API's backlog against its own
// backlog budget. Per target API, not summed: one target API at its limit is
// already turning work away, however idle the others are.
func (r *Registry) BacklogFullnessRatio() float64 {
	r.mu.Lock()
	workers := make([]*URLWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	r.mu.Unlock()

	var ratio float64
	for _, worker := range workers {
		snapshot := worker.Snapshot()
		if snapshot.MaxBacklog > 0 {
			ratio = math.Max(ratio, float64(snapshot.UpstreamBacklog)/float64(snapshot.MaxBacklog))
		}
	}
	return ratio
}

// Metrics is the adapter this registry reports to.
func (r *Registry) Metrics() MetricsAdapter { return r.metrics }

// Worker is the worker for rawURL's target API, if one exists.
func (r *Registry) Worker(rawURL string) (*URLWorker, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[DomainKey(rawURL)]
	return w, ok
}

// Store is the job store this registry dispatches from.
func (r *Registry) Store() storage.Store { return r.store }
