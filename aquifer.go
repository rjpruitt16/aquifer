// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/admission"
	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/region"
	"github.com/rjpruitt16/aquifer/l8"
)

var ErrJobNotFound = errors.New("job not found")
var ErrJobResultNotFound = errors.New("job result not found")

type EnqueueResult struct {
	JobID     string        `json:"job_id"`
	Status    Status        `json:"status"`
	Duplicate bool          `json:"duplicate,omitempty"`
	ResultKey string        `json:"result_key,omitempty"`
	Queue     QueueSnapshot `json:"-"`
	// With AQUIFER_READY_ESTIMATES, where a new job joined its queue and
	// about how long until its result is ready (absent without a rate yet).
	QueuePosition         int64    `json:"queue_position,omitempty"`
	EstimatedReadySeconds *float64 `json:"estimated_ready_seconds,omitempty"`
}

type Aquifer struct {
	store     JobStore
	registry  *Registry
	broker    *Broker
	l8        *L8Registry
	admission *AdmissionController
	pools     *PoolRegistry
	// fullnessSample caches Fullness() (see fullness.go), which goes on
	// every response.
	fullnessSample admission.Sampled
	// loadSampler backs Load() (see load.go).
	loadSampler loadSampler
	remote      RemoteIdempotency
	// regionAdapter backs /proxy's cross-region redirect (proxy.go). Left
	// nil by NewAquifer deliberately -- SetRegionAdapter is how it gets
	// wired in, so every existing NewAquifer caller (tests included) is
	// unaffected by this feature's existence. regionAdapterOrDefault()
	// falls back to NoopRegionAdapter, matching dispatch.EnsureMetrics' pattern.
	regionAdapter RegionAdapter
	// redirectGate answers "is attempting cross-region redirect itself
	// worth trying right now" -- see region_redirect.go. Always
	// initialized (not nil-checked elsewhere), since it's cheap and every
	// Aquifer instance can carry one regardless of whether redirect is
	// ever actually configured.
	redirectGate *region.Gate
	// redirectTargetURL builds the URL a redirect hop dials for a given
	// region. Set to the real .internal DNS builder by NewAquifer;
	// overridable in tests (which can't resolve real Fly private-network
	// DNS) to point at local httptest servers instead -- same
	// injectable-for-testability pattern as FlyRegionAdapter.healthCheckURL.
	redirectTargetURL func(region string) string
	clusterRouter     *ClusterRouter
	webSockets        *WebSocketManager
	draining          atomic.Bool
	inbound           *InboundRateController
	// newKeys holds an idempotency key from its job's insert until the job
	// is queued or deleted (see claimNewKey).
	newKeys keyClaims
	// afterInsert, when set, runs right after a new job's row is inserted.
	// Tests use it to hold a job between insert and admission.
	afterInsert func()
}

func NewAquifer(store JobStore, registry *Registry, broker *Broker, l8 *L8Registry, admission *AdmissionController, pools *PoolRegistry) *Aquifer {
	a := &Aquifer{
		store: store, registry: registry, broker: broker, l8: l8, admission: admission, pools: pools,
		redirectGate:      &region.Gate{},
		redirectTargetURL: region.DefaultTargetURL,
		inbound:           NewInboundRateController(),
	}
	if registry != nil {
		a.inbound.SetBacklogSource(func() int64 { return registry.QueueSnapshot().UpstreamBacklog })
		registry.SetFullnessSource(a.Fullness)
		registry.SetLoadSource(a.Load)
	}
	return a
}

func (a *Aquifer) Close() {
	if a == nil {
		return
	}
	a.inbound.Close()
	if closer, ok := a.regionAdapter.(interface{ Close() }); ok {
		closer.Close()
	}
	if a.webSockets != nil {
		a.webSockets.Close()
	}
	if a.registry != nil {
		a.registry.Close()
		if a.clusterRouter != nil {
			a.clusterRouter.Close()
		}
		return
	}
	if a.pools != nil {
		a.pools.Stop()
	}
	if a.l8 != nil {
		a.l8.Close()
	}
	if a.store != nil {
		a.store.Close()
	}
	if a.clusterRouter != nil {
		a.clusterRouter.Close()
	}
}

// SetRegionAdapter wires in a RegionAdapter after construction -- kept
// separate from NewAquifer's constructor so adding this opt-in feature
// doesn't change NewAquifer's signature for every existing caller. A nil
// Aquifer.regionAdapter (the default) behaves as NoopRegionAdapter via
// regionAdapterOrDefault.
func (a *Aquifer) SetRegionAdapter(adapter RegionAdapter) {
	a.regionAdapter = adapter
}

func (a *Aquifer) SetClusterRouter(router *ClusterRouter) {
	a.clusterRouter = router
	if a.registry != nil {
		a.registry.SetClusterRouter(router)
	}
}

func (a *Aquifer) SetRemoteIdempotency(remote RemoteIdempotency) {
	a.remote = remote
}

func (a *Aquifer) SetWebSocketManager(manager *WebSocketManager) {
	a.webSockets = manager
}

func (a *Aquifer) regionAdapterOrDefault() RegionAdapter {
	return region.Ensure(a.regionAdapter)
}

// RegisterPoolMember adds or refreshes (heartbeats) a member of a pool.
// The same call serves both roles — re-registering resets the member's
// liveness TTL and updates its declared capacity.
func (a *Aquifer) RegisterPoolMember(poolID, memberID, address string, declaredRPS float64, heartbeatIntervalSeconds int) error {
	if a.pools == nil {
		return errors.New("pool registry not configured")
	}
	if poolID == "" || memberID == "" || address == "" {
		return errors.New("pool_id, member_id, and address are required")
	}
	if declaredRPS <= 0 {
		return errors.New("capacity_rps must be greater than 0")
	}
	interval := time.Duration(heartbeatIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	a.pools.Register(poolID, memberID, address, declaredRPS, interval)
	return nil
}

func (a *Aquifer) Enqueue(req JobRequest) (EnqueueResult, error) {
	job, duplicate, release, err := a.prepareJob(req)
	defer release()
	if err != nil {
		return EnqueueResult{}, err
	}
	if duplicate != nil {
		return *duplicate, nil
	}

	queue, err := a.Dispatch(job, req.AccountQueueMode)
	if err != nil {
		return EnqueueResult{}, err
	}
	result := EnqueueResult{JobID: job.ID, Status: StatusQueued, Queue: queue}
	if est, ok := a.registry.EstimateNew(job); ok && est.Seconds >= 0 {
		seconds := math.Round(est.Seconds*10) / 10
		result.QueuePosition = est.Position
		result.EstimatedReadySeconds = &seconds
	}
	return result, nil
}

// PrepareJob validates, persists (idempotency-checked), and admission-checks
// a request without dispatching it — Enqueue's first two steps, exposed
// separately so a caller (proxy mode) can attempt a direct dispatch in
// between persistence and the durable-queue handoff. Returns a non-nil
// duplicate result if this idempotent_key already exists; job is nil in
// that case. Behavior is otherwise identical to Enqueue's first half.
func (a *Aquifer) PrepareJob(req JobRequest) (job *Job, duplicate *EnqueueResult, err error) {
	job, duplicate, release, err := a.prepareJob(req)
	release()
	return job, duplicate, err
}

// prepareJob is PrepareJob holding the job's idempotency key until the
// caller runs release, which it must always do. Enqueue holds it through
// Dispatch, whose rejection deletes the job too.
//
// Without the hold, a retry with the same key arriving between the insert
// and a rejection's DeleteJob was answered as a duplicate with the ID of a
// job about to be deleted: the caller had a job ID that 404s and never
// finishes. Canalis retries a hand-off that ran past its timeout, so under
// load (rejections for WAL sync lag or a full upstream queue) this lost
// accepted jobs in the Canalis soak.
func (a *Aquifer) prepareJob(req JobRequest) (job *Job, duplicate *EnqueueResult, release func(), err error) {
	release = func() {}
	if a.IsDraining() {
		return nil, nil, release, ErrAquiferDraining
	}
	if msg := req.Validate(); msg != "" {
		return nil, nil, release, errors.New(msg)
	}

	job = NewJob(&req)

	// A retry waits here while an earlier request for the same key is still
	// deciding, then sees its outcome: the job if it was queued, nothing if
	// it was rejected and deleted.
	release = a.newKeys.claim(job.IdempotencyHash())
	// The caller can't defer release until this returns; a panic before
	// then would hold the key forever and block every retry for it. A job
	// already inserted is deleted first, so a retry doesn't find a row
	// that will never be dispatched.
	inserted := false
	defer func() {
		if r := recover(); r != nil {
			if inserted {
				a.store.DeleteJob(job.ID)
			}
			release()
			panic(r)
		}
	}()

	// Idempotency check comes first: a retried job that already exists must
	// still succeed even while the system is over an admission limit.
	insertStarted := time.Now()
	existingID, isDuplicate := a.store.CheckOrInsert(job)
	inserted = !isDuplicate
	a.inbound.Observe(req.UserID, time.Since(insertStarted), !isDuplicate)
	if !isDuplicate && a.afterInsert != nil {
		a.afterInsert()
	}
	if isDuplicate {
		return nil, &EnqueueResult{
			JobID:     existingID,
			Status:    StatusQueued,
			Duplicate: true,
		}, release, nil
	}

	if a.remote != nil {
		entry, found := a.remote.Lookup(job.IdempotencyHash())
		if found {
			a.store.DeleteJob(job.ID)
			return nil, &EnqueueResult{
				JobID:     entry.JobID,
				Status:    entry.Status,
				Duplicate: true,
				ResultKey: entry.ResultKey,
			}, release, nil
		}
	}

	// CheckOrInsert already wrote this job's row since it wasn't a duplicate.
	// If schema validation or admission rejects it now, that row must be
	// deleted or it becomes a ghost "queued" entry that never dispatches.
	if job.URL != "" {
		if err := a.l8.ValidateRequestBody(job.URL, job.Method, job.Body); err != nil {
			a.store.DeleteJob(job.ID)
			return nil, nil, release, err
		}
	}
	if a.admission != nil {
		if decision := a.admission.Check(); !decision.Allowed {
			a.store.DeleteJob(job.ID)
			return nil, nil, release, &AdmissionRejectedError{Decision: decision}
		}
	}

	return job, nil, release, nil
}

// Dispatch hands an already-persisted, admission-approved job to the
// durable paced queue — Enqueue's last step, exposed for a caller (proxy
// mode) that already ran PrepareJob itself.
func (a *Aquifer) Dispatch(job *Job, accountQueueHeader string) (QueueSnapshot, error) {
	snapshot, err := a.registry.AdmitAndEnqueue(job, accountQueueHeader)
	if err != nil {
		a.store.DeleteJob(job.ID)
		return snapshot, err
	}
	return snapshot, nil
}

// AdmissionSnapshot reports current admission pressure for /health. Returns
// enabled:false if admission control isn't configured.
func (a *Aquifer) AdmissionSnapshot() map[string]any {
	if a.admission == nil {
		return map[string]any{"enabled": false}
	}
	snap := a.admission.Snapshot()
	snap["enabled"] = a.admission.AnyLimitConfigured()
	return snap
}

// MaxBodyBytes returns the configured request body ceiling, or 0 if
// unconfigured (unlimited).
func (a *Aquifer) MaxBodyBytes() int64 {
	if a.admission == nil {
		return 0
	}
	return a.admission.MaxBodyBytes()
}

// RetryAfterSeconds returns the configured Retry-After value for 429
// responses, defaulting to 5 seconds if admission control isn't configured.
func (a *Aquifer) RetryAfterSeconds() int {
	if a.admission == nil {
		return 5
	}
	return a.admission.RetryAfterSeconds()
}

func (a *Aquifer) GetJob(id string) (*Job, error) {
	job := a.store.GetJob(id)
	if job == nil {
		return nil, ErrJobNotFound
	}
	return job, nil
}

func (a *Aquifer) GetJobResult(userID, idempotentKey, scope string) (JobResult, error) {
	switch {
	case idempotentKey == "":
		return JobResult{}, errors.New("idempotent_key is required")
	case scope == IdempotencyScopeShared && !jobs.SharedIdempotencyEnabled():
		return JobResult{}, errors.New(`idempotency_scope "shared" requires AQUIFER_SHARED_IDEMPOTENCY_ENABLED=true`)
	case scope != IdempotencyScopeShared && userID == "":
		return JobResult{}, errors.New("user_id is required unless idempotency_scope is shared")
	}
	reader, ok := a.remote.(JobResultReader)
	if !ok {
		return JobResult{}, ErrJobResultNotFound
	}
	if result, found := reader.LookupResult(jobs.DedupHash(userID, idempotentKey, scope)); found {
		return result, nil
	}
	return JobResult{}, ErrJobResultNotFound
}

func (a *Aquifer) SubscribeJob(id string) (*Job, <-chan SSEEvent, func(), error) {
	job, err := a.GetJob(id)
	if err != nil {
		return nil, nil, nil, err
	}

	events, unsubscribe := a.broker.Subscribe(id)
	return job, events, unsubscribe, nil
}

func (a *Aquifer) Health() map[string]any {
	status := "ok"
	if a.IsDraining() {
		status = "draining"
	}
	h := map[string]any{
		"status":        status,
		"l8_protocol":   l8.Version,
		"l8_public_key": a.l8.PubB64,
		"admission":     a.AdmissionSnapshot(),
		"queues":        a.registry.QueueSnapshot(),
	}
	if a.pools != nil {
		h["pools"] = a.pools.Snapshot()
	}
	if drain := a.registry.DrainSnapshot(); drain != nil {
		h["drain"] = drain
	}
	if inbound := a.inbound.Snapshot(); inbound != nil {
		h["inbound"] = inbound
	}
	if cluster := a.clusterRouter.Snapshot(); cluster != nil {
		h["cluster"] = cluster
	}
	if sockets := a.webSockets.Snapshot(); sockets != nil {
		h["websocket"] = sockets
	}
	return h
}

func (a *Aquifer) L8Metadata(host string) L8Meta {
	if host == "" {
		host = "localhost"
	}
	return a.l8.Meta(host)
}

func (a *Aquifer) HandleL8Challenge(req L8ChallengeReq) (*L8ChallengeResp, error) {
	return a.l8.HandleChallenge(req)
}

// JobResult returns a finished job's stored response, so a caller who
// streamed instead of receiving a webhook can still fetch it.
func (a *Aquifer) JobResult(jobID string) (JobResult, bool) {
	return a.store.GetResult(jobID)
}

// ConfirmDelivered tells Aquifer a subscriber handed a job's final event to
// its client, so the completion webhook is skipped. Every subscriber that
// delivers results (SSE, /proxy, adapters) must call it, or the job waits
// AQUIFER_STREAM_DELIVERY_WAIT_MS and then sends the webhook anyway.
func (a *Aquifer) ConfirmDelivered(jobID string) {
	a.broker.ConfirmDelivered(jobID)
}

// InboundRPS is the X-Aqueduct-Rps to advertise to userID, or 0 when inbound
// pacing is disabled.
func (a *Aquifer) InboundRPS(userID string) float64 {
	return a.inbound.AdvertisedRPS(userID)
}

// keyClaims is a lock per key, held by at most one caller at a time.
type keyClaims struct {
	mu   sync.Mutex
	held map[string]chan struct{}
}

// claim waits until key is free, takes it, and returns its release.
func (k *keyClaims) claim(key string) func() {
	for {
		k.mu.Lock()
		done, busy := k.held[key]
		if !busy {
			if k.held == nil {
				k.held = make(map[string]chan struct{})
			}
			done = make(chan struct{})
			k.held[key] = done
			k.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					k.mu.Lock()
					delete(k.held, key)
					k.mu.Unlock()
					close(done)
				})
			}
		}
		k.mu.Unlock()
		<-done
	}
}
