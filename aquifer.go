package aquifer

import (
	"errors"
	"time"
)

var ErrJobNotFound = errors.New("job not found")

type EnqueueResult struct {
	JobID     string `json:"job_id"`
	Status    Status `json:"status"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type Aquifer struct {
	store     JobStore
	registry  *Registry
	broker    *Broker
	l8        *L8Registry
	admission *AdmissionController
	pools     *PoolRegistry
	// regionAdapter backs /proxy's cross-region redirect (proxy.go). Left
	// nil by NewAquifer deliberately -- SetRegionAdapter is how it gets
	// wired in, so every existing NewAquifer caller (tests included) is
	// unaffected by this feature's existence. regionAdapterOrDefault()
	// falls back to NoopRegionAdapter, matching ensureMetrics' pattern.
	regionAdapter RegionAdapter
	// redirectGate answers "is attempting cross-region redirect itself
	// worth trying right now" -- see region_redirect.go. Always
	// initialized (not nil-checked elsewhere), since it's cheap and every
	// Aquifer instance can carry one regardless of whether redirect is
	// ever actually configured.
	redirectGate *redirectGate
	// redirectTargetURL builds the URL a redirect hop dials for a given
	// region. Set to the real .internal DNS builder by NewAquifer;
	// overridable in tests (which can't resolve real Fly private-network
	// DNS) to point at local httptest servers instead -- same
	// injectable-for-testability pattern as FlyRegionAdapter.healthCheckURL.
	redirectTargetURL func(region string) string
}

func NewAquifer(store JobStore, registry *Registry, broker *Broker, l8 *L8Registry, admission *AdmissionController, pools *PoolRegistry) *Aquifer {
	return &Aquifer{
		store: store, registry: registry, broker: broker, l8: l8, admission: admission, pools: pools,
		redirectGate:      &redirectGate{},
		redirectTargetURL: defaultRedirectTargetURL,
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

func (a *Aquifer) regionAdapterOrDefault() RegionAdapter {
	return ensureRegionAdapter(a.regionAdapter)
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
	job, duplicate, err := a.PrepareJob(req)
	if err != nil {
		return EnqueueResult{}, err
	}
	if duplicate != nil {
		return *duplicate, nil
	}

	a.Dispatch(job, req.AccountQueueMode)
	return EnqueueResult{JobID: job.ID, Status: StatusQueued}, nil
}

// PrepareJob validates, persists (idempotency-checked), and admission-checks
// a request without dispatching it — Enqueue's first two steps, exposed
// separately so a caller (proxy mode) can attempt a direct dispatch in
// between persistence and the durable-queue handoff. Returns a non-nil
// duplicate result if this idempotent_key already exists; job is nil in
// that case. Behavior is otherwise identical to Enqueue's first half.
func (a *Aquifer) PrepareJob(req JobRequest) (job *Job, duplicate *EnqueueResult, err error) {
	if msg := req.Validate(); msg != "" {
		return nil, nil, errors.New(msg)
	}

	job = NewJob(&req)

	// Idempotency check comes first: a retried job that already exists must
	// still succeed even while the system is over an admission limit.
	if existingID, isDuplicate := a.store.CheckOrInsert(job); isDuplicate {
		return nil, &EnqueueResult{
			JobID:     existingID,
			Status:    StatusQueued,
			Duplicate: true,
		}, nil
	}

	// CheckOrInsert already wrote this job's row since it wasn't a duplicate.
	// If admission rejects it now, that row must be deleted or it becomes a
	// ghost "queued" entry that never dispatches.
	if a.admission != nil {
		if decision := a.admission.Check(); !decision.Allowed {
			a.store.DeleteJob(job.ID)
			return nil, nil, &AdmissionRejectedError{Decision: decision}
		}
	}

	return job, nil, nil
}

// Dispatch hands an already-persisted, admission-approved job to the
// durable paced queue — Enqueue's last step, exposed for a caller (proxy
// mode) that already ran PrepareJob itself.
func (a *Aquifer) Dispatch(job *Job, accountQueueHeader string) {
	a.registry.Enqueue(job, accountQueueHeader)
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

func (a *Aquifer) SubscribeJob(id string) (*Job, <-chan SSEEvent, func(), error) {
	job, err := a.GetJob(id)
	if err != nil {
		return nil, nil, nil, err
	}

	events, unsubscribe := a.broker.Subscribe(id)
	return job, events, unsubscribe, nil
}

func (a *Aquifer) Health() map[string]any {
	h := map[string]any{
		"status":        "ok",
		"l8_protocol":   "0.1",
		"l8_public_key": a.l8.PubB64,
		"admission":     a.AdmissionSnapshot(),
	}
	if a.pools != nil {
		h["pools"] = a.pools.Snapshot()
	}
	if drain := a.registry.DrainSnapshot(); drain != nil {
		h["drain"] = drain
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
