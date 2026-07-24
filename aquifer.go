package aquifer

import "errors"

var ErrJobNotFound = errors.New("job not found")

type EnqueueResult struct {
	JobID     string `json:"job_id"`
	Status    Status `json:"status"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type Aquifer struct {
	store     *Store
	registry  *Registry
	broker    *Broker
	l8        *L8Registry
	admission *AdmissionController
}

func NewAquifer(store *Store, registry *Registry, broker *Broker, l8 *L8Registry, admission *AdmissionController) *Aquifer {
	return &Aquifer{store: store, registry: registry, broker: broker, l8: l8, admission: admission}
}

func (a *Aquifer) Enqueue(req JobRequest) (EnqueueResult, error) {
	if msg := req.Validate(); msg != "" {
		return EnqueueResult{}, errors.New(msg)
	}

	job := NewJob(&req)

	// Idempotency check comes first: a retried job that already exists must
	// still succeed even while the system is over an admission limit.
	if existingID, isDuplicate := a.store.CheckOrInsert(job); isDuplicate {
		return EnqueueResult{
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
			return EnqueueResult{}, &AdmissionRejectedError{Decision: decision}
		}
	}

	a.registry.Enqueue(job, req.AccountQueueMode)
	return EnqueueResult{JobID: job.ID, Status: StatusQueued}, nil
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
	return map[string]any{
		"status":        "ok",
		"l8_protocol":   "0.1",
		"l8_public_key": a.l8.PubB64,
		"admission":     a.AdmissionSnapshot(),
	}
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
