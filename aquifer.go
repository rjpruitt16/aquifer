package main

import "errors"

var ErrJobNotFound = errors.New("job not found")

type EnqueueResult struct {
	JobID     string `json:"job_id"`
	Status    Status `json:"status"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

type Aquifer struct {
	store    *Store
	registry *Registry
	broker   *Broker
	l8       *L8Registry
}

func NewAquifer(store *Store, registry *Registry, broker *Broker, l8 *L8Registry) *Aquifer {
	return &Aquifer{store: store, registry: registry, broker: broker, l8: l8}
}

func (a *Aquifer) Enqueue(req JobRequest) (EnqueueResult, error) {
	if msg := req.Validate(); msg != "" {
		return EnqueueResult{}, errors.New(msg)
	}

	job := NewJob(&req)
	if existingID, isDuplicate := a.store.CheckOrInsert(job); isDuplicate {
		return EnqueueResult{
			JobID:     existingID,
			Status:    StatusQueued,
			Duplicate: true,
		}, nil
	}

	a.registry.Enqueue(job)
	return EnqueueResult{JobID: job.ID, Status: StatusQueued}, nil
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
