package main

import (
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu         sync.Mutex
	workers    map[string]*URLWorker
	store      *Store
	cfg        *Config
	broker     *Broker
	totalJobs  atomic.Int64
	queueDepth atomic.Int64
}

func NewRegistry(store *Store, cfg *Config, broker *Broker) *Registry {
	r := &Registry{
		workers: make(map[string]*URLWorker),
		store:   store,
		cfg:     cfg,
		broker:  broker,
	}
	counts := store.Counts()
	r.totalJobs.Store(counts.TotalJobs)
	r.queueDepth.Store(counts.QueueDepth)
	return r
}

func (r *Registry) Enqueue(job *Job) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalJobs.Add(1)
	r.queueDepth.Add(1)

	key := domainKey(job.URL)
	w, ok := r.workers[key]
	if !ok {
		rc := r.cfg.ForURL(job.URL)
		w = NewURLWorker(key, rc.RPS, rc.MaxConcurrent, r.store, r.broker, func(k string) {
			r.mu.Lock()
			delete(r.workers, k)
			r.mu.Unlock()
		})
		r.workers[key] = w
	}

	w.Enqueue(job)
}

func (r *Registry) JobDispatched() {
	r.queueDepth.Add(-1)
}

func (r *Registry) JobDone() {
	r.totalJobs.Add(-1)
}
