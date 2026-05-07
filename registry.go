package main

import (
	"sync"
)

type Registry struct {
	mu      sync.Mutex
	workers map[string]*URLWorker
	store   *Store
	cfg     *Config
}

func NewRegistry(store *Store, cfg *Config) *Registry {
	return &Registry{
		workers: make(map[string]*URLWorker),
		store:   store,
		cfg:     cfg,
	}
}

func (r *Registry) Enqueue(job *Job) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := domainKey(job.URL)
	w, ok := r.workers[key]
	if !ok {
		rc := r.cfg.ForURL(job.URL)
		w = NewURLWorker(key, rc.RPS, rc.MaxConcurrent, r.store, func(k string) {
			r.mu.Lock()
			delete(r.workers, k)
			r.mu.Unlock()
		})
		r.workers[key] = w
	}

	w.Enqueue(job)
}
