package aquifer

import (
	"context"
	"log"
)

type RuntimeOptions struct {
	DBPath          string
	ConfigPath      string
	Config          *Config
	L8KeyPath       string
	L8TrustDir      string
	Metrics         MetricsAdapter
	AdmissionLimits *AdmissionLimits
}

type Runtime struct {
	Aquifer   *Aquifer
	Store     *Store
	Broker    *Broker
	Registry  *Registry
	L8        *L8Registry
	Config    *Config
	Admission *AdmissionController
}

func NewRuntime(opts RuntimeOptions) *Runtime {
	dbPath := opts.DBPath
	if dbPath == "" {
		dbPath = "aquifer.db"
	}

	cfg := opts.Config
	if cfg == nil {
		cfg = LoadConfig(opts.ConfigPath)
	}

	l8KeyPath := opts.L8KeyPath
	if l8KeyPath == "" {
		l8KeyPath = ".l8-key"
	}

	l8TrustDir := opts.L8TrustDir
	if l8TrustDir == "" {
		l8TrustDir = "l8-trust"
	}

	admissionLimits := opts.AdmissionLimits
	if admissionLimits == nil {
		loaded := LoadAdmissionLimits()
		admissionLimits = &loaded
	}

	l8 := NewL8Registry(l8KeyPath, l8TrustDir)
	store := NewStore(dbPath)
	broker := NewBroker()
	metrics := ensureMetrics(opts.Metrics)
	registry := NewRegistry(store, cfg, broker, l8, metrics)
	admission := NewAdmissionController(*admissionLimits, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission)

	return &Runtime{
		Aquifer:   app,
		Store:     store,
		Broker:    broker,
		Registry:  registry,
		L8:        l8,
		Config:    cfg,
		Admission: admission,
	}
}

func (r *Runtime) RecoverQueuedJobs(dbPath string) {
	queued := r.Store.GetQueuedJobs()
	if len(queued) == 0 {
		return
	}

	log.Printf("recovering %d queued jobs from %s", len(queued), dbPath)
	for _, job := range queued {
		// No live HTTP request behind a recovered job, so no account-queue
		// opinion — "" leaves each upstream's mode exactly as it was.
		r.Registry.Enqueue(job, "")
	}
}

func RunAdapter(ctx context.Context, adapter FrameworkAdapter, opts RuntimeOptions) error {
	runtime := NewRuntime(opts)
	runtime.RecoverQueuedJobs(runtime.DBPath())
	return adapter.Start(ctx, runtime.Aquifer)
}

func (r *Runtime) DBPath() string {
	if r.Store == nil {
		return ""
	}
	return r.Store.Path()
}
