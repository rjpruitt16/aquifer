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
	// Store overrides the storage backend entirely. If nil, NewRuntime falls
	// back to NewJobStore(DBPath), selecting sqlite/pebble via
	// AQUIFER_STORE_BACKEND as before. Set this to plug in a custom JobStore
	// implementation (e.g. Postgres, rqlite) without needing to bypass
	// NewRuntime and wire the lower-level constructors by hand.
	Store JobStore
	// DrainConfig overrides drain mode's config (see drain.go). If nil,
	// the Registry reads AQUIFER_DRAIN_* env vars itself — disabled unless
	// AQUIFER_DRAIN_ENABLED is explicitly set to true.
	DrainConfig *DrainConfig
}

type Runtime struct {
	Aquifer   *Aquifer
	Store     JobStore
	Broker    *Broker
	Registry  *Registry
	L8        *L8Registry
	Config    *Config
	Admission *AdmissionController
	Pools     *PoolRegistry
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
	store := opts.Store
	if store == nil {
		store = NewJobStore(dbPath)
	}
	broker := NewBroker()
	metrics := ensureMetrics(opts.Metrics)
	pools := NewPoolRegistry()
	registry := NewRegistry(store, cfg, broker, l8, metrics, pools)
	if opts.DrainConfig != nil {
		registry.ConfigureDrain(*opts.DrainConfig)
	}
	admission := NewAdmissionController(*admissionLimits, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission, pools)

	return &Runtime{
		Aquifer:   app,
		Store:     store,
		Broker:    broker,
		Registry:  registry,
		L8:        l8,
		Config:    cfg,
		Admission: admission,
		Pools:     pools,
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
