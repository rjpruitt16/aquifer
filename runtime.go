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
	// RegionAdapter backs /proxy's cross-region redirect (proxy.go,
	// region_adapter.go). If nil, NewRuntime tries NewFlyRegionAdapter,
	// which itself only activates if AQUIFER_FLY_REGIONS is set — same
	// zero-code, env-var-only activation pattern as AQUIFER_DRAIN_ENABLED.
	// Set this to plug in a RegionAdapter for a different platform.
	RegionAdapter RegionAdapter
	// ClusterRouter optionally routes HTTP /jobs and /proxy requests to the
	// Aquifer node that owns the request's partition. If nil, NewRuntime
	// loads static env config from AQUIFER_CLUSTER_*.
	ClusterRouter *ClusterRouter
	// RemoteIdempotency optionally checks a shared remote ledger after the
	// local store accepts a new key but before the job is dispatched. If
	// nil, NewRuntime loads AQUIFER_REMOTE_IDEMPOTENCY_* env config.
	RemoteIdempotency RemoteIdempotency
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
	remote := opts.RemoteIdempotency
	if remote == nil {
		remote = NewValkeyRemoteIdempotency(LoadRemoteIdempotencyConfig())
	}
	registry.SetDrainRemote(remote)
	admission := NewAdmissionController(*admissionLimits, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission, pools)
	if remote != nil {
		app.SetRemoteIdempotency(remote)
	}

	regionAdapter := opts.RegionAdapter
	if regionAdapter == nil {
		if fly := NewFlyRegionAdapter(); fly != nil {
			regionAdapter = fly
		}
	}
	if regionAdapter != nil {
		app.SetRegionAdapter(regionAdapter)
	}

	clusterRouter := opts.ClusterRouter
	if clusterRouter == nil {
		clusterRouter = NewClusterRouter(LoadClusterConfig())
	}
	if clusterRouter != nil {
		app.SetClusterRouter(clusterRouter)
	}

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
	defer runtime.Close()
	runtime.RecoverQueuedJobs(runtime.DBPath())
	return adapter.Start(ctx, runtime.Aquifer)
}

func (r *Runtime) DBPath() string {
	if r.Store == nil {
		return ""
	}
	return r.Store.Path()
}

func (r *Runtime) Close() {
	if r == nil {
		return
	}
	if r.Aquifer != nil {
		r.Aquifer.Close()
		return
	}
	if r.Registry != nil {
		r.Registry.Close()
	}
}
