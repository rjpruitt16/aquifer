// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package core

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/rjpruitt16/aquifer/internal/dispatch"
	"github.com/rjpruitt16/aquifer/internal/jobs"
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
	// DrainConfig overrides drain mode's config (see internal/dispatch/drain.go). If nil,
	// the Registry reads AQUIFER_DRAIN_* env vars itself — disabled unless
	// AQUIFER_DRAIN_ENABLED is explicitly set to true.
	DrainConfig *DrainConfig
	// RegionAdapter backs /proxy's cross-region redirect (proxy.go,
	// internal/region/adapter.go). If nil, NewRuntime tries NewFlyRegionAdapter,
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
	// WebSocketConfig and WebSocketStore allow embedding applications and
	// tests to configure the durable WebSocket proxy without environment
	// variables. Nil values load AQUIFER_WS_* and AQUIFER_VALKEY_URL.
	WebSocketConfig *WebSocketConfig
	WebSocketStore  WebSocketStreamStore
	// ShutdownConfig controls SIGTERM quiescing, WebSocket handoff, and the
	// maximum time accepted work may continue draining. Nil loads
	// AQUIFER_SHUTDOWN_* and AQUIFER_WS_DRAIN_GRACE_SECONDS.
	ShutdownConfig *ShutdownConfig
}

type Runtime struct {
	Aquifer    *Aquifer
	Store      JobStore
	Broker     *Broker
	Registry   *Registry
	L8         *L8Registry
	Config     *Config
	Admission  *AdmissionController
	Pools      *PoolRegistry
	WebSockets *WebSocketManager
	// WebSocketExport is set when AQUIFER_WS_EXPORT_ENABLED=true.
	WebSocketExport *WebSocketExporter
	Shutdown        ShutdownConfig
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
	metrics := dispatch.EnsureMetrics(opts.Metrics)
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
	admission.WatchSyncLag(store)
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

	webSocketCfg := opts.WebSocketConfig
	if webSocketCfg == nil {
		loaded := LoadWebSocketConfig()
		webSocketCfg = &loaded
	}
	shutdownCfg := LoadShutdownConfig()
	if opts.ShutdownConfig != nil {
		shutdownCfg = *opts.ShutdownConfig
	}
	var webSocketManager *WebSocketManager
	var webSocketExporter *WebSocketExporter
	if webSocketCfg.Enabled {
		streamStore := opts.WebSocketStore
		if streamStore == nil {
			var err error
			streamStore, err = NewRedisWebSocketStreamStore(webSocketCfg.RedisURL, webSocketCfg.StreamPrefix, webSocketCfg.StreamMaxEvents, webSocketCfg.StreamTTL, webSocketCfg.ReaderShards, webSocketCfg.StorePoolSize)
			if err != nil {
				log.Printf("websocket: disabled: %v", err)
			}
		}
		if streamStore != nil {
			if exportCfg := LoadWebSocketExportConfig(); exportCfg.Enabled {
				if redisStore, ok := streamStore.(*RedisWebSocketStreamStore); ok {
					exporter, err := StartWebSocketExport(redisStore, exportCfg)
					if err != nil {
						log.Printf("websocket export: disabled: %v", err)
					} else {
						webSocketExporter = exporter
					}
				}
			}
			webSocketManager = NewWebSocketManager(*webSocketCfg, streamStore)
			webSocketManager.SetMemoryCheck(admission.Memory)
			app.SetWebSocketManager(webSocketManager)
		}
	}

	return &Runtime{
		Aquifer:    app,
		Store:      store,
		Broker:     broker,
		Registry:   registry,
		L8:         l8,
		Config:     cfg,
		Admission:  admission,
		Pools:      pools,
		WebSockets: webSocketManager,
		Shutdown:   shutdownCfg,

		WebSocketExport: webSocketExporter,
	}
}

// RecoverQueuedJobs puts the jobs left unfinished by the last run back in
// their queues before Aquifer serves anything. Each queue gets its first
// page (AQUIFER_RECOVERY_PAGE_SIZE); the rest stay on disk, counted in its
// backlog, and load as it drains, so restart memory doesn't grow with the
// backlog (issue #88).
//
// Jobs under a key from before this existed (no "target|"), never handed to
// a queue, or under a key whose jobs now route to a different queue (the
// target switched between one shared queue and a queue per tenant) load all
// at once, as they always did, first. Their hand-off rewrites their key, so
// the keys are counted again before the rest are paged.
//
// A store read that fails is retried; if it keeps failing, recovery returns
// the error instead of serving with jobs left behind.
func (r *Runtime) RecoverQueuedJobs(dbPath string) error {
	counts, err := retryRead(func() (map[string]int, error) { return r.Store.PendingQueueKeys() })
	if err != nil {
		return fmt.Errorf("recovery: count queued jobs: %w", err)
	}
	total := 0
	for _, n := range counts {
		total += n
	}
	if total == 0 {
		return nil
	}
	log.Printf("recovering %d queued jobs in %d queues from %s", total, len(counts), dbPath)

	size := dispatch.RecoveryPageSize()
	cutoff := time.Now().UnixMilli() + 1
	seen := map[string]bool{}
	// No live HTTP request behind a recovered job, so no account-queue
	// opinion: "" leaves each target API's mode exactly as it was.
	enqueue := func(job *Job) {
		if !seen[job.ID] {
			seen[job.ID] = true
			r.Registry.Enqueue(job, "")
		}
	}
	pageable := func(key string) (bool, error) {
		if !strings.Contains(key, "|") {
			return false, nil
		}
		first, err := retryRead(func() ([]*Job, error) { return r.Store.QueuedPage(key, jobs.PageCursor{}, 0, 1) })
		if err != nil || len(first) == 0 {
			return true, err
		}
		return r.Registry.StoreKeyFor(first[0]) == key, nil
	}

	// Load everything that can't be paged, the old way.
	moved := false
	for key := range counts {
		ok, err := pageable(key)
		if err != nil {
			return fmt.Errorf("recovery: read %q: %w", key, err)
		}
		if ok {
			continue
		}
		moved = true
		if !strings.Contains(key, "|") {
			for _, job := range r.Store.RecoverQueued(key) {
				enqueue(job)
			}
			continue
		}
		if err := r.pageAll(key, jobs.PageCursor{}, size, enqueue); err != nil {
			return err
		}
	}
	if moved {
		if counts, err = retryRead(func() (map[string]int, error) { return r.Store.PendingQueueKeys() }); err != nil {
			return fmt.Errorf("recovery: count queued jobs: %w", err)
		}
	}

	// Page the rest: one page now, the remainder on disk.
	onDisk := 0
	for key, n := range counts {
		if ok, err := pageable(key); err != nil {
			return fmt.Errorf("recovery: read %q: %w", key, err)
		} else if !ok {
			continue
		}
		page, err := retryRead(func() ([]*Job, error) { return r.Store.QueuedPage(key, jobs.PageCursor{}, 0, size) })
		if err != nil {
			return fmt.Errorf("recovery: read %q: %w", key, err)
		}
		for _, job := range page {
			enqueue(job)
		}
		rest := n - len(page)
		if len(page) < size || rest <= 0 {
			continue
		}
		last := page[len(page)-1]
		if r.Registry.AttachRecoveredBacklog(last, key, jobs.CursorAt(last), rest, cutoff) {
			onDisk += rest
			continue
		}
		// No queue to page into: load the rest now, as before.
		if err := r.pageAll(key, jobs.CursorAt(last), size, enqueue); err != nil {
			return err
		}
	}
	if onDisk > 0 {
		log.Printf("recovery: %d jobs stay on disk and load as their queues drain", onDisk)
	}
	return nil
}

// pageAll enqueues every job stored under key after the cursor.
func (r *Runtime) pageAll(key string, after jobs.PageCursor, size int, enqueue func(*Job)) error {
	for {
		page, err := retryRead(func() ([]*Job, error) { return r.Store.QueuedPage(key, after, 0, size) })
		if err != nil {
			return fmt.Errorf("recovery: read %q: %w", key, err)
		}
		for _, job := range page {
			enqueue(job)
		}
		if len(page) < size {
			return nil
		}
		after = jobs.CursorAt(page[len(page)-1])
	}
}

// retryRead tries a store read up to five times, backing off from 100 ms.
func retryRead[T any](read func() (T, error)) (T, error) {
	var (
		v   T
		err error
	)
	for attempt, wait := 0, 100*time.Millisecond; attempt < 5; attempt, wait = attempt+1, wait*2 {
		if v, err = read(); err == nil {
			return v, nil
		}
		log.Printf("recovery: store read failed (attempt %d of 5): %v", attempt+1, err)
		time.Sleep(wait)
	}
	return v, err
}

func RunAdapter(ctx context.Context, adapter FrameworkAdapter, opts RuntimeOptions) error {
	runtime := NewRuntime(opts)
	defer runtime.Close()
	if err := runtime.RecoverQueuedJobs(runtime.DBPath()); err != nil {
		return err
	}

	adapterCtx, stopAdapter := context.WithCancel(context.Background())
	defer stopAdapter()
	adapterDone := make(chan error, 1)
	go func() {
		adapterDone <- adapter.Start(adapterCtx, runtime.Aquifer)
	}()

	select {
	case err := <-adapterDone:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), runtime.Shutdown.Timeout)
	defer cancelShutdown()
	runtime.BeginDrain()
	if !dispatch.SleepBeforeRetryContext(shutdownCtx, runtime.Shutdown.Quiesce) {
		log.Printf("shutdown: quiesce window ended at deadline")
	}
	stopAdapter()

	if err := runtime.Drain(shutdownCtx); err != nil {
		log.Printf("shutdown: graceful drain deadline reached: %v", err)
	}
	select {
	case err := <-adapterDone:
		return err
	case <-shutdownCtx.Done():
		return nil
	}
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
	// Unacknowledged export messages stay in Valkey for the next start.
	r.WebSocketExport.Stop()
	if r.Aquifer != nil {
		r.Aquifer.Close()
		return
	}
	if r.Registry != nil {
		r.Registry.Close()
	}
}
