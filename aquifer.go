// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package aquifer is the public Go API for embedding Aquifer: a durable queue
// that paces traffic into a service at the rate it can take.
//
// The implementation lives in internal/core and the other internal/ packages.
// This file re-exports the public names, so code that imports
// github.com/rjpruitt16/aquifer keeps working unchanged. When you add an
// exported name to internal/core, add it here too (a type alias, a constant, or
// a wrapper function); TestRootPackageReExportsEveryExportedName in
// internal/core fails until you do.
package aquifer

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer/internal/core"
)

type (
	AccountQueue               = core.AccountQueue
	AdmissionController        = core.AdmissionController
	AdmissionDecision          = core.AdmissionDecision
	AdmissionLimits            = core.AdmissionLimits
	AdmissionMetrics           = core.AdmissionMetrics
	AdmissionRejectedError     = core.AdmissionRejectedError
	Aquifer                    = core.Aquifer
	Broker                     = core.Broker
	ClusterConfig              = core.ClusterConfig
	ClusterMember              = core.ClusterMember
	ClusterRouter              = core.ClusterRouter
	ClusterStateStore          = core.ClusterStateStore
	Config                     = core.Config
	DispatchMetrics            = core.DispatchMetrics
	DrainConfig                = core.DrainConfig
	DrainEvent                 = core.DrainEvent
	DrainState                 = core.DrainState
	EnqueueResult              = core.EnqueueResult
	FairAdmissionDecision      = core.FairAdmissionDecision
	FlyRegionAdapter           = core.FlyRegionAdapter
	FrameworkAdapter           = core.FrameworkAdapter
	HTTPAdapter                = core.HTTPAdapter
	InboundRateController      = core.InboundRateController
	InboundRateSnapshot        = core.InboundRateSnapshot
	Job                        = core.Job
	JobRequest                 = core.JobRequest
	JobResult                  = core.JobResult
	JobResultReader            = core.JobResultReader
	JobResultRecorder          = core.JobResultRecorder
	JobStore                   = core.JobStore
	JobTimingMetrics           = core.JobTimingMetrics
	L8ChallengeReq             = core.L8ChallengeReq
	L8ChallengeResp            = core.L8ChallengeResp
	L8Meta                     = core.L8Meta
	L8Registry                 = core.L8Registry
	LedgerEntry                = core.LedgerEntry
	LifecycleState             = core.LifecycleState
	Load                       = core.Load
	MetricsAdapter             = core.MetricsAdapter
	NoopMetricsAdapter         = core.NoopMetricsAdapter
	NoopRegionAdapter          = core.NoopRegionAdapter
	PebbleStore                = core.PebbleStore
	Pool                       = core.Pool
	PoolMember                 = core.PoolMember
	PoolRegistry               = core.PoolRegistry
	PrometheusMetrics          = core.PrometheusMetrics
	ProxyFallbackInfo          = core.ProxyFallbackInfo
	ProxyOutcome               = core.ProxyOutcome
	QueueSnapshot              = core.QueueSnapshot
	QueueStat                  = core.QueueStat
	RateConfig                 = core.RateConfig
	RedirectExhaustedError     = core.RedirectExhaustedError
	RedisWebSocketStreamStore  = core.RedisWebSocketStreamStore
	RegionAdapter              = core.RegionAdapter
	RegistrationConfig         = core.RegistrationConfig
	Registry                   = core.Registry
	RemoteIdempotency          = core.RemoteIdempotency
	RemoteIdempotencyConfig    = core.RemoteIdempotencyConfig
	RemoteIdempotencyEntry     = core.RemoteIdempotencyEntry
	Runtime                    = core.Runtime
	RuntimeOptions             = core.RuntimeOptions
	SSEEvent                   = core.SSEEvent
	SchemaMismatchError        = core.SchemaMismatchError
	Server                     = core.Server
	ShutdownConfig             = core.ShutdownConfig
	Status                     = core.Status
	Store                      = core.Store
	StoreCounts                = core.StoreCounts
	TargetStats                = core.TargetStats
	URLWorker                  = core.URLWorker
	ValkeyClusterStateStore    = core.ValkeyClusterStateStore
	ValkeyRemoteIdempotency    = core.ValkeyRemoteIdempotency
	WebSocketAdmission         = core.WebSocketAdmission
	WebSocketConfig            = core.WebSocketConfig
	WebSocketEnvelope          = core.WebSocketEnvelope
	WebSocketExportBatch       = core.WebSocketExportBatch
	WebSocketExportConfig      = core.WebSocketExportConfig
	WebSocketExportEvent       = core.WebSocketExportEvent
	WebSocketExporter          = core.WebSocketExporter
	WebSocketFollow            = core.WebSocketFollow
	WebSocketManager           = core.WebSocketManager
	WebSocketScheduler         = core.WebSocketScheduler
	WebSocketSchedulerConfig   = core.WebSocketSchedulerConfig
	WebSocketSchedulerSnapshot = core.WebSocketSchedulerSnapshot
	WebSocketStreamEvent       = core.WebSocketStreamEvent
	WebSocketStreamStore       = core.WebSocketStreamStore
	WebhookEnqueuer            = core.WebhookEnqueuer
	WebhookJobMetrics          = core.WebhookJobMetrics
)

const (
	DefaultMaxRetries        = core.DefaultMaxRetries
	DefaultWebhookMaxRetries = core.DefaultWebhookMaxRetries
	DrainStateActive         = core.DrainStateActive
	DrainStateDraining       = core.DrainStateDraining
	DrainStateUnassigned     = core.DrainStateUnassigned
	IdempotencyScopeShared   = core.IdempotencyScopeShared
	IdempotencyScopeUser     = core.IdempotencyScopeUser
	LifecycleStateActive     = core.LifecycleStateActive
	LifecycleStateDraining   = core.LifecycleStateDraining
	LifecycleStateOffline    = core.LifecycleStateOffline
	MaxAllowedRetries        = core.MaxAllowedRetries
	RetryUntilComplete       = core.RetryUntilComplete
	StatusCompleted          = core.StatusCompleted
	StatusFailed             = core.StatusFailed
	StatusInFlight           = core.StatusInFlight
	StatusQueued             = core.StatusQueued
	Version                  = core.Version
)

var (
	ErrAquiferDraining   = core.ErrAquiferDraining
	ErrJobNotFound       = core.ErrJobNotFound
	ErrJobResultNotFound = core.ErrJobResultNotFound
)

// LoadAdmissionLimits is re-exported from internal/core.
func LoadAdmissionLimits() AdmissionLimits {
	return core.LoadAdmissionLimits()
}

// LoadClusterConfig is re-exported from internal/core.
func LoadClusterConfig() ClusterConfig {
	return core.LoadClusterConfig()
}

// LoadConfig is re-exported from internal/core.
func LoadConfig(path string) *Config {
	return core.LoadConfig(path)
}

// LoadDrainConfig is re-exported from internal/core.
func LoadDrainConfig() DrainConfig {
	return core.LoadDrainConfig()
}

// LoadRegistrationConfig is re-exported from internal/core.
func LoadRegistrationConfig() RegistrationConfig {
	return core.LoadRegistrationConfig()
}

// LoadRemoteIdempotencyConfig is re-exported from internal/core.
func LoadRemoteIdempotencyConfig() RemoteIdempotencyConfig {
	return core.LoadRemoteIdempotencyConfig()
}

// LoadShutdownConfig is re-exported from internal/core.
func LoadShutdownConfig() ShutdownConfig {
	return core.LoadShutdownConfig()
}

// LoadWebSocketConfig is re-exported from internal/core.
func LoadWebSocketConfig() WebSocketConfig {
	return core.LoadWebSocketConfig()
}

// LoadWebSocketExportConfig is re-exported from internal/core.
func LoadWebSocketExportConfig() WebSocketExportConfig {
	return core.LoadWebSocketExportConfig()
}

// NewAccountQueue is re-exported from internal/core.
func NewAccountQueue(key string, upstream string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook WebhookEnqueuer, resultRecorder JobResultRecorder, workerActiveQueues *atomic.Int64, workerBacklog *atomic.Int64, onJobDone func(*Job), onIdle func(string), slowStart bool, onSlowStartSignal func(bool), onMaxBacklogSignal func(int64)) *AccountQueue {
	return core.NewAccountQueue(key, upstream, rps, maxConc, pool, store, broker, l8, metrics, enqueueWebhook, resultRecorder, workerActiveQueues, workerBacklog, onJobDone, onIdle, slowStart, onSlowStartSignal, onMaxBacklogSignal)
}

// NewAdmissionController is re-exported from internal/core.
func NewAdmissionController(limits AdmissionLimits, dbPath string) *AdmissionController {
	return core.NewAdmissionController(limits, dbPath)
}

// NewAquifer is re-exported from internal/core.
func NewAquifer(store JobStore, registry *Registry, broker *Broker, l8 *L8Registry, admission *AdmissionController, pools *PoolRegistry) *Aquifer {
	return core.NewAquifer(store, registry, broker, l8, admission, pools)
}

// NewBroker is re-exported from internal/core.
func NewBroker() *Broker {
	return core.NewBroker()
}

// NewClusterRouter is re-exported from internal/core.
func NewClusterRouter(cfg ClusterConfig) *ClusterRouter {
	return core.NewClusterRouter(cfg)
}

// NewFlyRegionAdapter is re-exported from internal/core.
func NewFlyRegionAdapter() *FlyRegionAdapter {
	return core.NewFlyRegionAdapter()
}

// NewHTTPAdapter is re-exported from internal/core.
func NewHTTPAdapter(addr string) *HTTPAdapter {
	return core.NewHTTPAdapter(addr)
}

// NewInboundRateController is re-exported from internal/core.
func NewInboundRateController() *InboundRateController {
	return core.NewInboundRateController()
}

// NewJob is re-exported from internal/core.
func NewJob(r *JobRequest) *Job {
	return core.NewJob(r)
}

// NewJobStore opens whichever backend AQUIFER_STORE_BACKEND names.
func NewJobStore(path string) JobStore {
	return core.NewJobStore(path)
}

// NewL8Registry is l8.NewRegistry with Aquifer's settings: handshakes share
// the dispatch client's pooled transport, and AQUIFER_L8_SCHEMA_VALIDATION
// turns on request-schema validation.
func NewL8Registry(keyPath string, trustDir string) *L8Registry {
	return core.NewL8Registry(keyPath, trustDir)
}

// NewPebbleStore is re-exported from internal/core.
func NewPebbleStore(path string) *PebbleStore {
	return core.NewPebbleStore(path)
}

// NewPoolRegistry is re-exported from internal/core.
func NewPoolRegistry() *PoolRegistry {
	return core.NewPoolRegistry()
}

// NewPrometheusMetrics reads AQUIFER_METRICS_TENANT_LABELS.
func NewPrometheusMetrics() *PrometheusMetrics {
	return core.NewPrometheusMetrics()
}

// NewRedisWebSocketStreamStore is re-exported from internal/core.
func NewRedisWebSocketStreamStore(rawURL string, prefix string, maxEvents int64, ttl time.Duration, readerShards int, poolSize int) (*RedisWebSocketStreamStore, error) {
	return core.NewRedisWebSocketStreamStore(rawURL, prefix, maxEvents, ttl, readerShards, poolSize)
}

// NewRegistry is re-exported from internal/core.
func NewRegistry(store JobStore, cfg *Config, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pools *PoolRegistry) *Registry {
	return core.NewRegistry(store, cfg, broker, l8, metrics, pools)
}

// NewRuntime is re-exported from internal/core.
func NewRuntime(opts RuntimeOptions) *Runtime {
	return core.NewRuntime(opts)
}

// NewServer is re-exported from internal/core.
func NewServer(aquifer *Aquifer) *Server {
	return core.NewServer(aquifer)
}

// NewStore is re-exported from internal/core.
func NewStore(path string) *Store {
	return core.NewStore(path)
}

// NewURLWorker is re-exported from internal/core.
func NewURLWorker(domain string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook WebhookEnqueuer, resultRecorder JobResultRecorder, onJobDone func(*Job), onIdle func(string, *URLWorker)) *URLWorker {
	return core.NewURLWorker(domain, rps, maxConc, pool, store, broker, l8, metrics, enqueueWebhook, resultRecorder, onJobDone, onIdle)
}

// NewValkeyClusterStateStore is re-exported from internal/core.
func NewValkeyClusterStateStore(rawURL string, prefix string) (*ValkeyClusterStateStore, error) {
	return core.NewValkeyClusterStateStore(rawURL, prefix)
}

// NewValkeyRemoteIdempotency is re-exported from internal/core.
func NewValkeyRemoteIdempotency(cfg RemoteIdempotencyConfig) RemoteIdempotency {
	return core.NewValkeyRemoteIdempotency(cfg)
}

// NewWebSocketManager is re-exported from internal/core.
func NewWebSocketManager(cfg WebSocketConfig, store WebSocketStreamStore) *WebSocketManager {
	return core.NewWebSocketManager(cfg, store)
}

// NewWebSocketManagerFromConfig is re-exported from internal/core.
func NewWebSocketManagerFromConfig(cfg WebSocketConfig) (*WebSocketManager, error) {
	return core.NewWebSocketManagerFromConfig(cfg)
}

// NewWebSocketScheduler is re-exported from internal/core.
func NewWebSocketScheduler(cfg WebSocketSchedulerConfig) *WebSocketScheduler {
	return core.NewWebSocketScheduler(cfg)
}

// NodeID names this Aquifer node to the backends it dispatches to
// (X-Aqueduct-Node-Id). It's the first of AQUIFER_NODE_ID,
// AQUIFER_CLUSTER_SELF_ID, FLY_MACHINE_ID and the host name that is set.
func NodeID() string {
	return core.NodeID()
}

// RunAdapter is re-exported from internal/core.
func RunAdapter(ctx context.Context, adapter FrameworkAdapter, opts RuntimeOptions) error {
	return core.RunAdapter(ctx, adapter, opts)
}

// ShutdownHTTPServer gives framework adapters the same bounded server
// shutdown behavior as Aquifer's built-in HTTP adapter.
func ShutdownHTTPServer(server *http.Server) {
	core.ShutdownHTTPServer(server)
}

// StartWebSocketExport is re-exported from internal/core.
func StartWebSocketExport(store *RedisWebSocketStreamStore, cfg WebSocketExportConfig) (*WebSocketExporter, error) {
	return core.StartWebSocketExport(store, cfg)
}
