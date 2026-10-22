// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"sync/atomic"

	"github.com/rjpruitt16/aquifer/internal/dispatch"
)

// The dispatch path (per-target-API workers, per-tenant queues, pools,
// webhook delivery, drain mode and registration) lives in internal/dispatch.
// These names keep the API embedding applications already use.
type (
	Registry           = dispatch.Registry
	URLWorker          = dispatch.URLWorker
	AccountQueue       = dispatch.AccountQueue
	Pool               = dispatch.Pool
	PoolMember         = dispatch.PoolMember
	PoolRegistry       = dispatch.PoolRegistry
	Broker             = dispatch.Broker
	SSEEvent           = dispatch.SSEEvent
	Config             = dispatch.Config
	RateConfig         = dispatch.RateConfig
	DrainConfig        = dispatch.DrainConfig
	DrainState         = dispatch.DrainState
	RegistrationConfig = dispatch.RegistrationConfig
	WebhookEnqueuer    = dispatch.WebhookEnqueuer
)

const (
	DrainStateActive     = dispatch.DrainStateActive
	DrainStateDraining   = dispatch.DrainStateDraining
	DrainStateUnassigned = dispatch.DrainStateUnassigned
)

func NewRegistry(store JobStore, cfg *Config, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pools *PoolRegistry) *Registry {
	return dispatch.NewRegistry(store, cfg, broker, l8, metrics, pools)
}

func NewBroker() *Broker { return dispatch.NewBroker() }

func NewPoolRegistry() *PoolRegistry { return dispatch.NewPoolRegistry() }

func LoadConfig(path string) *Config { return dispatch.LoadConfig(path) }

func LoadDrainConfig() DrainConfig { return dispatch.LoadDrainConfig() }

func LoadRegistrationConfig() RegistrationConfig { return dispatch.LoadRegistrationConfig() }

func NewURLWorker(domain string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook WebhookEnqueuer, resultRecorder JobResultRecorder, onJobDone func(*Job), onIdle func(string, *URLWorker)) *URLWorker {
	return dispatch.NewURLWorker(domain, rps, maxConc, pool, store, broker, l8, metrics, enqueueWebhook, resultRecorder, onJobDone, onIdle)
}

func NewAccountQueue(key, upstream string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook WebhookEnqueuer, resultRecorder JobResultRecorder, workerActiveQueues, workerBacklog *atomic.Int64, onJobDone func(*Job), onIdle func(string), slowStart bool, onSlowStartSignal func(bool), onMaxBacklogSignal func(int64)) *AccountQueue {
	return dispatch.NewAccountQueue(key, upstream, rps, maxConc, pool, store, broker, l8, metrics, enqueueWebhook, resultRecorder, workerActiveQueues, workerBacklog, onJobDone, onIdle, slowStart, onSlowStartSignal, onMaxBacklogSignal)
}
