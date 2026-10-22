// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/storage"

// The job stores live in internal/storage. These names keep them reachable
// for embedding applications that pick or wrap a backend themselves.
type (
	JobStore    = storage.Store
	Store       = storage.SQLite
	PebbleStore = storage.Pebble
	StoreCounts = storage.Counts

	// The shared idempotency and result ledger in Valkey.
	RemoteIdempotency       = storage.Remote
	RemoteIdempotencyEntry  = storage.RemoteEntry
	RemoteIdempotencyConfig = storage.RemoteConfig
	ValkeyRemoteIdempotency = storage.ValkeyRemote
	JobResultRecorder       = storage.ResultRecorder
	JobResultReader         = storage.ResultReader
)

// NewJobStore opens whichever backend AQUIFER_STORE_BACKEND names.
func NewJobStore(path string) JobStore { return storage.Open(path) }

func NewStore(path string) *Store { return storage.NewSQLite(path) }

func NewPebbleStore(path string) *PebbleStore { return storage.NewPebble(path) }

func LoadRemoteIdempotencyConfig() RemoteIdempotencyConfig { return storage.LoadRemoteConfig() }

func NewValkeyRemoteIdempotency(cfg RemoteIdempotencyConfig) RemoteIdempotency {
	return storage.NewValkeyRemote(cfg)
}
