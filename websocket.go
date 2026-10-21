// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"time"

	"github.com/rjpruitt16/aquifer/internal/websocket"
)

// The durable WebSocket proxy lives in internal/websocket. These names keep
// the API embedding applications already use.
type (
	RedisWebSocketStreamStore  = websocket.RedisStreamStore
	WebSocketSchedulerSnapshot = websocket.SchedulerSnapshot
	WebSocketSchedulerConfig   = websocket.SchedulerConfig
	WebSocketExportConfig      = websocket.ExportConfig
	WebSocketExportEvent       = websocket.ExportEvent
	WebSocketExportBatch       = websocket.ExportBatch
	WebSocketStreamStore       = websocket.StreamStore
	WebSocketStreamEvent       = websocket.StreamEvent
	WebSocketExporter          = websocket.Exporter
	WebSocketScheduler         = websocket.Scheduler
	WebSocketAdmission         = websocket.Admission
	WebSocketEnvelope          = websocket.Envelope
	WebSocketManager           = websocket.Manager
	WebSocketConfig            = websocket.Config
	WebSocketFollow            = websocket.Follow
)

func LoadWebSocketConfig() WebSocketConfig { return websocket.LoadConfig() }

func LoadWebSocketExportConfig() WebSocketExportConfig { return websocket.LoadExportConfig() }

func StartWebSocketExport(store *RedisWebSocketStreamStore, cfg WebSocketExportConfig) (*WebSocketExporter, error) {
	return websocket.StartExport(store, cfg)
}

func NewWebSocketManager(cfg WebSocketConfig, store WebSocketStreamStore) *WebSocketManager {
	return websocket.NewManager(cfg, store)
}

func NewWebSocketManagerFromConfig(cfg WebSocketConfig) (*WebSocketManager, error) {
	return websocket.NewManagerFromConfig(cfg)
}

func NewRedisWebSocketStreamStore(rawURL, prefix string, maxEvents int64, ttl time.Duration, readerShards int) (*RedisWebSocketStreamStore, error) {
	return websocket.NewRedisStreamStore(rawURL, prefix, maxEvents, ttl, readerShards)
}

func NewWebSocketScheduler(cfg WebSocketSchedulerConfig) *WebSocketScheduler {
	return websocket.NewScheduler(cfg)
}
