package aquifer

import (
	"log"
	"os"
	"strconv"
	"time"
)

const (
	defaultWebSocketMaxClients       = 1000
	defaultWebSocketMaxUpstreams     = 1000
	defaultWebSocketMaxWaiting       = 1000
	defaultWebSocketConnectRPS       = 20.0
	defaultWebSocketMaxMessageBytes  = 1024 * 1024
	defaultWebSocketReadBlockMS      = 1000
	defaultWebSocketHandshakeTimeout = 10 * time.Second
	defaultWebSocketReconnectMax     = 30 * time.Second
)

type WebSocketConfig struct {
	Enabled          bool
	RedisURL         string
	StreamPrefix     string
	StreamMaxEvents  int64
	ReadBatch        int64
	ReadBlock        time.Duration
	MaxMessageBytes  int64
	HandshakeTimeout time.Duration
	ReconnectMax     time.Duration
	Scheduler        WebSocketSchedulerConfig
}

func LoadWebSocketConfig() WebSocketConfig {
	return WebSocketConfig{
		Enabled:          webSocketEnabled(),
		RedisURL:         os.Getenv("AQUIFER_VALKEY_URL"),
		StreamPrefix:     defaultString(os.Getenv("AQUIFER_WS_STREAM_PREFIX"), defaultWebSocketStreamPrefix),
		StreamMaxEvents:  positiveEnvInt64("AQUIFER_WS_STREAM_MAX_EVENTS", defaultWebSocketStreamMaxEvents),
		ReadBatch:        positiveEnvInt64("AQUIFER_WS_READ_BATCH", defaultWebSocketReadBatch),
		ReadBlock:        time.Duration(positiveEnvInt64("AQUIFER_WS_READ_BLOCK_MS", defaultWebSocketReadBlockMS)) * time.Millisecond,
		MaxMessageBytes:  positiveEnvInt64("AQUIFER_WS_MAX_MESSAGE_BYTES", defaultWebSocketMaxMessageBytes),
		HandshakeTimeout: time.Duration(positiveEnvInt64("AQUIFER_WS_HANDSHAKE_TIMEOUT_SECONDS", int64(defaultWebSocketHandshakeTimeout/time.Second))) * time.Second,
		ReconnectMax:     time.Duration(positiveEnvInt64("AQUIFER_WS_RECONNECT_MAX_SECONDS", int64(defaultWebSocketReconnectMax/time.Second))) * time.Second,
		Scheduler: WebSocketSchedulerConfig{
			MaxClients:   int(positiveEnvInt64("AQUIFER_WS_MAX_CLIENT_CONNECTIONS", defaultWebSocketMaxClients)),
			MaxUpstreams: int(positiveEnvInt64("AQUIFER_WS_MAX_UPSTREAM_CONNECTIONS", defaultWebSocketMaxUpstreams)),
			MaxWaiting:   int(positiveEnvInt64("AQUIFER_WS_MAX_WAITING_CONNECTIONS", defaultWebSocketMaxWaiting)),
			ConnectRPS:   positiveEnvFloat64("AQUIFER_WS_CONNECT_RPS", defaultWebSocketConnectRPS),
		},
	}
}

func webSocketEnabled() bool {
	if raw := os.Getenv("AQUIFER_WS_ENABLED"); raw != "" {
		return envBool("AQUIFER_WS_ENABLED", true)
	}
	return os.Getenv("AQUIFER_VALKEY_URL") != ""
}

func positiveEnvInt64(key string, fallback int64) int64 {
	value := envInt64(key, fallback)
	if value <= 0 {
		log.Printf("websocket: %s must be positive, using default %d", key, fallback)
		return fallback
	}
	return value
}

func positiveEnvFloat64(key string, fallback float64) float64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value <= 0 {
		log.Printf("websocket: invalid %s=%q, using default %.2f", key, raw, fallback)
		return fallback
	}
	return value
}
