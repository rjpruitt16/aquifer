package aquifer

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	defaultShutdownTimeoutSeconds      = 30
	defaultShutdownQuiesceMilliseconds = 500
	defaultWebSocketDrainGraceSeconds  = 5
	shutdownIdlePollInterval           = 25 * time.Millisecond
)

type ShutdownConfig struct {
	Timeout        time.Duration
	Quiesce        time.Duration
	WebSocketGrace time.Duration
}

// ShutdownHTTPServer gives framework adapters the same bounded server
// shutdown behavior as Aquifer's built-in HTTP adapter.
func ShutdownHTTPServer(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), LoadShutdownConfig().Timeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
	}
}

func LoadShutdownConfig() ShutdownConfig {
	cfg := ShutdownConfig{
		Timeout:        time.Duration(shutdownEnvInt64("AQUIFER_SHUTDOWN_TIMEOUT_SECONDS", defaultShutdownTimeoutSeconds, false)) * time.Second,
		Quiesce:        time.Duration(shutdownEnvInt64("AQUIFER_SHUTDOWN_QUIESCE_MS", defaultShutdownQuiesceMilliseconds, true)) * time.Millisecond,
		WebSocketGrace: time.Duration(shutdownEnvInt64("AQUIFER_WS_DRAIN_GRACE_SECONDS", defaultWebSocketDrainGraceSeconds, true)) * time.Second,
	}
	if cfg.WebSocketGrace > cfg.Timeout {
		cfg.WebSocketGrace = cfg.Timeout
	}
	return cfg
}

func shutdownEnvInt64(key string, fallback int64, allowZero bool) int64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || (!allowZero && value == 0) {
		log.Printf("shutdown: invalid %s=%q, using default %d", key, raw, fallback)
		return fallback
	}
	return value
}

func (r *Runtime) BeginDrain() {
	if r == nil || r.Aquifer == nil {
		return
	}
	r.Aquifer.BeginDrain(r.Shutdown.WebSocketGrace)
}

func (r *Runtime) Drain(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.BeginDrain()

	webSocketsDone := make(chan error, 1)
	if r.WebSockets != nil {
		go func() { webSocketsDone <- r.WebSockets.WaitDrained(ctx) }()
	} else {
		webSocketsDone <- nil
	}

	if r.Registry != nil {
		if err := r.Registry.WaitIdle(ctx); err != nil {
			return err
		}
		if !r.Registry.FlushForShutdown(ctx) {
			log.Printf("shutdown: final drain-ledger flush failed; retaining unacknowledged local events")
		}
	}
	if err := <-webSocketsDone; err != nil {
		return err
	}
	if r.Registry != nil {
		r.Registry.ReportOffline(ctx)
	}
	return nil
}
