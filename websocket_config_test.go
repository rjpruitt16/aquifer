package aquifer

import (
	"testing"
	"time"
)

func TestWebSocketConfigAutomaticallyEnablesWithValkey(t *testing.T) {
	t.Setenv("AQUIFER_WS_ENABLED", "")
	t.Setenv("AQUIFER_VALKEY_URL", "redis://valkey:6379")

	if cfg := LoadWebSocketConfig(); !cfg.Enabled {
		t.Fatal("expected WebSockets to enable automatically when Valkey is configured")
	}
}

func TestWebSocketConfigLoadsSlowStartAndStreamTTL(t *testing.T) {
	t.Setenv("AQUIFER_WS_SLOW_START_RPS", "2.5")
	t.Setenv("AQUIFER_WS_STREAM_TTL_SECONDS", "60")

	cfg := LoadWebSocketConfig()
	if cfg.Scheduler.SlowStartRPS != 2.5 {
		t.Fatalf("expected slow-start rate 2.5, got %v", cfg.Scheduler.SlowStartRPS)
	}
	if cfg.StreamTTL != time.Minute {
		t.Fatalf("expected one-minute stream TTL, got %v", cfg.StreamTTL)
	}
}

func TestWebSocketConfigStaysInactiveWithoutValkey(t *testing.T) {
	t.Setenv("AQUIFER_WS_ENABLED", "")
	t.Setenv("AQUIFER_VALKEY_URL", "")

	if cfg := LoadWebSocketConfig(); cfg.Enabled {
		t.Fatal("expected WebSockets to stay inactive when no durable stream store is configured")
	}
}

func TestWebSocketConfigSupportsExplicitKillSwitch(t *testing.T) {
	t.Setenv("AQUIFER_WS_ENABLED", "false")
	t.Setenv("AQUIFER_VALKEY_URL", "redis://valkey:6379")

	if cfg := LoadWebSocketConfig(); cfg.Enabled {
		t.Fatal("expected AQUIFER_WS_ENABLED=false to disable WebSockets")
	}
}
