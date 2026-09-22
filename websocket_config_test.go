package aquifer

import "testing"

func TestWebSocketConfigAutomaticallyEnablesWithValkey(t *testing.T) {
	t.Setenv("AQUIFER_WS_ENABLED", "")
	t.Setenv("AQUIFER_VALKEY_URL", "redis://valkey:6379")

	if cfg := LoadWebSocketConfig(); !cfg.Enabled {
		t.Fatal("expected WebSockets to enable automatically when Valkey is configured")
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
