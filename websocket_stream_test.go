package aquifer

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNormalizeWebSocketRedisURLSupportsValkeySchemes(t *testing.T) {
	tests := map[string]string{
		"redis://localhost:6379/0":   "redis://localhost:6379/0",
		"rediss://localhost:6379/0":  "rediss://localhost:6379/0",
		"valkey://localhost:6379/0":  "redis://localhost:6379/0",
		"valkeys://localhost:6379/0": "rediss://localhost:6379/0",
	}
	for input, expected := range tests {
		got, err := normalizeWebSocketRedisURL(input)
		if err != nil {
			t.Fatalf("normalize %q: %v", input, err)
		}
		if got != expected {
			t.Fatalf("normalize %q: expected %q, got %q", input, expected, got)
		}
	}
}

func TestCompareRedisStreamIDs(t *testing.T) {
	tests := []struct {
		left, right string
		expected    int
	}{
		{"1-0", "2-0", -1},
		{"2-0", "2-0", 0},
		{"2-1", "2-0", 1},
		{"10-0", "2-99", 1},
	}
	for _, test := range tests {
		got, err := compareRedisStreamIDs(test.left, test.right)
		if err != nil {
			t.Fatalf("compare %q and %q: %v", test.left, test.right, err)
		}
		if got != test.expected {
			t.Fatalf("compare %q and %q: expected %d, got %d", test.left, test.right, test.expected, got)
		}
	}
}

func TestDecodeWebSocketStreamEvent(t *testing.T) {
	event, err := decodeWebSocketStreamEvent(redis.XMessage{
		ID: "10-2",
		Values: map[string]any{
			"direction":   "backend",
			"type":        "event",
			"message_id":  "backend-1",
			"caused_by":   "client-1",
			"payload":     `{"ok":true}`,
			"generation":  "3",
			"recorded_at": "1234",
		},
	})
	if err != nil {
		t.Fatalf("decode stream event: %v", err)
	}
	if event.StreamID != "10-2" || event.Direction != "backend" || event.Envelope.MessageID != "backend-1" {
		t.Fatalf("unexpected stream event: %+v", event)
	}
	if event.Envelope.CausedBy != "client-1" || event.Envelope.Generation != 3 || event.RecordedAt != 1234 {
		t.Fatalf("unexpected stream metadata: %+v", event)
	}
	if string(event.Envelope.Payload) != `{"ok":true}` {
		t.Fatalf("unexpected payload: %s", event.Envelope.Payload)
	}
}

// Hundreds of live sessions share a few pub/sub connections: appends stay
// fast, every follower of a stream that gets an entry is signaled, and
// followers of other streams are not.
func TestSharedFollowerSignalsManySessions(t *testing.T) {
	rawURL := os.Getenv("AQUIFER_TEST_VALKEY_URL")
	if rawURL == "" {
		t.Skip("set AQUIFER_TEST_VALKEY_URL to run the real Valkey integration test")
	}
	sessions := 10*runtime.GOMAXPROCS(0) + 500
	prefix := fmt.Sprintf("aqueduct:test:follow:%d:", time.Now().UnixNano())
	store, err := NewRedisWebSocketStreamStore(rawURL, prefix, 100, time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	follows := make([]WebSocketFollow, sessions)
	for i := range follows {
		f, err := store.Follow(ctx, fmt.Sprintf("s-%d", i), "0-0")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		follows[i] = f
	}
	// Each follower is signaled once when its subscription is confirmed (its
	// session re-reads then, covering anything recorded before). Wait for
	// those, then clear them so the checks below see only new signals.
	for i, f := range follows {
		select {
		case <-f.C():
		case <-time.After(2 * time.Second):
			t.Fatalf("follower %d never saw its subscription confirmed", i)
		}
	}

	started := time.Now()
	if _, err := store.Append(ctx, "s-7", "backend", WebSocketEnvelope{Type: "event", MessageID: "e1"}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(started); took > 200*time.Millisecond {
		t.Fatalf("append took %s with %d sessions following", took, sessions)
	}
	select {
	case <-follows[7].C():
	case <-time.After(2 * time.Second):
		t.Fatal("follower of s-7 was not signaled")
	}
	select {
	case <-follows[8].C():
		t.Fatal("a follower of a different stream was signaled")
	default:
	}

	// A follow created after the entry exists, with an older cursor, must be
	// signaled too (a client reconnecting from its last cursor).
	late, err := store.Follow(ctx, "s-7", "0-0")
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	select {
	case <-late.C(): // joining an existing subscription signals at once
	case <-time.After(2 * time.Second):
		t.Fatal("a follower joining an existing subscription was not signaled")
	}
	if _, err := store.Append(ctx, "s-7", "backend", WebSocketEnvelope{Type: "event", MessageID: "e2"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-late.C():
	case <-time.After(2 * time.Second):
		t.Fatal("a newly added follower was not signaled")
	}
}
