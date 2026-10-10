package aquifer

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
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

// With more live sessions than the default pool holds (10 per CPU), every
// append used to wait for some session's blocking read to time out. Live
// reads now have their own pool, so an append stays fast however many
// sessions are blocked reading.
func TestAppendIsNotStarvedByBlockingReads(t *testing.T) {
	rawURL := os.Getenv("AQUIFER_TEST_VALKEY_URL")
	if rawURL == "" {
		t.Skip("set AQUIFER_TEST_VALKEY_URL to run the real Valkey integration test")
	}
	sessions := 10*runtime.GOMAXPROCS(0) + 20 // more than the default pool
	prefix := fmt.Sprintf("aqueduct:test:starve:%d:", time.Now().UnixNano())
	store, err := NewRedisWebSocketStreamStore(rawURL, prefix, 100, time.Minute, sessions)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store.ReadAfter(ctx, fmt.Sprintf("idle-%d", i), "0-0", 10, 3*time.Second)
		}(i)
	}
	time.Sleep(300 * time.Millisecond) // let every read block

	started := time.Now()
	if _, err := store.Append(ctx, "busy", "client", WebSocketEnvelope{Type: "command", MessageID: "m1"}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(started); took > 500*time.Millisecond {
		t.Fatalf("append took %s while %d sessions were blocked reading; it waited for a pooled connection", took, sessions)
	}
	cancel()
	wg.Wait()
}
