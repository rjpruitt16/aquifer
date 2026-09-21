package aquifer

import (
	"testing"

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
