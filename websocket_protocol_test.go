package aquifer

import (
	"errors"
	"testing"
)

func TestDecodeClientWebSocketMessageRequiresCommandIdentityAndPayload(t *testing.T) {
	msg, err := decodeClientWebSocketMessage([]byte(`{"type":"command","message_id":"client-1","payload":{"action":"start"}}`))
	if err != nil {
		t.Fatalf("expected valid command, got %v", err)
	}
	if msg.MessageID != "client-1" || string(msg.Payload) != `{"action":"start"}` {
		t.Fatalf("unexpected decoded command: %+v", msg)
	}

	invalid := []string{
		`{"type":"event","message_id":"client-1","payload":{}}`,
		`{"type":"command","payload":{}}`,
		`{"type":"command","message_id":"client-1"}`,
	}
	for _, raw := range invalid {
		if _, err := decodeClientWebSocketMessage([]byte(raw)); !errors.Is(err, ErrWebSocketProtocol) {
			t.Fatalf("expected protocol error for %s, got %v", raw, err)
		}
	}
}

func TestDecodeBackendWebSocketMessageSupportsOneToManyCausation(t *testing.T) {
	for _, raw := range []string{
		`{"type":"ack","message_id":"client-1"}`,
		`{"type":"event","message_id":"backend-1","caused_by":"client-1","payload":{"part":1}}`,
		`{"type":"event","message_id":"backend-2","caused_by":"client-1","payload":{"part":2}}`,
		`{"type":"event","message_id":"backend-unsolicited","payload":{"notice":true}}`,
		`{"type":"aqueduct.capacity","max_connections":8,"connect_rps":2.5}`,
	} {
		if _, err := decodeBackendWebSocketMessage([]byte(raw)); err != nil {
			t.Fatalf("expected valid backend message %s, got %v", raw, err)
		}
	}
}

func TestDecodeBackendWebSocketMessageRejectsInvalidCapacity(t *testing.T) {
	for _, raw := range []string{
		`{"type":"aqueduct.capacity"}`,
		`{"type":"aqueduct.capacity","max_connections":0}`,
		`{"type":"aqueduct.capacity","connect_rps":-1}`,
		`{"type":"event","message_id":"backend-1"}`,
	} {
		if _, err := decodeBackendWebSocketMessage([]byte(raw)); !errors.Is(err, ErrWebSocketProtocol) {
			t.Fatalf("expected protocol error for %s, got %v", raw, err)
		}
	}
}
