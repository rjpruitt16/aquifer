// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	webSocketSubprotocol       = "aqueduct.v1"
	webSocketUpstreamURLHeader = "X-Aqueduct-Upstream-URL"
)

var (
	ErrWebSocketProtocol  = errors.New("invalid aqueduct websocket message")
	ErrWebSocketReplayGap = errors.New("websocket replay cursor is older than retained history")
	// ErrWebSocketTranscriptInvalid marks a transcript entry that can't be
	// decoded; reading it again won't help.
	ErrWebSocketTranscriptInvalid = errors.New("invalid websocket transcript entry")
)

// Envelope is the application and control protocol shared by the
// client, Aquifer, and the target API WebSocket server. Redis stream IDs provide
// delivery order; message_id provides producer identity; caused_by expresses
// causal relationships without assuming one request has exactly one response.
type Envelope struct {
	Type           string          `json:"type"`
	State          string          `json:"state,omitempty"`
	MessageID      string          `json:"message_id,omitempty"`
	CausedBy       string          `json:"caused_by,omitempty"`
	StreamID       string          `json:"stream_id,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	Position       int             `json:"position,omitempty"`
	Generation     int64           `json:"generation,omitempty"`
	RetryAfterMS   int64           `json:"retry_after_ms,omitempty"`
	MaxConnections *int            `json:"max_connections,omitempty"`
	ConnectRPS     *float64        `json:"connect_rps,omitempty"`
}

func decodeClientWebSocketMessage(raw []byte) (Envelope, error) {
	var msg Envelope
	if err := json.Unmarshal(raw, &msg); err != nil {
		return Envelope{}, fmt.Errorf("%w: invalid json", ErrWebSocketProtocol)
	}
	if msg.Type != "command" {
		return Envelope{}, fmt.Errorf("%w: clients may only send command messages", ErrWebSocketProtocol)
	}
	if msg.MessageID == "" {
		return Envelope{}, fmt.Errorf("%w: message_id is required", ErrWebSocketProtocol)
	}
	if len(msg.Payload) == 0 {
		return Envelope{}, fmt.Errorf("%w: payload is required", ErrWebSocketProtocol)
	}
	return msg, nil
}

func decodeBackendWebSocketMessage(raw []byte) (Envelope, error) {
	var msg Envelope
	if err := json.Unmarshal(raw, &msg); err != nil {
		return Envelope{}, fmt.Errorf("%w: invalid json", ErrWebSocketProtocol)
	}

	switch msg.Type {
	case "ack":
		if msg.MessageID == "" {
			return Envelope{}, fmt.Errorf("%w: ack message_id is required", ErrWebSocketProtocol)
		}
	case "event":
		if msg.MessageID == "" {
			return Envelope{}, fmt.Errorf("%w: event message_id is required", ErrWebSocketProtocol)
		}
		if len(msg.Payload) == 0 {
			return Envelope{}, fmt.Errorf("%w: event payload is required", ErrWebSocketProtocol)
		}
	case "aqueduct.capacity":
		if msg.MaxConnections == nil && msg.ConnectRPS == nil {
			return Envelope{}, fmt.Errorf("%w: capacity message must set max_connections or connect_rps", ErrWebSocketProtocol)
		}
		if msg.MaxConnections != nil && *msg.MaxConnections <= 0 {
			return Envelope{}, fmt.Errorf("%w: max_connections must be positive", ErrWebSocketProtocol)
		}
		if msg.ConnectRPS != nil && *msg.ConnectRPS <= 0 {
			return Envelope{}, fmt.Errorf("%w: connect_rps must be positive", ErrWebSocketProtocol)
		}
	default:
		return Envelope{}, fmt.Errorf("%w: unsupported backend message type %q", ErrWebSocketProtocol, msg.Type)
	}

	return msg, nil
}
