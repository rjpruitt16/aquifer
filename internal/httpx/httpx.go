// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package httpx holds the small HTTP response helpers shared by Aquifer's
// handlers, so every endpoint answers errors and drains the same way.
package httpx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// NodeStateHeader tells clients and load balancers a node is draining.
const NodeStateHeader = "X-Aqueduct-Node-State"

// ErrDraining is returned for work refused because the node is shutting down.
var ErrDraining = errors.New("aquifer is draining")

// JSONError writes {"error": msg} with the given status.
func JSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// WriteDraining answers 503 with Retry-After and closes the connection, so
// the client retries on another node.
func WriteDraining(w http.ResponseWriter, retryAfter int) {
	if retryAfter <= 0 {
		retryAfter = 1
	}
	w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
	w.Header().Set(NodeStateHeader, "draining")
	w.Header().Set("X-Aquifer-Node-State", "draining")
	w.Header().Set("Connection", "close")
	JSONError(w, ErrDraining.Error(), http.StatusServiceUnavailable)
}
