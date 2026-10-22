// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// testL8Receiver spins up a receiver implementing just enough of the L8
// protocol (GET /.well-known/l8, POST /l8/challenge -- the same two
// handlers server.go exposes) to let a sender's L8Registry.EnsureTrust
// actually establish trust, plus a catch-all that captures the headers of
// whatever gets POSTed to it afterward.
func testL8Receiver(t *testing.T) (*httptest.Server, <-chan http.Header) {
	t.Helper()
	dir := t.TempDir()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	app := NewAquifer(nil, nil, nil, l8, nil, nil)
	t.Cleanup(app.Close)

	captured := make(chan http.Header, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/l8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(app.L8Metadata(r.Host))
	})
	mux.HandleFunc("POST /l8/challenge", func(w http.ResponseWriter, r *http.Request) {
		var req L8ChallengeReq
		json.NewDecoder(r.Body).Decode(&req)
		resp, err := app.HandleL8Challenge(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		captured <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, captured
}
