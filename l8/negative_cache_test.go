// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// A webhook receiver that doesn't speak L8 is probed once, not before every
// delivery: each probe was an extra request (and, with the old per-call
// client, a new TCP connection) per webhook.
func TestEnsureTrustRemembersNonL8Receivers(t *testing.T) {
	var probes atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/l8" {
			probes.Add(1)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer receiver.Close()

	dir := t.TempDir()
	l8 := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	defer l8.Close()

	for i := 0; i < 20; i++ {
		l8.EnsureTrust(receiver.URL + "/hook")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("expected 1 probe of a non-L8 receiver across 20 webhooks, got %d", got)
	}
}

// Receiver and target hosts come from callers' URLs, so per-domain caches
// must shrink once their entries expire, or a node that sees many distinct
// hosts grows without bound.
func TestSweepDropsExpiredPerDomainEntries(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"), WithSchemaValidation(true))
	defer r.Close()

	const hosts = 5
	for i := 0; i < hosts; i++ {
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		r.EnsureTrust(receiver.URL + "/hook")
		if err := r.ValidateRequestBody(receiver.URL+"/v1/chat", http.MethodPost, `{}`); err != nil {
			t.Fatalf("a target without schemas must accept any body: %v", err)
		}
		receiver.Close()
	}
	if got, want := countEntries(r), [2]int{hosts, hosts}; got != want {
		t.Fatalf("before the sweep: got (notL8, schemas) = %v, want %v", got, want)
	}

	r.sweep(time.Now())
	if got, want := countEntries(r), [2]int{hosts, hosts}; got != want {
		t.Fatalf("unexpired entries must stay: got %v, want %v", got, want)
	}

	r.sweep(time.Now().Add(schemaTTL + negativeTTL))
	if got := countEntries(r); got != [2]int{} {
		t.Fatalf("expired entries must be dropped: got (notL8, schemas) = %v", got)
	}
}

func countEntries(r *Registry) [2]int {
	var n [2]int
	r.notL8.Range(func(any, any) bool { n[0]++; return true })
	r.schemas.mu.Lock()
	n[1] = len(r.schemas.entries)
	r.schemas.mu.Unlock()
	return n
}
