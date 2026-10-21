// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// Aquifer's own tests cover validation through POST /jobs and /proxy; this
// one covers the schema cache's refetch rule directly.

const chatSchemaV1 = `{"type":"object","required":["model","messages"],"properties":{"model":{"type":"string"},"messages":{"type":"array"}}}`
const chatSchemaV2 = `{"type":"object","required":["model","input"],"properties":{"model":{"type":"string"},"input":{"type":"string"}}}`

type schemaUpstream struct {
	*httptest.Server
	mu         sync.Mutex
	schema     string
	hash       string
	metaFetchs atomic.Int64
}

func newSchemaUpstream(t *testing.T, schema, hash string) *schemaUpstream {
	t.Helper()
	u := &schemaUpstream{schema: schema, hash: hash}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		schema, hash := u.schema, u.hash
		u.mu.Unlock()
		if r.URL.Path == "/.well-known/l8" {
			u.metaFetchs.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"protocol_version":"0.2","service_name":"upstream","public_key":"cGs=","challenge_endpoint":"/l8/challenge","schema_hash":"` + hash + `","request_schemas":{"POST /v1/chat":` + schema + `}}`))
			return
		}
		w.Header().Set("X-Aqueduct-Schema-Hash", hash)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(u.Close)
	return u
}

func (u *schemaUpstream) set(schema, hash string) {
	u.mu.Lock()
	u.schema, u.hash = schema, hash
	u.mu.Unlock()
}

func TestSchemaHashChangeRefetches(t *testing.T) {
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	dir := t.TempDir()
	r := NewRegistry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"), WithSchemaValidation(true))
	t.Cleanup(r.Close)
	r.schemas.minRefetchAge = 0
	url := up.URL + "/v1/chat"

	v2Body := `{"model":"m","input":"hi"}`
	if err := r.ValidateRequestBody(url, "POST", v2Body); err == nil {
		t.Fatal("v2 body should fail the v1 schema")
	}

	up.set(chatSchemaV2, "sha256:v2")
	r.ObserveSchemaHash(url, "sha256:v1")
	if err := r.ValidateRequestBody(url, "POST", v2Body); err == nil {
		t.Fatal("an unchanged hash must not trigger a refetch")
	}

	r.ObserveSchemaHash(url, "sha256:v2")
	if err := r.ValidateRequestBody(url, "POST", v2Body); err != nil {
		t.Fatalf("expected refetched v2 schema to accept body, got %v", err)
	}
	if n := up.metaFetchs.Load(); n != 2 {
		t.Fatalf("expected exactly one refetch, got %d fetches", n)
	}
}
