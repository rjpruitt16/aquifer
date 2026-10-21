// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package l8

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	schemaTTL         = 10 * time.Minute
	schemaNegativeTTL = time.Minute
	// A target API whose X-Aqueduct-Schema-Hash header disagrees with its own
	// /.well-known/l8 would otherwise force a metadata refetch per response.
	schemaMinRefetchAge = 10 * time.Second
)

// SchemaMismatchError means a job body failed the JSON Schema its target API
// advertises for that route. Surfaced to callers as 422.
type SchemaMismatchError struct {
	Route      string
	SchemaHash string
	Detail     string
}

func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("request body does not match the schema the upstream advertises for %s", e.Route)
}

type schemaEntry struct {
	ready     chan struct{}
	hash      string
	routes    map[string]*jsonschema.Schema
	fetchedAt time.Time
	expiresAt time.Time
}

type schemaCache struct {
	enabled       bool
	fetch         func(domain string) (*Meta, error)
	minRefetchAge time.Duration

	mu      sync.Mutex
	entries map[string]*schemaEntry
}

func newSchemaCache(enabled bool, fetch func(string) (*Meta, error)) *schemaCache {
	return &schemaCache{
		enabled:       enabled,
		fetch:         fetch,
		minRefetchAge: schemaMinRefetchAge,
		entries:       map[string]*schemaEntry{},
	}
}

// ValidateRequestBody checks body against the JSON Schema the target API at
// rawURL advertises for method+path in its /.well-known/l8 request_schemas.
// Returns nil when validation is disabled, the target API advertises nothing,
// or its metadata can't be fetched -- L8 stays opt-in and degrades open.
func (r *Registry) ValidateRequestBody(rawURL, method, body string) error {
	if r == nil || r.schemas == nil || !r.schemas.enabled {
		return nil
	}
	domain := extractDomain(rawURL)
	if domain == "" {
		return nil
	}
	entry := r.schemas.get(domain)
	route := routeKey(method, rawURL)
	schema, ok := entry.routes[route]
	if !ok {
		return nil
	}

	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var instance any
	if err := dec.Decode(&instance); err != nil {
		return &SchemaMismatchError{Route: route, SchemaHash: entry.hash, Detail: "body is not valid JSON"}
	}
	if dec.More() {
		return &SchemaMismatchError{Route: route, SchemaHash: entry.hash, Detail: "body has trailing data after the JSON value"}
	}
	if err := schema.Validate(instance); err != nil {
		var verr *jsonschema.ValidationError
		if errors.As(err, &verr) {
			return &SchemaMismatchError{Route: route, SchemaHash: entry.hash, Detail: fmt.Sprintf("%#v", verr)}
		}
		return &SchemaMismatchError{Route: route, SchemaHash: entry.hash, Detail: err.Error()}
	}
	return nil
}

// ObserveSchemaHash reacts to a target API's X-Aqueduct-Schema-Hash response
// header. A hash that differs from the cached one means the target API changed
// its contract: drop the cached schemas and the L8 trust for that domain so
// the next request refetches metadata and re-runs the handshake.
func (r *Registry) ObserveSchemaHash(rawURL, hash string) {
	if r == nil || r.schemas == nil || !r.schemas.enabled || hash == "" {
		return
	}
	domain := extractDomain(rawURL)
	if domain == "" || !r.schemas.invalidateIfStale(domain, hash) {
		return
	}
	log.Printf("[L8] %s advertised schema hash %s; refetching metadata and re-handshaking", domain, hash)
	r.trusts.Delete(domain)
	os.Remove(filepath.Join(r.trustDir, sanitizeDomain(domain)+".json"))
}

func (c *schemaCache) get(domain string) *schemaEntry {
	c.mu.Lock()
	entry, ok := c.entries[domain]
	if ok {
		select {
		case <-entry.ready:
			if time.Now().Before(entry.expiresAt) {
				c.mu.Unlock()
				return entry
			}
		default:
			c.mu.Unlock()
			<-entry.ready
			return entry
		}
	}
	// One fetch per domain at a time; concurrent callers wait on ready
	// instead of all hitting /.well-known/l8 at once.
	entry = &schemaEntry{ready: make(chan struct{})}
	c.entries[domain] = entry
	c.mu.Unlock()

	c.load(domain, entry)
	close(entry.ready)
	return entry
}

func (c *schemaCache) load(domain string, entry *schemaEntry) {
	now := time.Now()
	entry.fetchedAt = now
	entry.expiresAt = now.Add(schemaNegativeTTL)

	meta, err := c.fetch(domain)
	if err != nil || len(meta.RequestSchemas) == 0 {
		return
	}
	if meta.SchemaHash == "" {
		log.Printf("[L8] %s advertises request_schemas without schema_hash; ignoring them", domain)
		return
	}
	routes, err := compileRouteSchemas(meta.RequestSchemas)
	if err != nil {
		log.Printf("[L8] %s request_schemas did not compile: %v", domain, err)
		return
	}
	entry.hash = meta.SchemaHash
	entry.routes = routes
	entry.expiresAt = now.Add(schemaTTL)
}

// sweep deletes fetched entries that have expired. In-flight fetches are
// left alone; get replaces an expired entry anyway, so this only bounds
// memory for domains that stop being called.
func (c *schemaCache) sweep(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for domain, entry := range c.entries {
		select {
		case <-entry.ready:
			if !now.Before(entry.expiresAt) {
				delete(c.entries, domain)
			}
		default:
		}
	}
}

func (c *schemaCache) invalidateIfStale(domain, hash string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[domain]
	if !ok {
		return false
	}
	select {
	case <-entry.ready:
	default:
		return false
	}
	if entry.hash == hash || time.Since(entry.fetchedAt) < c.minRefetchAge {
		return false
	}
	delete(c.entries, domain)
	return true
}

func compileRouteSchemas(raw map[string]json.RawMessage) (map[string]*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020
	// Schemas come from a remote party. Refusing external $ref loads keeps
	// compilation from becoming an SSRF or local-file-read primitive.
	compiler.LoadURL = func(s string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("external $ref %q is not allowed in L8 request_schemas", s)
	}

	routes := make(map[string]*jsonschema.Schema, len(raw))
	for route, doc := range raw {
		key, ok := normalizeRoute(route)
		if !ok {
			return nil, fmt.Errorf("route %q must look like \"POST /path\"", route)
		}
		resource := "l8://request-schemas/" + url.PathEscape(key)
		if err := compiler.AddResource(resource, bytes.NewReader(doc)); err != nil {
			return nil, fmt.Errorf("%s: %w", route, err)
		}
		schema, err := compiler.Compile(resource)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", route, err)
		}
		routes[key] = schema
	}
	return routes, nil
}

func normalizeRoute(route string) (string, bool) {
	method, path, ok := strings.Cut(strings.TrimSpace(route), " ")
	path = strings.TrimSpace(path)
	if !ok || method == "" || !strings.HasPrefix(path, "/") {
		return "", false
	}
	return strings.ToUpper(method) + " " + path, true
}

func routeKey(method, rawURL string) string {
	path := "/"
	if u, err := url.Parse(rawURL); err == nil && u.Path != "" {
		path = u.Path
	}
	return strings.ToUpper(method) + " " + path
}
