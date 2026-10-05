package aquifer

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
	l8SchemaTTL         = 10 * time.Minute
	l8SchemaNegativeTTL = time.Minute
	// An upstream whose X-Aqueduct-Schema-Hash header disagrees with its own
	// /.well-known/l8 would otherwise force a metadata refetch per response.
	l8SchemaMinRefetchAge = 10 * time.Second
)

// SchemaMismatchError means a job body failed the JSON Schema its upstream
// advertises for that route. Surfaced to callers as 422.
type SchemaMismatchError struct {
	Route      string
	SchemaHash string
	Detail     string
}

func (e *SchemaMismatchError) Error() string {
	return fmt.Sprintf("request body does not match the schema the upstream advertises for %s", e.Route)
}

type l8SchemaEntry struct {
	ready     chan struct{}
	hash      string
	routes    map[string]*jsonschema.Schema
	fetchedAt time.Time
	expiresAt time.Time
}

type l8SchemaCache struct {
	enabled       bool
	fetch         func(domain string) (*L8Meta, error)
	minRefetchAge time.Duration

	mu      sync.Mutex
	entries map[string]*l8SchemaEntry
}

func newL8SchemaCache() *l8SchemaCache {
	return &l8SchemaCache{
		enabled:       envBool("AQUIFER_L8_SCHEMA_VALIDATION", false),
		fetch:         fetchL8Meta,
		minRefetchAge: l8SchemaMinRefetchAge,
		entries:       map[string]*l8SchemaEntry{},
	}
}

// ValidateRequestBody checks body against the JSON Schema the upstream at
// rawURL advertises for method+path in its /.well-known/l8 request_schemas.
// Returns nil when validation is disabled, the upstream advertises nothing,
// or its metadata can't be fetched -- L8 stays opt-in and degrades open.
func (r *L8Registry) ValidateRequestBody(rawURL, method, body string) error {
	if r == nil || r.schemas == nil || !r.schemas.enabled {
		return nil
	}
	domain := extractDomain(rawURL)
	if domain == "" {
		return nil
	}
	entry := r.schemas.get(domain)
	route := l8RouteKey(method, rawURL)
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

// ObserveSchemaHash reacts to an upstream's X-Aqueduct-Schema-Hash response
// header. A hash that differs from the cached one means the upstream changed
// its contract: drop the cached schemas and the L8 trust for that domain so
// the next request refetches metadata and re-runs the handshake.
func (r *L8Registry) ObserveSchemaHash(rawURL, hash string) {
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

func (c *l8SchemaCache) get(domain string) *l8SchemaEntry {
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
	entry = &l8SchemaEntry{ready: make(chan struct{})}
	c.entries[domain] = entry
	c.mu.Unlock()

	c.load(domain, entry)
	close(entry.ready)
	return entry
}

func (c *l8SchemaCache) load(domain string, entry *l8SchemaEntry) {
	now := time.Now()
	entry.fetchedAt = now
	entry.expiresAt = now.Add(l8SchemaNegativeTTL)

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
	entry.expiresAt = now.Add(l8SchemaTTL)
}

func (c *l8SchemaCache) invalidateIfStale(domain, hash string) bool {
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
		key, ok := normalizeL8Route(route)
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

func normalizeL8Route(route string) (string, bool) {
	method, path, ok := strings.Cut(strings.TrimSpace(route), " ")
	path = strings.TrimSpace(path)
	if !ok || method == "" || !strings.HasPrefix(path, "/") {
		return "", false
	}
	return strings.ToUpper(method) + " " + path, true
}

func l8RouteKey(method, rawURL string) string {
	path := "/"
	if u, err := url.Parse(rawURL); err == nil && u.Path != "" {
		path = u.Path
	}
	return strings.ToUpper(method) + " " + path
}
