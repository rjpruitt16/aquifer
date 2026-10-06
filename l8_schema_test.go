package aquifer

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

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

func schemaJob(url, body string) JobRequest {
	req := sampleJobRequest("agent-a", "k-"+body)
	req.URL = url
	req.Body = body
	return req
}

func TestSchemaValidationOffByDefault(t *testing.T) {
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	if _, err := app.Enqueue(schemaJob(up.URL+"/v1/chat", `{"nope":1}`)); err != nil {
		t.Fatalf("validation must be off by default, got %v", err)
	}
	if n := up.metaFetchs.Load(); n != 0 {
		t.Fatalf("expected no metadata fetch while disabled, got %d", n)
	}
}

func TestSchemaValidationRejectsMismatch(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, store := testAquiferWithLimits(t, AdmissionLimits{})

	_, err := app.Enqueue(schemaJob(up.URL+"/v1/chat", `{"model":"m"}`))
	var mismatch *SchemaMismatchError
	if !errors.As(err, &mismatch) || mismatch.Route != "POST /v1/chat" || mismatch.SchemaHash != "sha256:v1" {
		t.Fatalf("expected schema mismatch for POST /v1/chat, got %v", err)
	}
	if entries := store.ListIdempotentKeys(); len(entries) != 0 {
		t.Fatalf("rejected job must not leave a ghost row, got %+v", entries)
	}

	if _, err := app.Enqueue(schemaJob(up.URL+"/v1/chat", `not json`)); !errors.As(err, &mismatch) {
		t.Fatalf("expected non-JSON body to be rejected, got %v", err)
	}
	if _, err := app.Enqueue(schemaJob(up.URL+"/v1/chat", `{"model":"m","messages":[]}`)); err != nil {
		t.Fatalf("valid body rejected: %v", err)
	}
	if _, err := app.Enqueue(schemaJob(up.URL+"/v1/other", `anything`)); err != nil {
		t.Fatalf("route without a schema must pass, got %v", err)
	}
	if n := up.metaFetchs.Load(); n != 1 {
		t.Fatalf("expected metadata cached after one fetch, got %d fetches", n)
	}
}

func TestSchemaHashChangeRefetches(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	app.l8.schemas.minRefetchAge = 0
	url := up.URL + "/v1/chat"

	v2Body := `{"model":"m","input":"hi"}`
	if err := app.l8.ValidateRequestBody(url, "POST", v2Body); err == nil {
		t.Fatal("v2 body should fail the v1 schema")
	}

	up.set(chatSchemaV2, "sha256:v2")
	app.l8.ObserveSchemaHash(url, "sha256:v1")
	if err := app.l8.ValidateRequestBody(url, "POST", v2Body); err == nil {
		t.Fatal("an unchanged hash must not trigger a refetch")
	}

	app.l8.ObserveSchemaHash(url, "sha256:v2")
	if err := app.l8.ValidateRequestBody(url, "POST", v2Body); err != nil {
		t.Fatalf("expected refetched v2 schema to accept body, got %v", err)
	}
	if n := up.metaFetchs.Load(); n != 2 {
		t.Fatalf("expected exactly one refetch, got %d fetches", n)
	}
}

func TestSchemaHashHeaderObservedOnDispatch(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	app.l8.schemas.minRefetchAge = 0
	url := up.URL + "/v1/chat"
	if err := app.l8.ValidateRequestBody(url, "POST", `{"model":"m","messages":[]}`); err != nil {
		t.Fatal(err)
	}

	up.set(chatSchemaV2, "sha256:v2")
	job := NewJob(&JobRequest{UserID: "u", IdempotentKey: "k", URL: url, Method: "POST", WebhookURL: "https://example.com/cb"})
	resp, err := makeRequest(t.Context(), job, url, 0, 0, 0, 0, 0, app.l8)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if err := app.l8.ValidateRequestBody(url, "POST", `{"model":"m","input":"hi"}`); err != nil {
		t.Fatalf("dispatch response hash should have invalidated the cache, got %v", err)
	}
}

func TestSchemaExternalRefIsNotFetched(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	var refHits atomic.Int64
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refHits.Add(1)
		w.Write([]byte(`{"type":"object"}`))
	}))
	defer evil.Close()

	up := newSchemaUpstream(t, `{"$ref":"`+evil.URL+`/schema.json"}`, "sha256:ref")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	if err := app.l8.ValidateRequestBody(up.URL+"/v1/chat", "POST", `{}`); err != nil {
		t.Fatalf("uncompilable schemas degrade open, got %v", err)
	}
	if n := refHits.Load(); n != 0 {
		t.Fatalf("external $ref must never be fetched, got %d hits", n)
	}
}

func TestSchemaMetadataFetchedOncePerDomainUnderConcurrency(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app.l8.ValidateRequestBody(up.URL+"/v1/chat", "POST", `{}`)
		}()
	}
	wg.Wait()
	if n := up.metaFetchs.Load(); n != 1 {
		t.Fatalf("expected one metadata fetch, got %d", n)
	}
}

func TestSchemaMismatchReturns422(t *testing.T) {
	t.Setenv("AQUIFER_L8_SCHEMA_VALIDATION", "true")
	up := newSchemaUpstream(t, chatSchemaV1, "sha256:v1")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	handler := NewServer(app).Routes()

	for _, path := range []string{"/jobs", "/proxy"} {
		body, _ := json.Marshal(schemaJob(up.URL+"/v1/chat", `{"model":"m"}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: expected 422, got %d: %s", path, rec.Code, rec.Body.String())
		}
		var resp map[string]any
		json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp["schema_route"] != "POST /v1/chat" || resp["schema_hash"] != "sha256:v1" || resp["schema_errors"] == "" {
			t.Fatalf("%s: unexpected 422 body %v", path, resp)
		}
	}
}
