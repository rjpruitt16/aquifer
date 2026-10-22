// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInboundRateOffByDefault(t *testing.T) {
	if c := NewInboundRateController(); c != nil {
		t.Fatal("inbound pacing must be off unless AQUIFER_INBOUND_RPS_ENABLED is set")
	}
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	body, _ := json.Marshal(sampleJobRequest("u", "off-by-default"))
	rec := httptest.NewRecorder()
	NewServer(app).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if rec.Header().Get("X-Aqueduct-Rps") != "" {
		t.Fatal("no X-Aqueduct-Rps header should be sent while disabled")
	}
}

func TestInboundRateHeaderOnJobs(t *testing.T) {
	t.Setenv("AQUIFER_INBOUND_RPS_ENABLED", "true")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	body, _ := json.Marshal(sampleJobRequest("u", "header-on"))
	rec := httptest.NewRecorder()
	NewServer(app).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if got := rec.Header().Get("X-Aqueduct-Rps"); got != "160.00" {
		t.Fatalf("expected the lone caller to be advertised the full 160 rps, got %q", got)
	}
}
