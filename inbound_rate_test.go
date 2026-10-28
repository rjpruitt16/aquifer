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

func TestInboundRateHeaderOnJobs(t *testing.T) {
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	body, _ := json.Marshal(sampleJobRequest("u", "header-on"))
	rec := httptest.NewRecorder()
	NewServer(app).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if got := rec.Header().Get("X-Aqueduct-Rps"); got != "160.00" {
		t.Fatalf("expected the lone caller to be advertised the full 160 rps, got %q", got)
	}
}
