// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/admission"
)

func TestFullnessPercentRoundsAndClamps(t *testing.T) {
	for ratio, want := range map[float64]int64{0: 0, 0.404: 40, 0.805: 81, 1: 100, 3.2: 100, -1: 0} {
		if got := fullnessPercent(ratio); got != want {
			t.Fatalf("fullnessPercent(%v) = %d, want %d", ratio, got, want)
		}
	}
}

// TestFullnessFollowsTheDatabaseAgainstItsLimit: a 400-byte database
// against a 1000-byte limit is a 40% full instance.
func TestFullnessFollowsTheDatabaseAgainstItsLimit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "aquifer.db")
	if err := os.WriteFile(dbPath, make([]byte, 400), 0o600); err != nil {
		t.Fatal(err)
	}
	a := &Aquifer{admission: NewAdmissionController(AdmissionLimits{DBMaxBytes: 1000}, dbPath)}

	if got := a.Fullness(); got != 40 {
		t.Fatalf("expected 40, got %d", got)
	}
}

// TestFullnessIgnoresLimitsThatAreSwitchedOff: no limit configured means
// that resource can't make the instance full.
func TestFullnessIgnoresLimitsThatAreSwitchedOff(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "aquifer.db")
	if err := os.WriteFile(dbPath, make([]byte, 400), 0o600); err != nil {
		t.Fatal(err)
	}
	a := &Aquifer{admission: NewAdmissionController(AdmissionLimits{}, dbPath)}

	if got := a.Fullness(); got != 0 {
		t.Fatalf("expected 0 with every limit off, got %d", got)
	}
}

// TestFullnessFollowsAnUpstreamsBacklogAgainstItsBudget fills a real
// backlog: jobs aimed at a target API that never answers pile up against a
// backlog budget of 10, and every response says how full that makes the
// instance.
func TestFullnessFollowsAnUpstreamsBacklogAgainstItsBudget(t *testing.T) {
	t.Setenv("AQUIFER_MAX_PENDING_PER_UPSTREAM", "10")
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	defer close(release)

	srv := testServer(t, AdmissionLimits{})
	routes := srv.Routes()
	for i := 0; i < 5; i++ {
		job := sampleJobRequest("user-fullness", fmt.Sprintf("fullness-%d", i))
		job.URL = upstream.URL + "/work"
		body, _ := json.Marshal(job)
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
		if rec.Code != http.StatusCreated {
			t.Fatalf("job %d: expected 201, got %d: %s", i, rec.Code, rec.Body.String())
		}
	}

	// Past the 250ms sample window, so the reading reflects all five jobs.
	time.Sleep(admission.SampleTTL + 50*time.Millisecond)
	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	got, err := strconv.Atoi(rec.Header().Get(fullnessHeader))
	if err != nil {
		t.Fatalf("expected a numeric %s header, got %q", fullnessHeader, rec.Header().Get(fullnessHeader))
	}
	if got != 50 {
		t.Fatalf("expected 5 of 10 backlog slots to read as 50, got %d", got)
	}
}

func TestFullnessHeaderIsOnEveryResponse(t *testing.T) {
	routes := testServer(t, AdmissionLimits{}).Routes()
	for _, path := range []string{"/health", "/ready", "/jobs/does-not-exist"} {
		rec := httptest.NewRecorder()
		routes.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Header().Get(fullnessHeader) == "" {
			t.Fatalf("expected %s on %s (status %d)", fullnessHeader, path, rec.Code)
		}
	}
}
