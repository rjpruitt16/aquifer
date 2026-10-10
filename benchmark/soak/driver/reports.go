package main

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// reports holds the monitoring agent's write-ups. Posting needs
// SOAK_REPORT_TOKEN as a bearer token; reading is public like the rest of
// the status pages. Earlier reports double as the agent's memory between
// runs.
type reports struct {
	mu    sync.Mutex
	path  string
	token string
	all   []report
}

type report struct {
	At       time.Time `json:"at"`
	Severity string    `json:"severity"` // ok, warning, problem
	Summary  string    `json:"summary"`
	Body     string    `json:"body"`
}

func openReports(dir string) *reports {
	r := &reports{path: filepath.Join(dir, "reports.jsonl"), token: os.Getenv("SOAK_REPORT_TOKEN")}
	readJSONL(r.path, func(b []byte) {
		var rep report
		if json.Unmarshal(b, &rep) == nil {
			r.all = append(r.all, rep)
		}
	})
	return r
}

func (r *reports) authorized(w http.ResponseWriter, req *http.Request) bool {
	got := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
	if r.token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(r.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (r *reports) handlePost(w http.ResponseWriter, req *http.Request) {
	if !r.authorized(w, req) {
		return
	}
	var rep report
	if err := json.NewDecoder(io.LimitReader(req.Body, 256<<10)).Decode(&rep); err != nil || rep.Summary == "" {
		http.Error(w, `body must be JSON: {"severity":"ok|warning|problem","summary":"...","body":"..."}`, http.StatusBadRequest)
		return
	}
	switch rep.Severity {
	case "ok", "warning", "problem":
	default:
		rep.Severity = "warning"
	}
	rep.At = time.Now().UTC()
	r.mu.Lock()
	r.all = append(r.all, rep)
	r.mu.Unlock()
	appendJSONL(r.path, rep)
	writeJSON(w, map[string]any{"stored": true, "at": rep.At})
}

func (r *reports) handleList(w http.ResponseWriter, req *http.Request) {
	n := queryInt(req, "n", 20, 1000)
	r.mu.Lock()
	out := append([]report(nil), r.all[max(0, len(r.all)-n):]...)
	r.mu.Unlock()
	writeJSON(w, out)
}

func (r *reports) latest() *report {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.all) == 0 {
		return nil
	}
	rep := r.all[len(r.all)-1]
	return &rep
}
