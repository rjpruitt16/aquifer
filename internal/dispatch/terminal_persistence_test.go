// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"bytes"
	"context"
	"errors"
	"github.com/rjpruitt16/aquifer/internal/jobs"
	"github.com/rjpruitt16/aquifer/internal/storage"
	"github.com/rjpruitt16/aquifer/l8"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

type rejectedFinishStore struct {
	storage.Store
	calls int
}

func (s *rejectedFinishStore) Finish(string, jobs.Status, *jobs.Result) error {
	s.calls++
	return errors.New("injected persistence failure")
}

func TestTerminalPersistenceFailureStillDeliversResponse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		w.Write([]byte("actual response"))
	}))
	defer up.Close()
	dir := t.TempDir()
	base := storage.NewSQLite(filepath.Join(dir, "jobs"))
	defer base.Close()
	store := &rejectedFinishStore{Store: base}
	req := retryJob("finish", up.URL, "https://example.com/hook", nil)
	job := jobs.New(&req)
	store.CheckOrInsert(job)
	registry := l8.NewRegistry(filepath.Join(dir, "key"), filepath.Join(dir, "trust"))
	defer registry.Close()
	var output bytes.Buffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	var delivered map[string]any
	execute(context.Background(), job, up.URL, up.URL, store, NewBroker(), registry, NoopMetricsAdapter{}, nil, nil, 1, 1, 0,
		func(_ string, _ string, _ string, _ int, payload map[string]any) { delivered = payload }, nil)
	if store.calls != 1 {
		t.Fatalf("finish calls: %d", store.calls)
	}
	if delivered["body"] != "actual response" || delivered["status"] != "completed" {
		t.Fatalf("lost outcome: %+v", delivered)
	}
	if !strings.Contains(output.String(), "injected persistence failure") {
		t.Fatal("persistence failure not reported")
	}
	if got := store.GetJob(job.ID); got.Status != jobs.StatusQueued {
		t.Fatalf("partial completion: %+v", got)
	}
	if _, ok := store.GetResult(job.ID); ok {
		t.Fatal("unexpected persisted result")
	}
}
