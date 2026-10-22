// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestClusterHTTPJobLifecycleReleasesActiveUser(t *testing.T) {
	app, jobStore := testClusterAquifer(t)
	self := ClusterMember{ID: "node-a", Address: "http://node-a", Capacity: 10}
	stateStore := &releaseRecorder{}
	// Scopes go idle 15ms after their last job, and the state loop runs
	// every 20ms, so a finished user is released within a few ticks.
	router := NewClusterRouter(ClusterConfig{
		Enabled:           true,
		Provider:          "valkey",
		Self:              self,
		PruneTTL:          time.Minute,
		HeartbeatInterval: 20 * time.Millisecond,
		InstanceTTL:       time.Minute,
		AssignmentIdle:    15 * time.Millisecond,
		RequestTimeout:    time.Second,
		StateStore:        stateStore,
	})
	t.Cleanup(router.Close)
	app.SetClusterRouter(router)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(webhook.Close)

	body, err := json.Marshal(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "cluster-lifecycle",
		URL:           upstream.URL,
		Method:        http.MethodPost,
		WebhookURL:    webhook.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	NewServer(app).Routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected job acceptance, got %d: %s", recorder.Code, recorder.Body.String())
	}
	if owner := recorder.Header().Get("X-Aquifer-Cluster-Owner"); owner != self.ID {
		t.Fatalf("expected local owner header %q, got %q", self.ID, owner)
	}
	if !waitUntil(2*time.Second, func() bool { return jobStore.Counts().TotalJobs == 0 }) {
		t.Fatalf("job and completion webhook did not finish: %+v", jobStore.Counts())
	}

	if !waitUntil(2*time.Second, func() bool { return len(stateStore.Released()) > 0 }) {
		t.Fatal("completed HTTP lifecycle never released its user")
	}
	if released := stateStore.Released(); len(released) != 1 || released[0] != "user-1" {
		t.Fatalf("expected completed HTTP lifecycle to release user-1, got %v", released)
	}
}

// releaseRecorder is a single-node ClusterStateStore that assigns every
// scope to the first candidate and records releases.
type releaseRecorder struct {
	mu       sync.Mutex
	self     ClusterMember
	released []string
}

func (s *releaseRecorder) Heartbeat(_ context.Context, member ClusterMember, _ time.Duration) error {
	s.mu.Lock()
	s.self = member
	s.mu.Unlock()
	return nil
}

func (s *releaseRecorder) Members(context.Context) ([]ClusterMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self.ID == "" {
		return nil, nil
	}
	return []ClusterMember{s.self}, nil
}

func (s *releaseRecorder) Assign(_ context.Context, _ string, candidates []ClusterMember, _ time.Duration) (ClusterMember, error) {
	if len(candidates) == 0 {
		return ClusterMember{}, nil
	}
	return candidates[0], nil
}

func (s *releaseRecorder) Renew(context.Context, []string, string, time.Duration) error { return nil }

func (s *releaseRecorder) Release(_ context.Context, scope, _ string) error {
	s.mu.Lock()
	s.released = append(s.released, scope)
	s.mu.Unlock()
	return nil
}

func (s *releaseRecorder) RemoveInstance(context.Context, string) error { return nil }

func (s *releaseRecorder) Close() error { return nil }

func (s *releaseRecorder) Released() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.released...)
}
