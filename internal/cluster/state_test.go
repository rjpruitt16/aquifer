// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type fakeClusterStateStore struct {
	mu             sync.Mutex
	members        map[string]Member
	assignments    map[string]Member
	assignOverride *Member
	assignErr      error
	releaseErr     error
	heartbeats     []Member
	renewed        []string
	released       []string
	removed        []string
}

func newFakeClusterStateStore(members ...Member) *fakeClusterStateStore {
	store := &fakeClusterStateStore{
		members:     make(map[string]Member, len(members)),
		assignments: make(map[string]Member),
	}
	for _, member := range members {
		store.members[member.ID] = member
	}
	return store
}

func (s *fakeClusterStateStore) Heartbeat(_ context.Context, member Member, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members[member.ID] = member
	s.heartbeats = append(s.heartbeats, member)
	return nil
}

func (s *fakeClusterStateStore) Members(context.Context) ([]Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	members := make([]Member, 0, len(s.members))
	for _, member := range s.members {
		members = append(members, member)
	}
	return members, nil
}

func (s *fakeClusterStateStore) Assign(_ context.Context, scope string, candidates []Member, _ time.Duration) (Member, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.assignErr != nil {
		return Member{}, s.assignErr
	}
	if s.assignOverride != nil {
		s.assignments[scope] = *s.assignOverride
		return *s.assignOverride, nil
	}
	if assigned, ok := s.assignments[scope]; ok {
		return assigned, nil
	}
	if len(candidates) == 0 {
		return Member{}, nil
	}
	s.assignments[scope] = candidates[0]
	return candidates[0], nil
}

func (s *fakeClusterStateStore) Renew(_ context.Context, scopes []string, _ string, _ time.Duration) error {
	s.mu.Lock()
	s.renewed = append(s.renewed, scopes...)
	s.mu.Unlock()
	return nil
}

func (s *fakeClusterStateStore) Release(_ context.Context, scope, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.releaseErr != nil {
		return s.releaseErr
	}
	if assigned, ok := s.assignments[scope]; ok && assigned.ID == nodeID {
		delete(s.assignments, scope)
	}
	s.released = append(s.released, scope)
	return nil
}

func (s *fakeClusterStateStore) RemoveInstance(_ context.Context, nodeID string) error {
	s.mu.Lock()
	delete(s.members, nodeID)
	s.removed = append(s.removed, nodeID)
	s.mu.Unlock()
	return nil
}

func (s *fakeClusterStateStore) Close() error { return nil }

func coordinatedTestRouter(t *testing.T, store StateStore, self Member, members ...Member) *Router {
	t.Helper()
	router := NewRouter(Config{
		Enabled:           true,
		Provider:          clusterProviderValkey,
		Self:              self,
		Members:           members,
		PruneTTL:          time.Minute,
		HeartbeatInterval: time.Hour,
		InstanceTTL:       3 * time.Hour,
		AssignmentIdle:    time.Minute,
		RequestTimeout:    time.Second,
		StateStore:        store,
	})
	t.Cleanup(router.Close)
	return router
}

func TestClusterRouterUsesAssignmentReturnedByCoordinator(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 10}
	assigned := Member{ID: "node-b", Address: "http://node-b", Capacity: 10}
	store := newFakeClusterStateStore()
	store.assignOverride = &assigned
	router := coordinatedTestRouter(t, store, self)

	owner, ok := router.OwnerFor("user-1")
	if !ok || owner.ID != assigned.ID || owner.Address != assigned.Address {
		t.Fatalf("expected coordinator owner %+v, got %+v (ok=%v)", assigned, owner, ok)
	}
}

func TestClusterRouterFallsBackToCachedRendezvousWhenCoordinatorFails(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 10}
	peer := Member{ID: "node-b", Address: "http://node-b", Capacity: 10}
	store := newFakeClusterStateStore(self, peer)
	store.assignErr = errors.New("valkey unavailable")
	router := coordinatedTestRouter(t, store, self, peer)
	want := router.RankedOwners("user-1")[0]

	owner, ok := router.OwnerFor("user-1")
	if !ok || owner.ID != want.ID {
		t.Fatalf("expected cached rendezvous owner %q, got %+v (ok=%v)", want.ID, owner, ok)
	}
	if failures := router.Snapshot()["coordination_failures"].(int64); failures != 1 {
		t.Fatalf("expected one coordination failure, got %d", failures)
	}
}

func TestClusterRouterReleasesOnlyCompletedIdleUsers(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 10}
	store := newFakeClusterStateStore(self)
	router := coordinatedTestRouter(t, store, self)
	router.idleTTL = 15 * time.Millisecond

	owner, ok := router.OwnerFor("user-1")
	if !ok || owner.ID != self.ID {
		t.Fatalf("expected local ownership, got %+v (ok=%v)", owner, ok)
	}
	router.TrackLocalJob("user-1")
	time.Sleep(20 * time.Millisecond)
	router.syncState()

	store.mu.Lock()
	releasedWhileActive := len(store.released)
	store.mu.Unlock()
	if releasedWhileActive != 0 {
		t.Fatalf("active work must retain its assignment, got %d releases", releasedWhileActive)
	}

	router.CompleteLocalJob("user-1")
	time.Sleep(20 * time.Millisecond)
	router.syncState()

	store.mu.Lock()
	released := append([]string(nil), store.released...)
	store.mu.Unlock()
	if len(released) != 1 || released[0] != "user-1" {
		t.Fatalf("expected the idle user assignment to be released once, got %v", released)
	}
	if active := router.Snapshot()["active_users"].(int); active != 0 {
		t.Fatalf("expected no active users after release, got %d", active)
	}
}

func TestLoadClusterConfigValkeyProvider(t *testing.T) {
	t.Setenv("AQUIFER_CLUSTER_ENABLED", "true")
	t.Setenv("AQUIFER_CLUSTER_PROVIDER", "valkey")
	t.Setenv("AQUIFER_CLUSTER_SELF_ID", "aquifer-a")
	t.Setenv("AQUIFER_CLUSTER_SELF_ADDR", "http://aquifer-a:8080")
	t.Setenv("AQUIFER_CLUSTER_VALKEY_URL", "valkey://valkey:6379")
	t.Setenv("AQUIFER_CLUSTER_VALKEY_PREFIX", "aqueduct:test")
	t.Setenv("AQUIFER_CLUSTER_MAX_ACTIVE_USERS", "250")
	t.Setenv("AQUIFER_CLUSTER_HEARTBEAT_SECONDS", "2")
	t.Setenv("AQUIFER_CLUSTER_INSTANCE_TTL_SECONDS", "9")
	t.Setenv("AQUIFER_CLUSTER_ASSIGNMENT_IDLE_SECONDS", "30")
	t.Setenv("AQUIFER_CLUSTER_VALKEY_TIMEOUT_MS", "75")

	cfg := LoadConfig()
	if !cfg.Enabled || cfg.Provider != clusterProviderValkey {
		t.Fatalf("expected enabled Valkey cluster config, got %+v", cfg)
	}
	if cfg.Self.ID != "aquifer-a" || cfg.Self.Address != "http://aquifer-a:8080" || cfg.Self.Capacity != 250 {
		t.Fatalf("unexpected self config: %+v", cfg.Self)
	}
	if cfg.ValkeyURL != "valkey://valkey:6379" || cfg.ValkeyPrefix != "aqueduct:test" {
		t.Fatalf("unexpected Valkey config: URL=%q prefix=%q", cfg.ValkeyURL, cfg.ValkeyPrefix)
	}
	if cfg.HeartbeatInterval != 2*time.Second || cfg.InstanceTTL != 9*time.Second || cfg.AssignmentIdle != 30*time.Second || cfg.RequestTimeout != 75*time.Millisecond {
		t.Fatalf("unexpected timing config: %+v", cfg)
	}
}

func TestClusterRouterPublishesDrainingStateImmediately(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 10}
	store := newFakeClusterStateStore(self)
	router := coordinatedTestRouter(t, store, self)

	router.SetState(StateDraining)
	store.mu.Lock()
	lastHeartbeat := store.heartbeats[len(store.heartbeats)-1]
	store.mu.Unlock()
	if lastHeartbeat.State != string(StateDraining) {
		t.Fatalf("expected draining heartbeat, got %+v", lastHeartbeat)
	}
	if owners := router.RankedOwners("user-1"); len(owners) != 0 {
		t.Fatalf("draining self must not remain locally assignable, got %+v", owners)
	}
}

func TestValkeyClusterConcurrentClaimsCapacityAndDraining(t *testing.T) {
	rawURL := os.Getenv("AQUIFER_TEST_VALKEY_URL")
	if rawURL == "" {
		t.Skip("set AQUIFER_TEST_VALKEY_URL to run the real Valkey integration test")
	}

	prefix := "aqueduct:test:" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":"
	store, err := NewValkeyStateStore(rawURL, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer func() {
		keys, _ := store.client.Keys(context.Background(), prefix+"*").Result()
		if len(keys) > 0 {
			_ = store.client.Del(context.Background(), keys...).Err()
		}
	}()

	members := []Member{
		{ID: "node-a", Address: "http://node-a", State: string(StateActive), Capacity: 1},
		{ID: "node-b", Address: "http://node-b", State: string(StateActive), Capacity: 1},
		{ID: "node-c", Address: "http://node-c", State: string(StateActive), Capacity: 1},
	}
	for _, member := range members {
		if err := store.Heartbeat(ctx, member, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	const claimers = 32
	owners := make(chan string, claimers)
	errCh := make(chan error, claimers)
	var wg sync.WaitGroup
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner, err := store.Assign(ctx, "same-user", members, time.Minute)
			if err != nil {
				errCh <- err
				return
			}
			owners <- owner.ID
		}()
	}
	wg.Wait()
	close(errCh)
	close(owners)
	for err := range errCh {
		t.Fatal(err)
	}
	for owner := range owners {
		if owner != "node-a" {
			t.Fatalf("concurrent claims diverged: expected node-a, got %q", owner)
		}
	}

	// A stale node view must still honor an existing healthy assignment.
	owner, err := store.Assign(ctx, "same-user", members[1:], time.Minute)
	if err != nil || owner.ID != "node-a" {
		t.Fatalf("expected sticky assignment to node-a, got %+v, err=%v", owner, err)
	}

	owner, err = store.Assign(ctx, "second-user", members, time.Minute)
	if err != nil || owner.ID != "node-b" {
		t.Fatalf("expected capacity spillover to node-b, got %+v, err=%v", owner, err)
	}

	draining := members[0]
	draining.State = string(StateDraining)
	if err := store.Heartbeat(ctx, draining, time.Minute); err != nil {
		t.Fatal(err)
	}
	owner, err = store.Assign(ctx, "same-user", members, time.Minute)
	if err != nil || owner.ID != "node-c" {
		t.Fatalf("expected draining owner to move to node-c, got %+v, err=%v", owner, err)
	}

	// Availability wins once every active node is at capacity.
	owner, err = store.Assign(ctx, "overflow-user", members, time.Minute)
	if err != nil || owner.ID != "node-b" {
		t.Fatalf("expected least-loaded active fallback node-b, got %+v, err=%v", owner, err)
	}
}

// While the store is failing, idle scopes are still forgotten locally: their
// assignments expire in the store on their own, and keeping them here would
// grow by one entry per user for as long as the outage lasts.
func TestClusterRouterForgetsIdleScopesWhileTheStoreFails(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 1000}
	store := newFakeClusterStateStore(self)
	store.assignErr = errors.New("valkey down")
	store.releaseErr = errors.New("valkey down")
	router := coordinatedTestRouter(t, store, self)
	router.idleTTL = time.Millisecond

	for i := range 50 {
		key := fmt.Sprintf("user-%d", i)
		if owner, ok := router.ResolveOwner(key, nil); !ok || owner.ID != self.ID {
			t.Fatalf("%s: want local owner while the store is down, got %+v %v", key, owner, ok)
		}
	}
	if got := router.Snapshot()["tracked_scopes"]; got != 50 {
		t.Fatalf("want 50 tracked scopes before they go idle, got %v", got)
	}
	time.Sleep(5 * time.Millisecond)
	router.syncState()
	if got := router.Snapshot()["tracked_scopes"]; got != 0 {
		t.Fatalf("idle scopes should be forgotten even when Release fails, got %v", got)
	}
}
