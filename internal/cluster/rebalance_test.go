// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	"math"
	"testing"
)

// overloadedRouter makes node-a own n active users (all idle between jobs)
// while node-b and node-c report others.
func overloadedRouter(t *testing.T, n, bUsers, cUsers int) (*Router, *fakeClusterStateStore) {
	t.Helper()
	self := Member{ID: "node-a", Address: "http://node-a", Capacity: 100}
	b := Member{ID: "node-b", Address: "http://node-b", Capacity: 100, ActiveUsers: bUsers}
	c := Member{ID: "node-c", Address: "http://node-c", Capacity: 100, ActiveUsers: cUsers}
	store := newFakeClusterStateStore(self, b, c)
	router := coordinatedTestRouter(t, store, self, self, b, c)
	// Everyone lands on node-a, as after b and c failed and came back.
	store.assignOverride = &self
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user-%d", i)
		if owner, _ := router.OwnerFor(key); owner.ID != self.ID {
			t.Fatalf("setup: %s assigned to %s", key, owner.ID)
		}
	}
	store.mu.Lock()
	store.assignOverride = nil
	store.mu.Unlock()
	return router, store
}

func TestOverloadedNodeHandsIdleUsersBackToTheirPreferredNode(t *testing.T) {
	router, store := overloadedRouter(t, 30, 0, 0)
	// user-0 still has a job here and must stay.
	router.TrackLocalJob("user-0")

	router.syncState()

	store.mu.Lock()
	released := append([]string(nil), store.released...)
	store.mu.Unlock()
	if len(released) == 0 {
		t.Fatal("a node with all 30 users of a 3-node cluster handed none back")
	}
	for _, key := range released {
		if key == "user-0" {
			t.Fatal("released a user with a job still running here")
		}
		if preferred := router.RankedOwners(key)[0]; preferred.ID == "node-a" {
			t.Fatalf("released %s, whose preferred node is this one", key)
		}
	}
	// Fair share is 10 of 30; node-a keeps at least that many.
	remaining := router.Snapshot()["active_users"].(int)
	if remaining < int(math.Ceil(30.0/3)) {
		t.Fatalf("node-a kept %d users, below its fair share of 10", remaining)
	}
	t.Logf("handed back %d of 30 users; node-a keeps %d", len(released), remaining)

	// A released user's next request is assigned afresh, to its preferred node.
	key := released[0]
	if owner, _ := router.OwnerFor(key); owner.ID == "node-a" {
		t.Fatalf("%s was assigned back to node-a after its release", key)
	}
}

func TestBalancedClusterMovesNobody(t *testing.T) {
	router, store := overloadedRouter(t, 10, 10, 10)
	router.syncState()
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.released) != 0 {
		t.Fatalf("a node at its fair share released %v", store.released)
	}
}
