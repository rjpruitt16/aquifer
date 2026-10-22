// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"testing"
	"time"
)

func testClusterRouter(self Member, members ...Member) *Router {
	return NewRouter(Config{
		Enabled:  true,
		Self:     self,
		Members:  members,
		PruneTTL: 30 * time.Second,
	})
}

func keyOwnedBy(t *testing.T, router *Router, ownerID string) string {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := "user-" + strconvItoa(i)
		owner, ok := router.OwnerFor(key)
		if ok && owner.ID == ownerID {
			return key
		}
	}
	t.Fatalf("could not find a key owned by %s", ownerID)
	return ""
}

func keyRankedBy(t *testing.T, router *Router, firstID, secondID string) string {
	t.Helper()

	for i := 0; i < 10000; i++ {
		key := "user-" + strconvItoa(i)
		ranked := router.RankedOwners(key)
		if len(ranked) >= 2 && ranked[0].ID == firstID && ranked[1].ID == secondID {
			return key
		}
	}
	t.Fatalf("could not find a key ranked %s then %s", firstID, secondID)
	return ""
}

func TestClusterRouterLocatesStableOwner(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a"}
	other := Member{ID: "node-b", Address: "http://node-b"}
	router := testClusterRouter(self, self, other)

	first, ok := router.OwnerFor("tenant-1")
	if !ok {
		t.Fatal("expected an owner")
	}
	second, ok := router.OwnerFor("tenant-1")
	if !ok {
		t.Fatal("expected an owner on repeat lookup")
	}
	if first.ID != second.ID {
		t.Fatalf("expected stable owner, got %q then %q", first.ID, second.ID)
	}
}

func TestClusterRouterRanksOwnersStably(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a"}
	router := testClusterRouter(
		self,
		self,
		Member{ID: "node-b", Address: "http://node-b"},
		Member{ID: "node-c", Address: "http://node-c"},
	)

	first := router.RankedOwners("tenant-1")
	second := router.RankedOwners("tenant-1")
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("expected three ranked owners, got %d and %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("expected stable ranking, got %+v then %+v", first, second)
		}
	}
}

func TestClusterRouterSoftPrunesMember(t *testing.T) {
	self := Member{ID: "node-a", Address: "http://node-a"}
	other := Member{ID: "node-b", Address: "http://node-b"}
	router := NewRouter(Config{
		Enabled:  true,
		Self:     self,
		Members:  []Member{self, other},
		PruneTTL: time.Hour,
	})
	key := keyOwnedBy(t, router, other.ID)

	router.PruneMember(other.ID)

	owner, ok := router.OwnerFor(key)
	if !ok {
		t.Fatal("expected an owner after pruning")
	}
	if owner.ID == other.ID {
		t.Fatalf("expected pruned member not to own key, got %q", owner.ID)
	}
	if router.prunedCount() != 1 {
		t.Fatalf("expected one pruned member, got %d", router.prunedCount())
	}
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
