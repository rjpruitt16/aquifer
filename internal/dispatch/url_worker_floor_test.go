// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import "testing"

// A shrink keeps the backlog held at that moment as the floor; a raise drops
// it, so a later shrink doesn't reuse an earlier, larger floor.
func TestSetMaxBacklogFloorFollowsTheLatestShrink(t *testing.T) {
	w := &URLWorker{}
	w.maxBacklog.Store(20)
	w.backlog.Store(15)

	w.setMaxBacklog(4)
	if got := w.shrinkFloor.Load(); got != 15 {
		t.Fatalf("floor after shrinking to 4 with 15 held: got %d, want 15", got)
	}

	w.backlog.Store(10)
	w.setMaxBacklog(20)
	if got := w.shrinkFloor.Load(); got != 0 {
		t.Fatalf("floor after raising the budget: got %d, want 0", got)
	}

	w.setMaxBacklog(8)
	if got := w.shrinkFloor.Load(); got != 10 {
		t.Fatalf("floor after shrinking to 8 with 10 held: got %d, want 10", got)
	}

	// Shrinking to a budget that already covers what's held sets no floor.
	w.setMaxBacklog(20)
	w.setMaxBacklog(12)
	if got := w.shrinkFloor.Load(); got != 0 {
		t.Fatalf("floor after shrinking to 12 with 10 held: got %d, want 0", got)
	}
}
