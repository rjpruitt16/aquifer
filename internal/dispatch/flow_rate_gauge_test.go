// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import "testing"

// A pool with no live members dispatches nothing, so its gauge reads 0 even
// while its queues still hold their old rates.
func TestFlowRateGaugeIsZeroForAPoolWithNoMembers(t *testing.T) {
	q := &AccountQueue{}
	q.currentRPS.Store(40 * 100)
	q.backlog.Store(5)
	w := &URLWorker{domain: "pool:empty", pool: &Pool{id: "empty", members: map[string]*PoolMember{}}, queues: map[string]*AccountQueue{"q": q}}
	if got := w.Stats().FlowRate; got != 0 {
		t.Fatalf("flow rate for a pool with no members = %v, want 0", got)
	}
}
