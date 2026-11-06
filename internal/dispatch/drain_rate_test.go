// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"
)

func TestBacklogStuckComparesBacklogWithTheDrainRate(t *testing.T) {
	w := &URLWorker{queues: map[string]*AccountQueue{}}
	q := &AccountQueue{}
	w.queues["q"] = q
	now := time.Now()
	limit := 60 * time.Second

	w.backlog.Store(100)
	if w.BacklogStuck(limit) {
		t.Fatal("a target not yet measured must not count as stuck")
	}
	w.sampleDrain(now)
	q.served.Add(30) // 10 jobs/s over 3 s: 100 queued drain in 10 s
	w.sampleDrain(now.Add(3 * time.Second))
	if w.BacklogStuck(limit) {
		t.Fatal("a backlog that drains in about 10 s is not stuck")
	}

	// Nothing finishes for a while: the smoothed rate falls toward 0.
	for i := 2; i < 12; i++ {
		w.sampleDrain(now.Add(time.Duration(3*i) * time.Second))
	}
	if !w.BacklogStuck(limit) {
		t.Fatal("a backlog that stopped draining should be stuck")
	}
	w.backlog.Store(0)
	if w.BacklogStuck(limit) {
		t.Fatal("no backlog is never stuck")
	}
}
