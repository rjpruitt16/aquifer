// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"
	"time"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

func TestRateMeterAveragesWholeSeconds(t *testing.T) {
	var m rateMeter
	start := time.Unix(1_000_000, 0)
	for s := 0; s < 10; s++ {
		for i := 0; i < 4; i++ {
			m.Mark(start.Add(time.Duration(s) * time.Second))
		}
	}
	if got := m.Rate(start.Add(10 * time.Second)); got != 4 {
		t.Fatalf("rate after 10 s at 4/s = %v, want 4", got)
	}
	// The rate follows a change within the window, and old seconds age out.
	for i := 0; i < 40; i++ {
		m.Mark(start.Add(10 * time.Second))
	}
	if got := m.Rate(start.Add(11 * time.Second)); got != 7.6 {
		t.Fatalf("rate after a burst = %v, want 7.6", got)
	}
	if got := m.Rate(start.Add(30 * time.Second)); got != 0 {
		t.Fatalf("rate after 20 idle seconds = %v, want 0", got)
	}
}

func TestEstimateCountsPlaceInQueue(t *testing.T) {
	q := &AccountQueue{estimates: true}
	var js []*jobs.Job
	for _, id := range []string{"a", "b", "c", "d"} {
		j := &jobs.Job{ID: id}
		js = append(js, j)
		q.issueTicket(j)
	}
	q.markStarted(js[0])
	q.markStarted(js[1])
	if _, ok := q.Estimate("a"); ok {
		t.Fatal("a started job should have no estimate")
	}
	est, ok := q.Estimate("d")
	if !ok || est.Position != 2 {
		t.Fatalf("d: %+v, %v; want position 2", est, ok)
	}
	// A retried job rejoins at the back.
	q.issueTicket(js[0])
	if est, _ := q.Estimate("a"); est.Position != 3 {
		t.Fatalf("retried a: position %d, want 3", est.Position)
	}

	off := &AccountQueue{}
	off.issueTicket(js[2])
	if _, ok := off.Estimate("c"); ok {
		t.Fatal("estimates off: want no estimate")
	}
}
