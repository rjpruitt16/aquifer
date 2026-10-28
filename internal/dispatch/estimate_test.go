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
	if est, _ := q.Estimate("a"); est.Position != 0 || est.Seconds != -1 {
		t.Fatalf("started job a: %+v, want no place in line and no run time yet", est)
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

func TestPollAfterErrsEarly(t *testing.T) {
	for i := 0; i < 200; i++ {
		if d := PollAfter(10); d < 8*time.Second || d > 10*time.Second {
			t.Fatalf("PollAfter(10) = %v, want 8-10 s", d)
		}
	}
	if d := PollAfter(0.01); d != minPollAfter {
		t.Fatalf("PollAfter(0.01) = %v, want %v", d, minPollAfter)
	}
	if d := PollAfter(600); d != MaxPollAfter {
		t.Fatalf("PollAfter(600) = %v, want %v", d, MaxPollAfter)
	}
}

func TestRateMeterWarmsUpOverSecondsRunning(t *testing.T) {
	var m rateMeter
	start := time.Unix(2_000_000, 0)
	for i := 0; i < 6; i++ {
		m.Mark(start.Add(time.Duration(i%2) * time.Second))
	}
	// 6 events over the 2 whole seconds since they began: 3 per second,
	// not 6 spread over the full 10-second window.
	if got := m.Rate(start.Add(2 * time.Second)); got != 3 {
		t.Fatalf("rate 2 s after starting = %v, want 3", got)
	}
}

func TestFarBackStreamGetsPollEvent(t *testing.T) {
	q := &AccountQueue{estimates: true, streamMaxWait: 1}
	q.starts.Mark(time.Now().Add(-2 * time.Second))
	q.starts.Mark(time.Now().Add(-2 * time.Second))
	q.starts.Mark(time.Now().Add(-1 * time.Second))
	q.starts.Mark(time.Now().Add(-1 * time.Second))
	q.runs.Observe(500 * time.Millisecond)
	if ev := q.pollEvent("near", 0); ev != nil {
		t.Fatalf("job at the front got %v", ev)
	}
	ev := q.pollEvent("far", 6)
	if ev == nil || ev.Event != "poll" {
		t.Fatalf("job 6 back at 2/s: got %v, want a poll event", ev)
	}
	// 6 / 2 per second + 0.5 s run = 3.5 s; come back at 80-100% of it.
	if ms := ev.Data["retry_after_ms"].(int64); ms < 2800 || ms > 3500 {
		t.Fatalf("retry_after_ms = %d, want 2800-3500", ms)
	}
}
