// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
	"time"
)

func TestParsePhases(t *testing.T) {
	got, err := parsePhases("15m@500, 2h@1500,45m@300")
	if err != nil {
		t.Fatal(err)
	}
	want := []stormPhase{{15 * time.Minute, 500}, {2 * time.Hour, 1500}, {45 * time.Minute, 300}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("phase %d: got %v, want %v", i, got[i], want[i])
		}
	}
	for _, bad := range []string{"2h", "x@500", "2h@lots"} {
		if _, err := parsePhases(bad); err == nil {
			t.Errorf("%q: want an error", bad)
		}
	}
}

func TestReceiverClassMix(t *testing.T) {
	var n [numClasses]int
	for i := 0; i < 200; i++ {
		n[receiverClass(i)]++
	}
	if n[classHealthy] != 140 || n[classSlow] != 30 || n[classFlaky] != 20 || n[classDead] != 10 {
		t.Fatalf("got %v", n)
	}
}

func TestJobsSpreadOverReceiverPorts(t *testing.T) {
	s := newStorm(config{Self: "http://aquifer-storm-driver.internal:8080", Receivers: 3, BodyKB: 2}, nil)
	if got := s.jobURL("http://x:8080", 4); got != "http://aquifer-storm-driver.internal:9001/work?s=4" {
		t.Fatalf("got %s", got)
	}
	if s.classOfSeq(4) != receiverClass(1) {
		t.Fatal("class follows the receiver index")
	}
	if b := s.body(); !strings.HasPrefix(b, `,"body":"`) || len(b) > 2*1024+11 {
		t.Fatalf("body %q...", b[:20])
	}

	var off *storm
	if got := off.jobURL("http://d:8080", 7); got != "http://d:8080/work?s=7" {
		t.Fatalf("without a storm: %s", got)
	}
	if off.body() != "" || off.status() != nil {
		t.Fatal("without a storm there's no body and no status")
	}
}
