// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A reboot kills Aquifer 200ms after the driver asks for it. A job accepted
// in that gap was lost to the flush window like any kill's, not lost outright
// (aquifer#67: jobs accepted 168-191ms after the request were counted as lost).
func TestRebootRestartCountsJobsAcceptedBeforeTheKill(t *testing.T) {
	requested := time.UnixMilli(1_792_336_828_671)
	accepted := requested.Add(180 * time.Millisecond).UnixMilli()

	l := openLedger(t.TempDir())
	l.recordRestart(restartTime("reboot", requested))
	if !l.acceptedJustBeforeRestart(accepted) {
		t.Fatal("job accepted 20ms before the reboot's kill was not counted as a flush-window loss")
	}

	// A plain kill is sent before /kill answers, so it keeps the request time.
	k := openLedger(t.TempDir())
	k.recordRestart(restartTime("kill", requested))
	if k.acceptedJustBeforeRestart(accepted) {
		t.Fatal("job accepted 180ms after a kill was excused as a flush-window loss")
	}
}

// recent_lost shows only the last 20 lost jobs. In canalis soak 2, 112 were
// lost at once and 92 could no longer be traced, so every one goes to
// lost.jsonl too, with the node that accepted it.
func TestEveryLostJobIsWrittenDown(t *testing.T) {
	dir := t.TempDir()
	l := openLedger(dir)
	now := time.Now()
	for seq := uint64(1); seq <= 25; seq++ {
		l.acceptedOn(seq, fmt.Sprintf("j%d", seq), "b", now.Add(-time.Hour))
		l.resolve(seq, "", now)
	}
	if got := len(l.view().RecentLost); got != 20 {
		t.Fatalf("recent_lost: got %d, want 20", got)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "lost.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 25 {
		t.Fatalf("lost.jsonl: got %d lines, want 25", len(lines))
	}
	var first lostJob
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first.JobID != "j1" || first.Node != "b" || first.Kind != "lost" || first.LostAt == "" {
		t.Fatalf("first line: %+v", first)
	}
}
