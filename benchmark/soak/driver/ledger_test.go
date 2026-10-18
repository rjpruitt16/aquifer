// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
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
