// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"testing"

	"github.com/rjpruitt16/aquifer/internal/cluster"
)

func TestRegistrationReportsFullness(t *testing.T) {
	r := drainTestRegistry(t, NoopMetricsAdapter{})
	if _, ok := r.registrationPayload(cluster.StateActive)["fullness"]; ok {
		t.Fatalf("expected no fullness before a source is wired")
	}

	r.SetFullnessSource(func() int { return 42 })
	if got := r.registrationPayload(cluster.StateActive)["fullness"]; got != 42 {
		t.Fatalf("expected fullness 42 in the ping, got %v", got)
	}
}
