// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import "testing"

func TestMemoryRefusalIsSoftOnlyBelowTheHardLimit(t *testing.T) {
	soft := NewController(Limits{MemoryLimitMB: 1, MemoryHardLimitPercent: 1_000_000_000}, "")
	if d := soft.Check(); d.Allowed || d.Reason != "memory" || !d.Soft {
		t.Fatalf("over the limit, under the hard limit: %+v, want a soft memory refusal", d)
	}
	hard := NewController(Limits{MemoryLimitMB: 1, MemoryHardLimitPercent: 115}, "")
	if d := hard.Check(); d.Allowed || d.Reason != "memory" || d.Soft {
		t.Fatalf("over the hard limit: %+v, want a hard memory refusal", d)
	}
	unset := NewController(Limits{MemoryLimitMB: 1}, "")
	if d := unset.Check(); d.Soft {
		t.Fatalf("with no hard limit set, memory refuses at the limit as before: %+v", d)
	}
}
