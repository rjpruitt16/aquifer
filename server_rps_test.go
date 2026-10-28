// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "testing"

// A direct /proxy response carries one X-Aqueduct-Rps: the lower of
// Aquifer's own and the target's, so a caller reading the header honors
// whichever is tighter.
func TestLowerRPS(t *testing.T) {
	for _, tc := range []struct{ ours, target, want string }{
		{"160.00", "5", "5"},
		{"2.00", "50", "2.00"},
		{"160.00", "", "160.00"},
		{"", "7", "7"},
		{"160.00", "garbage", "160.00"},
		{"", "", ""},
	} {
		if got := lowerRPS(tc.ours, tc.target); got != tc.want {
			t.Errorf("lowerRPS(%q, %q) = %q, want %q", tc.ours, tc.target, got, tc.want)
		}
	}
}
