// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package aquifer

import (
	"math"
	"syscall"
)

// openFileLimit is the process's limit on open files (RLIMIT_NOFILE), which
// Go raises to the hard limit at startup; 0 if it can't be read.
func openFileLimit() int {
	var r syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r); err != nil {
		return 0
	}
	return int(min(r.Cur, math.MaxInt32))
}
