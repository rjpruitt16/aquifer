// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"syscall"
)

// openFiles is how many files the driver has open against its limit: a
// driver out of file descriptors fails its own connections, which counts
// as client errors.
func openFiles() map[string]uint64 {
	out := map[string]uint64{}
	var lim syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim) == nil {
		out["limit"] = lim.Cur
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		out["open"] = uint64(len(entries))
	}
	return out
}
