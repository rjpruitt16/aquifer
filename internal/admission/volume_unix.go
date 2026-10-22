// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package admission

import "syscall"

// volumeAvailableBytes reports the space an unprivileged process can still
// write on the filesystem holding dir.
func volumeAvailableBytes(dir string) (int64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return int64(st.Bavail) * int64(st.Bsize), true
}
