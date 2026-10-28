// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package aquifer

// openFileLimit can't be read here; connections and streams are then not
// capped unless configured.
func openFileLimit() int { return 0 }
