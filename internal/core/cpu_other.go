// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package core

import "time"

// processCPUTime can't be read here; the load then assumes an idle CPU.
func processCPUTime() time.Duration { return 0 }
