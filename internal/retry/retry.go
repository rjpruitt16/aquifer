// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package retry has the backoff helpers shared by Aquifer's retry loops.
package retry

import (
	"context"
	"math/rand"
	"time"
)

const jitterRatio = 0.10

// Jitter adds up to 10% to d, so many clients backing off together don't
// retry in lockstep.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	ceiling := time.Duration(float64(d) * jitterRatio)
	if ceiling <= 0 {
		ceiling = time.Nanosecond
	}
	return d + time.Duration(rand.Int63n(int64(ceiling)+1))
}

// Sleep waits for d and reports true, or returns false as soon as ctx is
// done.
func Sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
