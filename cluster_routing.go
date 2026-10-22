// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

// Shared-scope requests route by their dedup identity so every caller asking
// for the same resource lands on the node whose store can coalesce them;
// everything else keeps per-user routing.
func clusterRoutingKey(req JobRequest) string {
	if req.IdempotencyScope == IdempotencyScopeShared {
		return "shared\x00" + req.IdempotentKey
	}
	return req.UserID
}
