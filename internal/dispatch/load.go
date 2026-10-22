// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

// Load is how many HTTP users and WebSocket sessions this instance has, and
// how many of each it can hold, for whatever is in front of it (Canalis):
// reported in the registration ping and on every response. See
// REGISTRATION.md.
//
// Both kinds of traffic spend the same CPU, so they share one budget:
// whatever one isn't using, the other can have. The CPU in use is split
// between the two by what each costs (benchmark.md: about 750 jobs/s and
// 1,500 WebSocket round trips/s per CPU, which is 3,000 messages: a client
// message and a backend one), which gives the CPU one user or one session
// costs, and:
//
//	headroom      = 80% - CPU in use
//	http_capacity = users + headroom / CPU per user
//	ws_capacity   = least of: sessions + headroom / CPU per session,
//	                sessions + memory left under AQUIFER_MEMORY_LIMIT_MB / 90 KB,
//	                AQUIFER_WS_MAX_CLIENT_CONNECTIONS
//
// Neither is ever below what the instance already has. With nothing to
// measure yet, a user is taken to send 10 jobs/s and a session 1 message/s.
// Spillway reports the same four numbers the same way, with its own costs.
type Load struct {
	HTTPUsers    int64 `json:"http_users"`
	HTTPCapacity int64 `json:"http_capacity"`
	WSSessions   int64 `json:"ws_sessions"`
	WSCapacity   int64 `json:"ws_capacity"`
}
