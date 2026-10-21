// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/nodeid"

// NodeID names this Aquifer node to the backends it dispatches to
// (X-Aqueduct-Node-Id). It's the first of AQUIFER_NODE_ID,
// AQUIFER_CLUSTER_SELF_ID, FLY_MACHINE_ID and the host name that is set.
func NodeID() string { return nodeid.Get() }
