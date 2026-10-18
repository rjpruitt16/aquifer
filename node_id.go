// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"os"
	"strings"
	"sync"
)

// NodeID names this Aquifer node to the backends it dispatches to
// (X-Aqueduct-Node-Id), so a backend can count how many nodes are sending
// to it and divide its pacing budget between them. It's the first of
// AQUIFER_NODE_ID, AQUIFER_CLUSTER_SELF_ID, FLY_MACHINE_ID and the host
// name that is set.
func NodeID() string {
	nodeIDOnce.Do(func() { nodeID = resolveNodeID(os.Getenv, os.Hostname) })
	return nodeID
}

var (
	nodeIDOnce sync.Once
	nodeID     string
)

func resolveNodeID(getenv func(string) string, hostname func() (string, error)) string {
	for _, name := range []string{"AQUIFER_NODE_ID", "AQUIFER_CLUSTER_SELF_ID", "FLY_MACHINE_ID"} {
		if v := strings.TrimSpace(getenv(name)); v != "" {
			return v
		}
	}
	if h, err := hostname(); err == nil && h != "" {
		return h
	}
	return "unknown"
}
