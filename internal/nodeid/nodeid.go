// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// Package nodeid names this Aquifer node.
package nodeid

import (
	"os"
	"strings"
	"sync"
)

// Get names this Aquifer node to the backends it dispatches to
// (X-Aqueduct-Node-Id), so a backend can count how many nodes are sending
// to it and divide its pacing budget between them. It's the first of
// AQUIFER_NODE_ID, AQUIFER_CLUSTER_SELF_ID, FLY_MACHINE_ID and the host
// name that is set.
func Get() string {
	once.Do(func() { id = Resolve(os.Getenv, os.Hostname) })
	return id
}

var (
	once sync.Once
	id   string
)

// Resolve is Get without the caching, for tests.
func Resolve(getenv func(string) string, hostname func() (string, error)) string {
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
