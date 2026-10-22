// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import "github.com/rjpruitt16/aquifer/internal/cluster"

// Cluster membership and routing live in internal/cluster. These names keep
// the API embedding applications already use.
type (
	ClusterMember           = cluster.Member
	ClusterStateStore       = cluster.StateStore
	ClusterConfig           = cluster.Config
	ClusterRouter           = cluster.Router
	ValkeyClusterStateStore = cluster.ValkeyStateStore
)

func LoadClusterConfig() ClusterConfig { return cluster.LoadConfig() }

func NewClusterRouter(cfg ClusterConfig) *ClusterRouter { return cluster.NewRouter(cfg) }

func NewValkeyClusterStateStore(rawURL, prefix string) (*ValkeyClusterStateStore, error) {
	return cluster.NewValkeyStateStore(rawURL, prefix)
}
