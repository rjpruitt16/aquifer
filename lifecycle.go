// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"time"

	"github.com/rjpruitt16/aquifer/internal/httpx"
)

const nodeStateHeader = httpx.NodeStateHeader

// ErrAquiferDraining is returned for work refused because the node is
// shutting down.
var ErrAquiferDraining = httpx.ErrDraining

type LifecycleState string

const (
	LifecycleStateActive   LifecycleState = "active"
	LifecycleStateDraining LifecycleState = "draining"
	LifecycleStateOffline  LifecycleState = "offline"
)

func (a *Aquifer) BeginDrain(webSocketGrace time.Duration) bool {
	if a == nil || !a.draining.CompareAndSwap(false, true) {
		return false
	}
	if a.webSockets != nil {
		a.webSockets.BeginDrain(webSocketGrace)
	}
	if a.registry != nil {
		a.registry.BeginDrain()
	}
	if a.clusterRouter != nil {
		a.clusterRouter.SetState(LifecycleStateDraining)
	}
	return true
}

func (a *Aquifer) IsDraining() bool {
	return a != nil && a.draining.Load()
}
