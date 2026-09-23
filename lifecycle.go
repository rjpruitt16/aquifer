package aquifer

import (
	"errors"
	"time"
)

const nodeStateHeader = "X-Aqueduct-Node-State"

var ErrAquiferDraining = errors.New("aquifer is draining")

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
	return true
}

func (a *Aquifer) IsDraining() bool {
	return a != nil && a.draining.Load()
}
