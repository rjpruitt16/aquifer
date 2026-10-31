// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package admission

import (
	"math"
	"math/rand"
	"os"
	"strconv"
)

const (
	defaultMaxPendingPerUpstream int64 = 10_000
)

// FairStart is the share of a target API's backlog budget at which fair
// admission starts turning work away.
const FairStart = 0.70

// FairCeiling is how far past its budget a target API's backlog may grow
// while queues at or below their fair share keep entering. Past it, every
// request is refused. After a target API shrinks the budget, it applies to
// the backlog held at that moment instead (see DecideFairWithFloor).
const FairCeiling = 1.25

// QueueSnapshot is the per-target-API state used for fair admission and
// returned to callers as response headers. Counts include the request being
// evaluated, so an accepted response describes the queue after admission.
type QueueSnapshot struct {
	ActiveQueues         int     `json:"active"`
	UpstreamBacklog      int64   `json:"backlog"`
	QueueBacklog         int64   `json:"queue_backlog,omitempty"`
	MaxBacklog           int64   `json:"max_backlog,omitempty"`
	AdmissionPressure    float64 `json:"admission_pressure"`
	RejectionProbability float64 `json:"-"`
}

type FairDecision struct {
	Allowed  bool
	Reason   string
	Snapshot QueueSnapshot
}

func MaxPendingPerUpstream() int64 {
	raw := os.Getenv("AQUIFER_MAX_PENDING_PER_UPSTREAM")
	if raw == "" {
		return defaultMaxPendingPerUpstream
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return defaultMaxPendingPerUpstream
	}
	return value
}

func DecideFair(queuePending, totalPending int64, activeQueues int, maxBacklog int64, draw float64) FairDecision {
	return DecideFairWithFloor(queuePending, totalPending, activeQueues, maxBacklog, 0, draw)
}

// DecideFairWithFloor is DecideFair where the ceiling for under-share queues
// is FairCeiling times the larger of maxBacklog and ceilingFloor. A target
// API's X-Aqueduct-Max-Backlog can shrink the budget below what the node
// already holds; without a floor, the backlog then sits above the shrunken
// ceiling and every queue is refused, the small ones too. The caller passes
// the backlog held when the budget shrank, until it drains: queues over their
// share of the new budget stay refused, and the backlog grows at most 25%
// past what was already held.
func DecideFairWithFloor(queuePending, totalPending int64, activeQueues int, maxBacklog, ceilingFloor int64, draw float64) FairDecision {
	if activeQueues < 1 {
		activeQueues = 1
	}

	projectedQueue := queuePending + 1
	projectedTotal := totalPending + 1
	snapshot := QueueSnapshot{
		ActiveQueues:    activeQueues,
		UpstreamBacklog: projectedTotal,
		QueueBacklog:    projectedQueue,
		MaxBacklog:      maxBacklog,
	}

	if maxBacklog <= 0 {
		return FairDecision{Allowed: true, Snapshot: snapshot}
	}
	fairShare := float64(maxBacklog) / float64(activeQueues)
	if projectedTotal > maxBacklog {
		// Past the budget only queues above their fair share are refused, so
		// one large queue filling the budget doesn't lock out small ones.
		underShare := activeQueues > 1 && float64(projectedQueue) <= fairShare
		snapshot.AdmissionPressure = 1
		if underShare && float64(projectedTotal) <= FairCeiling*float64(max(maxBacklog, ceilingFloor)) {
			return FairDecision{Allowed: true, Snapshot: snapshot}
		}
		snapshot.RejectionProbability = 1
		return FairDecision{Allowed: false, Reason: "upstream_queue", Snapshot: snapshot}
	}
	if activeQueues == 1 {
		return FairDecision{Allowed: true, Snapshot: snapshot}
	}

	start := FairStart * float64(maxBacklog)
	pressure := Clamp((float64(projectedTotal)-start)/(float64(maxBacklog)-start), 0, 1)
	// A queue at twice its fair share or more is fully in excess, so at full
	// pressure it is refused every time, before the budget fills.
	excess := Clamp((float64(projectedQueue)-fairShare)/fairShare, 0, 1)
	probability := pressure * excess

	snapshot.AdmissionPressure = pressure
	snapshot.RejectionProbability = probability
	if probability > 0 && Clamp(draw, 0, math.Nextafter(1, 0)) < probability {
		return FairDecision{Allowed: false, Reason: "queue_fair_share", Snapshot: snapshot}
	}
	return FairDecision{Allowed: true, Snapshot: snapshot}
}

func RandomDraw() float64 {
	return rand.Float64()
}

func Clamp(value, low, high float64) float64 {
	return math.Max(low, math.Min(value, high))
}
