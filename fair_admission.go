package aquifer

import (
	"math"
	"math/rand"
	"os"
	"strconv"
)

const (
	defaultMaxPendingPerUpstream int64 = 10_000
	fairAdmissionStart                 = 0.70
)

// QueueSnapshot is the per-upstream state used for fair admission and
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

type FairAdmissionDecision struct {
	Allowed  bool
	Reason   string
	Snapshot QueueSnapshot
}

func configuredMaxPendingPerUpstream() int64 {
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

func decideFairAdmission(queuePending, totalPending int64, activeQueues int, maxBacklog int64, draw float64) FairAdmissionDecision {
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
		return FairAdmissionDecision{Allowed: true, Snapshot: snapshot}
	}
	if projectedTotal > maxBacklog {
		snapshot.AdmissionPressure = 1
		snapshot.RejectionProbability = 1
		return FairAdmissionDecision{Allowed: false, Reason: "upstream_queue", Snapshot: snapshot}
	}
	if activeQueues == 1 {
		return FairAdmissionDecision{Allowed: true, Snapshot: snapshot}
	}

	start := fairAdmissionStart * float64(maxBacklog)
	pressure := clampFloat((float64(projectedTotal)-start)/(float64(maxBacklog)-start), 0, 1)
	fairShare := float64(maxBacklog) / float64(activeQueues)
	excess := clampFloat((float64(projectedQueue)-fairShare)/(float64(maxBacklog)-fairShare), 0, 1)
	probability := pressure * excess

	snapshot.AdmissionPressure = pressure
	snapshot.RejectionProbability = probability
	if probability > 0 && clampFloat(draw, 0, math.Nextafter(1, 0)) < probability {
		return FairAdmissionDecision{Allowed: false, Reason: "queue_fair_share", Snapshot: snapshot}
	}
	return FairAdmissionDecision{Allowed: true, Snapshot: snapshot}
}

func randomAdmissionDraw() float64 {
	return rand.Float64()
}

func clampFloat(value, low, high float64) float64 {
	return math.Max(low, math.Min(value, high))
}
