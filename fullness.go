// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package aquifer

import (
	"math"
	"net/http"
	"strconv"

	"github.com/rjpruitt16/aquifer/internal/admission"
)

// fullnessHeader tells whoever is in front of this instance -- a gateway
// like Canalis, or a client -- how full it is, 0-100, on every response.
// What to do about it is the caller's call: Canalis stops sending new users
// to an instance past its own threshold, and queues them itself once every
// instance is past it.
const fullnessHeader = "X-Aqueduct-Fullness"

// Fullness is how full this instance is, 0-100: the most-constrained of the
// limits it already enforces -- memory against AQUIFER_MEMORY_LIMIT_MB, the
// database against AQUIFER_DB_MAX_BYTES, and each target API's backlog
// against its backlog budget (AQUIFER_MAX_PENDING_PER_UPSTREAM, or the
// target API's own X-Aqueduct-Max-Backlog). The tightest one wins, because
// that's the one that will start turning work away first. Limits that are
// switched off don't count.
//
// Unlike X-Aqueduct-Admission-Pressure, which stays at 0 until a backlog
// reaches fair admission's 70% starting point, this rises linearly from
// the first queued job, so a caller can see an instance filling up before
// it starts pushing back.
//
// Sampled at most once per admission.SampleTTL: it goes on every response,
// and the readings behind it aren't free.
func (a *Aquifer) Fullness() int {
	if a == nil {
		return 0
	}
	return int(a.fullnessSample.Get(func() int64 {
		ratio := a.admission.FullnessRatio()
		if a.registry != nil {
			ratio = math.Max(ratio, a.registry.backlogFullnessRatio())
		}
		return fullnessPercent(ratio)
	}))
}

func fullnessPercent(ratio float64) int64 {
	return int64(math.Round(admission.Clamp(ratio, 0, 1) * 100))
}

// backlogFullnessRatio is the fullest target API's backlog against its own
// backlog budget. Per target API, not summed: one target API at its limit is
// already turning work away, however idle the others are.
func (r *Registry) backlogFullnessRatio() float64 {
	r.mu.Lock()
	workers := make([]*URLWorker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	r.mu.Unlock()

	var ratio float64
	for _, worker := range workers {
		snapshot := worker.Snapshot()
		if snapshot.MaxBacklog > 0 {
			ratio = math.Max(ratio, float64(snapshot.UpstreamBacklog)/float64(snapshot.MaxBacklog))
		}
	}
	return ratio
}

func withFullnessHeader(next http.Handler, a *Aquifer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a != nil {
			w.Header().Set(fullnessHeader, strconv.Itoa(a.Fullness()))
		}
		next.ServeHTTP(w, r)
	})
}
