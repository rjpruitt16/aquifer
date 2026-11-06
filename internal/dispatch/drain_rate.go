// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"sync"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// DefaultStuckBacklogSeconds: under memory pressure, a target whose backlog
// would take longer than this to drain at its recent pace gets no new jobs
// (AQUIFER_MEMORY_STUCK_BACKLOG_SECONDS; 0 refuses every target, as before).
const DefaultStuckBacklogSeconds = 60

// StuckBacklogLimit is AQUIFER_MEMORY_STUCK_BACKLOG_SECONDS as a duration.
func StuckBacklogLimit() time.Duration {
	return time.Duration(env.Int64("AQUIFER_MEMORY_STUCK_BACKLOG_SECONDS", DefaultStuckBacklogSeconds)) * time.Second
}

// drainTracker measures how fast a target's queues finish jobs, sampled on
// the worker's 3 s budget tick.
type drainTracker struct {
	mu         sync.Mutex
	lastAt     time.Time
	lastServed int64
	rate       float64 // jobs/s, smoothed
	sampled    bool
}

func (w *URLWorker) servedTotal() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var total int64
	for _, q := range w.queues {
		total += q.Served()
	}
	return total
}

// sampleDrain records how many jobs left this target's queues since the
// last sample.
func (w *URLWorker) sampleDrain(now time.Time) {
	served := w.servedTotal()
	d := &w.drain
	d.mu.Lock()
	defer d.mu.Unlock()
	// A retired queue takes its count with it; start over from here.
	if d.lastAt.IsZero() || served < d.lastServed {
		d.lastAt, d.lastServed = now, served
		return
	}
	dt := now.Sub(d.lastAt).Seconds()
	if dt <= 0 {
		return
	}
	instant := float64(served-d.lastServed) / dt
	if d.sampled {
		d.rate = 0.7*d.rate + 0.3*instant
	} else {
		d.rate, d.sampled = instant, true
	}
	d.lastAt, d.lastServed = now, served
}

// BacklogStuck reports whether this target's backlog would take longer than
// limit to drain at its recent pace. A target with no backlog, or not yet
// measured, isn't stuck.
func (w *URLWorker) BacklogStuck(limit time.Duration) bool {
	backlog := w.backlog.Load()
	if backlog <= 0 || limit <= 0 {
		return false
	}
	w.drain.mu.Lock()
	rate, sampled := w.drain.rate, w.drain.sampled
	w.drain.mu.Unlock()
	if !sampled {
		return false
	}
	return float64(backlog) > rate*limit.Seconds()
}

// TargetBacklogStuck reports whether job's target is stuck (see
// URLWorker.BacklogStuck), without creating a worker for a new target.
func (r *Registry) TargetBacklogStuck(job *jobs.Job, limit time.Duration) bool {
	key := DomainKey(job.URL)
	if job.PoolID != "" {
		key = "pool:" + job.PoolID
	}
	r.mu.Lock()
	w, ok := r.workers[key]
	r.mu.Unlock()
	return ok && w.BacklogStuck(limit)
}
