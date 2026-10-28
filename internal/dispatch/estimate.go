// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"sync"
	"time"

	"github.com/rjpruitt16/aquifer/internal/env"
	"github.com/rjpruitt16/aquifer/internal/jobs"
)

// ReadyEstimates reports whether Aquifer tells callers when a queued job is
// likely to be dispatched (GET /jobs/{id}, poll responses, position events),
// so they can come back then instead of guessing with backoff.
func ReadyEstimates() bool {
	return env.Bool("AQUIFER_READY_ESTIMATES", false)
}

// Estimate is where a queued job stands: its place in its account queue and,
// when the queue has been dispatching, about how long until its turn.
type Estimate struct {
	Position int64
	// Seconds is the expected wait until dispatch, or -1 when the queue
	// hasn't dispatched anything recently to measure a rate from.
	Seconds float64
}

// rateWindow is how many whole seconds the drain rate averages over: short
// enough to follow capacity changing, long enough to smooth a bursty gate.
const rateWindow = 10

// rateMeter counts events per second over the last rateWindow seconds.
type rateMeter struct {
	mu      sync.Mutex
	sec     int64
	buckets [rateWindow + 1]int64
}

// advance moves the meter to second s, clearing the seconds it skipped.
// m.mu must be held.
func (m *rateMeter) advance(s int64) {
	if s <= m.sec {
		return
	}
	for i := m.sec + 1; i <= s && i <= m.sec+rateWindow+1; i++ {
		m.buckets[i%(rateWindow+1)] = 0
	}
	m.sec = s
}

func (m *rateMeter) Mark(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := now.Unix()
	m.advance(s)
	m.buckets[s%(rateWindow+1)]++
}

// Rate is the events per second over the last rateWindow whole seconds,
// leaving out the current, partial one.
func (m *rateMeter) Rate(now time.Time) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := now.Unix()
	m.advance(s)
	var total int64
	for i := s - rateWindow; i < s; i++ {
		total += m.buckets[i%(rateWindow+1)]
	}
	return float64(total) / rateWindow
}

// issueTicket records that job joined the back of the queue. Only the run
// goroutine calls it, in the order jobs are appended.
func (q *AccountQueue) issueTicket(job *jobs.Job) {
	if !q.estimates {
		return
	}
	q.tickets.Store(job.ID, q.issued.Add(1))
}

// markStarted records that job left the front of the queue, dispatched or
// expired.
func (q *AccountQueue) markStarted(job *jobs.Job) {
	if !q.estimates {
		return
	}
	q.tickets.Delete(job.ID)
	q.started.Add(1)
	q.starts.Mark(time.Now())
}

// waitFor is the expected wait for the job at position, from the queue's
// recent dispatch rate; -1 when there is no rate yet.
func (q *AccountQueue) waitFor(position int64) float64 {
	rate := q.starts.Rate(time.Now())
	if rate <= 0 {
		return -1
	}
	return float64(position) / rate
}

// Estimate is jobID's place in this queue and its expected wait. ok is
// false when the job isn't waiting in the queue: in flight, waiting out a
// retry backoff, or not here.
func (q *AccountQueue) Estimate(jobID string) (Estimate, bool) {
	if !q.estimates {
		return Estimate{}, false
	}
	v, ok := q.tickets.Load(jobID)
	if !ok {
		return Estimate{}, false
	}
	position := max(v.(int64)-q.started.Load(), 1)
	return Estimate{Position: position, Seconds: q.waitFor(position)}, true
}

// Estimate finds job's account queue on this worker and asks it.
func (w *URLWorker) Estimate(job *jobs.Job) (Estimate, bool) {
	w.mu.Lock()
	key := SharedQueueKey
	if w.accountQueueMode {
		key = JobQueueKey(job)
	}
	q := w.queues[key]
	w.mu.Unlock()
	if q == nil {
		return Estimate{}, false
	}
	return q.Estimate(job.ID)
}

// Estimate looks up job's queue without creating a worker for it.
func (r *Registry) Estimate(job *jobs.Job) (Estimate, bool) {
	r.mu.Lock()
	w := r.workers[workerKey(job)]
	r.mu.Unlock()
	if w == nil {
		return Estimate{}, false
	}
	return w.Estimate(job)
}
