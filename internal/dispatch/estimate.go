// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"math"
	"math/rand/v2"
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

// StreamMaxWait is the longest expected wait Aquifer holds a stream open
// for (AQUIFER_STREAM_MAX_WAIT_SECONDS, default 30; 0 or less: no limit).
// A caller further back is told when to come back instead, so connections
// are held only for jobs near the front. Applies with ReadyEstimates.
func StreamMaxWait() float64 {
	return float64(env.Int64("AQUIFER_STREAM_MAX_WAIT_SECONDS", 30))
}

// MaxPollAfter caps how long a caller is told to wait before polling, so a
// bad estimate can't park it for long and a long wait is re-estimated at
// least this often.
const MaxPollAfter = 60 * time.Second

// minPollAfter keeps a caller told "any moment now" from spinning.
const minPollAfter = 100 * time.Millisecond

// PollAfter is when a caller should come back for a result expected in
// seconds: a little early, at 80-100% of the estimate, because arriving
// early costs one more cheap poll and arriving late costs the caller time;
// the spread keeps callers told the same time from all returning at once.
func PollAfter(seconds float64) time.Duration {
	d := time.Duration(seconds * (0.8 + 0.2*rand.Float64()) * float64(time.Second))
	return min(max(d, minPollAfter), MaxPollAfter)
}

// Estimate is where a job stands in its account queue.
type Estimate struct {
	// Position is the job's place in line, or 0 when it isn't waiting in
	// line: in flight, or waiting out a retry backoff.
	Position int64
	// Seconds is about how long until the job's result is ready: its wait
	// for a turn plus the queue's average run time. -1 when the queue
	// hasn't dispatched recently enough to tell.
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
	// first is the second the current run of events began, so a queue that
	// just started (or restarted after going quiet) isn't averaged over
	// seconds it wasn't running.
	first int64
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
	if s-m.sec > rateWindow {
		m.first = s
	}
	m.advance(s)
	m.buckets[s%(rateWindow+1)]++
}

// Rate is the events per second over the last rateWindow whole seconds,
// leaving out the current, partial one, or over the whole seconds since
// events began if fewer.
func (m *rateMeter) Rate(now time.Time) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := now.Unix()
	m.advance(s)
	span := min(int64(rateWindow), s-m.first)
	if span <= 0 {
		return 0
	}
	var total int64
	for i := s - span; i < s; i++ {
		total += m.buckets[i%(rateWindow+1)]
	}
	return float64(total) / float64(span)
}

// runTimer keeps a moving average of how long a queue's requests take.
type runTimer struct {
	mu  sync.Mutex
	avg float64 // seconds; 0 until the first request finishes
}

func (r *runTimer) Observe(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.avg == 0 {
		r.avg = d.Seconds()
		return
	}
	r.avg += 0.1 * (d.Seconds() - r.avg)
}

func (r *runTimer) Seconds() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.avg
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

// observeRun records how long one of this queue's requests took.
func (q *AccountQueue) observeRun(d time.Duration) {
	if q.estimates {
		q.runs.Observe(d)
	}
}

// readyIn is about how long until the result of the job at position is
// ready: its wait for a turn at the queue's recent dispatch rate, plus the
// average run time. -1 when there is no rate yet.
func (q *AccountQueue) readyIn(position int64) float64 {
	rate := q.starts.Rate(time.Now())
	if rate <= 0 {
		return -1
	}
	return float64(position)/rate + q.runs.Seconds()
}

// Estimate is where jobID stands in this queue. A job not waiting in line
// (in flight, or waiting out a retry) has position 0 and, once the queue
// has finished a request, the average run time as its estimate. ok is
// false when estimates are off.
func (q *AccountQueue) Estimate(jobID string) (Estimate, bool) {
	if !q.estimates {
		return Estimate{}, false
	}
	v, ok := q.tickets.Load(jobID)
	if !ok {
		run := q.runs.Seconds()
		if run <= 0 {
			return Estimate{Seconds: -1}, true
		}
		return Estimate{Seconds: run}, true
	}
	position := max(v.(int64)-q.started.Load(), 1)
	return Estimate{Position: position, Seconds: q.readyIn(position)}, true
}

// EstimateNew is Estimate for a job just handed to this queue, which may not
// have joined the line yet: until it has, it counts as last in line.
func (q *AccountQueue) EstimateNew(jobID string) (Estimate, bool) {
	if !q.estimates {
		return Estimate{}, false
	}
	if _, ok := q.tickets.Load(jobID); ok {
		return q.Estimate(jobID)
	}
	position := max(q.issued.Load()-q.started.Load()+1, 1)
	return Estimate{Position: position, Seconds: q.readyIn(position)}, true
}

// pollEvent is the event that ends a stream whose job is expected to take
// longer than StreamMaxWait, telling the caller when to come back; nil
// when the stream should stay open.
func (q *AccountQueue) pollEvent(jobID string, position int64) *SSEEvent {
	if !q.estimates || q.streamMaxWait <= 0 {
		return nil
	}
	ready := q.readyIn(position)
	if ready <= q.streamMaxWait {
		return nil
	}
	return &SSEEvent{Event: "poll", Data: map[string]any{
		"job_id":                  jobID,
		"position":                position,
		"estimated_ready_seconds": math.Round(ready*10) / 10,
		"retry_after_ms":          PollAfter(ready).Milliseconds(),
	}}
}

// queueFor finds job's account queue on this worker, or nil.
func (w *URLWorker) queueFor(job *jobs.Job) *AccountQueue {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := SharedQueueKey
	if w.accountQueueMode {
		key = JobQueueKey(job)
	}
	return w.queues[key]
}

// queueFor finds job's account queue without creating a worker for it.
func (r *Registry) queueFor(job *jobs.Job) *AccountQueue {
	r.mu.Lock()
	w := r.workers[workerKey(job)]
	r.mu.Unlock()
	if w == nil {
		return nil
	}
	return w.queueFor(job)
}

// Estimate is where job stands in its account queue.
func (r *Registry) Estimate(job *jobs.Job) (Estimate, bool) {
	q := r.queueFor(job)
	if q == nil {
		return Estimate{}, false
	}
	return q.Estimate(job.ID)
}

// EstimateNew is Estimate for a job just enqueued (AccountQueue.EstimateNew).
func (r *Registry) EstimateNew(job *jobs.Job) (Estimate, bool) {
	q := r.queueFor(job)
	if q == nil {
		return Estimate{}, false
	}
	return q.EstimateNew(job.ID)
}
