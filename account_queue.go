package aquifer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minRPS                     = 0.5
	maxRetries                 = 4
	noPoolMembersRetryInterval = time.Second
)

var retrySleepFunc atomic.Value
var poolEmptySleepFunc atomic.Value

func init() {
	retrySleepFunc.Store(time.Sleep)
	poolEmptySleepFunc.Store(time.Sleep)
}

func sleepBeforeRetry(d time.Duration) {
	retrySleepFunc.Load().(func(time.Duration))(d)
}

func sleepBeforeRetryContext(ctx context.Context, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		sleepBeforeRetry(d)
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleepWhilePoolEmpty(d time.Duration) {
	poolEmptySleepFunc.Load().(func(time.Duration))(d)
}

type jobDoneMsg struct {
	userID        string
	completed     bool
	rps           *float64
	maxConcurrent *int
	accountQueue  *string
	slowStart     *bool
	maxBacklog    *int64
}

// webhookEnqueuer queues a webhook delivery through the same account-queue
// pacing machinery as forward dispatch — see Registry.EnqueueWebhook.
// Threaded down from Registry through URLWorker and AccountQueue as a
// closure (matching the existing onIdle pattern) rather than a *Registry
// back-reference, so AccountQueue/execute only get the one capability they
// actually need.
type webhookEnqueuer func(originalJobID, userID, webhookURL string, payload map[string]any)

type AccountQueue struct {
	key                string
	upstream           string
	pool               *Pool // nil unless this queue dispatches into a registered pool instead of a fixed URL
	cmds               chan *Job
	done               chan jobDoneMsg
	store              JobStore
	broker             *Broker
	l8                 *L8Registry
	metrics            MetricsAdapter
	enqueueWebhook     webhookEnqueuer
	resultRecorder     JobResultRecorder
	workerActiveQueues *atomic.Int64
	workerBacklog      *atomic.Int64
	onJobDone          func(string)
	currentRPS         atomic.Int64 // stored as rps * 100
	backlog            atomic.Int64 // accepted queued + in-flight work
	// nextDeadline is the earliest execute_before (Unix ms) among queued
	// jobs, or 0 when none has one. Owned by the run goroutine. It lets
	// dropExpiredJobs skip its scan of the whole queue, which otherwise ran
	// on every loop pass and grew quadratically once a backlog built up.
	nextDeadline int64
	stop         chan struct{}
	stopped      chan struct{}
	stopOnce     sync.Once
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func (q *AccountQueue) RPS() float64 {
	return float64(q.currentRPS.Load()) / 100
}

// Active reports whether this queue currently has real backlog (queued or
// in-flight work) — used by proxy mode to decide whether a domain should
// keep routing through the durable queue even after its circuit breaker's
// cooldown has elapsed, since a cooldown timer alone doesn't know whether
// the backlog it caused has actually finished draining yet.
func (q *AccountQueue) Active() bool {
	return q.backlog.Load() > 0
}

func (q *AccountQueue) Backlog() int64 {
	return q.backlog.Load()
}

func (q *AccountQueue) reserveBacklog() {
	if q.backlog.Add(1) == 1 && q.workerActiveQueues != nil {
		q.workerActiveQueues.Add(1)
	}
	if q.workerBacklog != nil {
		q.workerBacklog.Add(1)
	}
}

func (q *AccountQueue) releaseBacklog() {
	if q.backlog.Add(-1) == 0 && q.workerActiveQueues != nil {
		q.workerActiveQueues.Add(-1)
	}
	if q.workerBacklog != nil {
		q.workerBacklog.Add(-1)
	}
}

func (q *AccountQueue) clearBacklog() {
	remaining := q.backlog.Swap(0)
	if remaining <= 0 {
		return
	}
	if q.workerActiveQueues != nil {
		q.workerActiveQueues.Add(-1)
	}
	if q.workerBacklog != nil {
		q.workerBacklog.Add(-remaining)
	}
}

func qLoad(activeQueues, backlog *atomic.Int64) (int, int64) {
	var active int
	var pending int64
	if activeQueues != nil {
		active = int(activeQueues.Load())
	}
	if backlog != nil {
		pending = backlog.Load()
	}
	return active, pending
}

// Throttle pushes an external rate adjustment into the queue's dispatch
// loop, reusing the same jobDoneMsg channel that header-driven pacing
// already uses — the loop doesn't need to know whether a lower rate came
// from the upstream's own response header or from the URLWorker capping
// this queue's share of a shared aggregate budget, it's the same signal
// either way.
func (q *AccountQueue) Throttle(rps float64) {
	select {
	case q.done <- jobDoneMsg{rps: &rps}:
	default:
		// Queue's done channel is momentarily full (100-deep buffer) —
		// skip this tick, the next aggregate-budget check will retry.
	}
}

func NewAccountQueue(key, upstream string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook webhookEnqueuer, resultRecorder JobResultRecorder, workerActiveQueues, workerBacklog *atomic.Int64, onJobDone func(string), onIdle func(string), slowStart bool, onSlowStartSignal func(bool), onMaxBacklogSignal func(int64)) *AccountQueue {
	q := &AccountQueue{
		key:                key,
		upstream:           upstream,
		pool:               pool,
		cmds:               make(chan *Job, 1000),
		done:               make(chan jobDoneMsg, 100),
		store:              store,
		broker:             broker,
		l8:                 l8,
		metrics:            ensureMetrics(metrics),
		enqueueWebhook:     enqueueWebhook,
		resultRecorder:     resultRecorder,
		workerActiveQueues: workerActiveQueues,
		workerBacklog:      workerBacklog,
		onJobDone:          onJobDone,
		stop:               make(chan struct{}),
		stopped:            make(chan struct{}),
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())
	go q.supervise(rps, maxConc, onIdle, slowStart, onSlowStartSignal, onMaxBacklogSignal)
	return q
}

func (q *AccountQueue) Enqueue(job *Job) bool {
	q.reserveBacklog()
	return q.handoff(job)
}

// handoff gives an already-reserved job to the run loop. URLWorker calls it
// after releasing its lock, so the store write and the channel send don't
// serialize every submission and webhook on one lock.
func (q *AccountQueue) handoff(job *Job) bool {
	q.store.SetQueueKey(job.ID, q.key)
	select {
	case q.cmds <- job:
		return true
	case <-q.stop:
		q.releaseBacklog()
		return false
	}
}

func (q *AccountQueue) Stop() {
	q.stopOnce.Do(func() {
		q.cancel()
		close(q.stop)
		<-q.stopped
		q.wg.Wait()
	})
}

func (q *AccountQueue) supervise(rps float64, maxConc int, onIdle func(string), slowStart bool, onSlowStartSignal func(bool), onMaxBacklogSignal func(int64)) {
	defer close(q.stopped)

	for {
		panicked := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[AccountQueue] panic in %s: %v — restarting", q.key, r)
					panicked = true
				}
			}()
			q.run(rps, maxConc, slowStart, onSlowStartSignal, onMaxBacklogSignal)
		}()

		select {
		case <-q.stop:
			return
		default:
		}

		if panicked {
			// The panicked loop's in-memory queue is gone; re-enqueue every
			// unfinished job of this queue from storage (some may still be
			// dispatching, which at-least-once delivery already allows).
			recovered := q.store.RecoverQueued(q.key)
			for _, j := range recovered {
				log.Printf("[AccountQueue] re-enqueued job %s after panic", j.ID)
				select {
				case q.cmds <- j:
				case <-q.stop:
					return
				}
			}
			continue
		}

		if len(q.cmds) == 0 {
			if onIdle != nil {
				onIdle(q.key)
			}
			// onIdle refuses to retire a queue that has reserved backlog
			// (an enqueuer reserved under the worker lock and is about to
			// hand the job over) or buffered jobs; keep running for them.
			if q.backlog.Load() > 0 || len(q.cmds) > 0 {
				continue
			}
			return
		}
	}
}

const defaultAccountQueueIdleTimeoutSeconds = 300

// accountQueueIdleTimeout is how long a queue can sit genuinely idle
// before self-terminating -- 5 minutes by default, overridable via
// AQUIFER_IDLE_TIMEOUT_SECONDS. Exists mainly so contract tests
// (aqueduct-runner) don't have to burn 5+ real minutes per drain-mode
// run; production should leave this at the default.
func accountQueueIdleTimeout() time.Duration {
	return time.Duration(envInt64("AQUIFER_IDLE_TIMEOUT_SECONDS", defaultAccountQueueIdleTimeoutSeconds)) * time.Second
}

func (q *AccountQueue) run(configuredRPS float64, configuredMaxConc int, slowStart bool, onSlowStartSignal func(bool), onMaxBacklogSignal func(int64)) {
	idleTimeout := accountQueueIdleTimeout()
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()

	positionTicker := time.NewTicker(2 * time.Second)
	defer positionTicker.Stop()

	rps := configuredRPS
	if slowStart {
		// Start below the configured ceiling instead of firing at it
		// immediately -- the existing creep-back-up-toward-configuredRPS
		// behavior below (msg.rps == nil branch) is the ramp; slow start
		// only changes the starting point, not the mechanism.
		rps = minRPS
	}
	maxConc := configuredMaxConc
	lastRequestAt := time.Time{}
	inFlight := 0
	queue := make([]*Job, 0, 64)
	q.currentRPS.Store(int64(rps * 100))

	for {
		queue = q.dropExpiredJobs(queue)

		// Pool-backed queues don't have a fixed configured ceiling — the
		// pool's aggregate capacity is the live sum of whatever its
		// current members are individually reporting, so it's resampled
		// here rather than captured once at queue startup. TotalCapacity
		// is O(n) over the pool's own member count (typically small), so
		// this is cheap to call on every pass through the dispatch loop.
		if q.pool != nil {
			if cap := q.pool.TotalCapacity(); cap > 0 {
				rps = math.Max(math.Min(cap, configuredRPS), minRPS)
			}
		}

		for len(queue) > 0 && inFlight < maxConc {
			if queue[0].ExecutionExpired(time.Now()) {
				queue = q.dropExpiredJobs(queue)
				continue
			}

			var member *PoolMember
			if q.pool != nil {
				member = q.pool.Pick()
				if member == nil {
					// A pool may be temporarily empty during process
					// restart before members have had a chance to
					// heartbeat back in. Keep queued work durable here
					// instead of converting recoverable absence into a
					// permanent job failure.
					sleepWhilePoolEmpty(noPoolMembersRetryInterval)
					continue
				}
			}

			interval := time.Duration(float64(time.Second) / rps)
			elapsed := time.Since(lastRequestAt)

			if elapsed < interval {
				time.Sleep(withJitter(interval - elapsed))
			}
			if queue[0].ExecutionExpired(time.Now()) {
				queue = q.dropExpiredJobs(queue)
				continue
			}

			job := queue[0]
			queue = queue[1:]
			q.metrics.QueueDepth(q.upstream, len(queue))
			inFlight++
			lastRequestAt = time.Now()

			q.metrics.JobDispatched(job.UserID, q.upstream)

			dispatchURL := job.URL
			if member != nil {
				dispatchURL = member.Address
			}

			currentRPS := rps
			activeQueues, upstreamBacklog := qLoad(q.workerActiveQueues, q.workerBacklog)
			q.wg.Add(1)
			go func(j *Job, url string, m *PoolMember, flowRate float64, active int, backlog int64) {
				defer q.wg.Done()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[AccountQueue] panic executing job %s: %v", j.ID, r)
						q.store.UpdateStatus(j.ID, StatusFailed)
						q.metrics.JobFailed(j.UserID, q.upstream, "internal panic")
						if !j.isWebhookDeliveryJob() {
							q.enqueueWebhook(j.ID, j.UserID, j.WebhookURL, map[string]any{
								"job_id": j.ID,
								"status": "failed",
								"reason": "internal panic",
							})
						}
						select {
						case q.done <- jobDoneMsg{userID: j.UserID, completed: true}:
						case <-q.stop:
						}
					}
				}()
				msg := execute(q.ctx, j, url, q.upstream, q.store, q.broker, q.l8, q.metrics, q.pool, m, flowRate, active, backlog, q.enqueueWebhook, q.resultRecorder)
				msg.userID = j.UserID
				msg.completed = true
				select {
				case q.done <- msg:
				case <-q.stop:
				}
			}(job, dispatchURL, member, currentRPS, activeQueues, upstreamBacklog)
		}

		select {
		case job := <-q.cmds:
			queue = append(queue, job)
			if job.ExecuteBefore > 0 && (q.nextDeadline == 0 || job.ExecuteBefore < q.nextDeadline) {
				q.nextDeadline = job.ExecuteBefore
			}
			q.metrics.QueueDepth(q.upstream, len(queue))
			idle.Reset(idleTimeout)

		case msg := <-q.done:
			if msg.completed {
				inFlight--
				q.releaseBacklog()
				if q.onJobDone != nil && msg.userID != "" {
					q.onJobDone(msg.userID)
				}
			}
			prevRPS := rps
			if msg.rps != nil {
				rps = math.Max(math.Min(*msg.rps, configuredRPS), minRPS)
			} else if rps < configuredRPS {
				rps = math.Min(rps*1.05, configuredRPS)
			}
			if msg.maxConcurrent != nil && *msg.maxConcurrent > 0 {
				maxConc = int(math.Min(float64(*msg.maxConcurrent), float64(configuredMaxConc)))
			}
			q.currentRPS.Store(int64(rps * 100))
			if rps != prevRPS {
				q.metrics.FlowRate(q.upstream, rps)
			}
			if msg.slowStart != nil && onSlowStartSignal != nil {
				onSlowStartSignal(*msg.slowStart)
			}
			if msg.maxBacklog != nil && onMaxBacklogSignal != nil {
				onMaxBacklogSignal(*msg.maxBacklog)
			}
			idle.Reset(idleTimeout)

		case <-positionTicker.C:
			queue = q.dropExpiredJobs(queue)
			for i, j := range queue {
				if !q.broker.HasSubscribers(j.ID) {
					continue
				}
				q.broker.Publish(j.ID, SSEEvent{
					Event: "position",
					Data:  map[string]any{"job_id": j.ID, "position": i + 1},
				})
			}

		case <-idle.C:
			if len(queue) == 0 && inFlight == 0 {
				return
			}
			idle.Reset(idleTimeout)

		case <-q.stop:
			q.clearBacklog()
			return
		}
	}
}

func (q *AccountQueue) dropExpiredJobs(queue []*Job) []*Job {
	if len(queue) == 0 {
		q.nextDeadline = 0
		return queue
	}
	now := time.Now()
	if q.nextDeadline == 0 || now.UnixMilli() < q.nextDeadline {
		return queue
	}
	q.nextDeadline = 0
	kept := queue[:0]
	for _, job := range queue {
		if !job.ExecutionExpired(now) {
			kept = append(kept, job)
			if job.ExecuteBefore > 0 && (q.nextDeadline == 0 || job.ExecuteBefore < q.nextDeadline) {
				q.nextDeadline = job.ExecuteBefore
			}
			continue
		}
		q.releaseBacklog()
		failExpiredJob(job, q.upstream, q.store, q.broker, q.metrics, q.enqueueWebhook, q.resultRecorder)
		if q.onJobDone != nil {
			q.onJobDone(job.UserID)
		}
	}
	if len(kept) != len(queue) {
		q.metrics.QueueDepth(q.upstream, len(kept))
	}
	return kept
}

func execute(ctx context.Context, job *Job, dispatchURL, upstream string, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pool *Pool, member *PoolMember, flowRate float64, activeQueues int, upstreamBacklog int64, enqueueWebhook webhookEnqueuer, resultRecorder JobResultRecorder) jobDoneMsg {
	metrics = ensureMetrics(metrics)
	startedAt := time.Now()

	broker.Publish(job.ID, SSEEvent{
		Event: "dispatching",
		Data:  map[string]any{"job_id": job.ID},
	})

	var resp *http.Response
	var err error
	currentURL := dispatchURL
	currentMember := member
	reason := "connection error"

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if job.ExecutionExpired(time.Now()) {
			reason = "execution_deadline_exceeded"
			err = fmt.Errorf("%s", reason)
			break
		}
		if attempt > 0 {
			backoff := withJitter(time.Duration(math.Pow(2, float64(attempt-1))) * time.Second)
			log.Printf("[AccountQueue] retry %d/%d for %s in %s", attempt, maxRetries, currentURL, backoff)
			if !sleepBeforeRetryContext(ctx, backoff) {
				store.UpdateStatus(job.ID, StatusQueued)
				return jobDoneMsg{}
			}
			if job.ExecutionExpired(time.Now()) {
				reason = "execution_deadline_exceeded"
				err = fmt.Errorf("%s", reason)
				break
			}
		}

		counts := store.Counts()
		resp, err = makeRequest(ctx, job, currentURL, counts.TotalJobs, counts.QueueDepth, flowRate, activeQueues, upstreamBacklog, l8)
		if err != nil {
			if ctx.Err() != nil {
				store.UpdateStatus(job.ID, StatusQueued)
				return jobDoneMsg{}
			}
			reason = err.Error()
			if pool != nil && currentMember != nil {
				pool.RecordFailure(currentMember.ID)
				if attempt < maxRetries {
					currentMember = pool.Pick()
					if currentMember == nil {
						reason = "no pool members registered"
						break
					}
					currentURL = currentMember.Address
				}
			}
			continue
		}
		if resp.StatusCode >= 500 && attempt < maxRetries {
			reason = fmt.Sprintf("upstream returned %d", resp.StatusCode)
			resp.Body.Close()
			resp = nil
			if pool != nil && currentMember != nil {
				pool.RecordFailure(currentMember.ID)
				currentMember = pool.Pick()
				if currentMember == nil {
					reason = "no pool members registered"
					break
				}
				currentURL = currentMember.Address
			}
			continue
		}
		break
	}

	if err != nil || resp == nil || resp.StatusCode >= 500 {
		var body []byte
		var responseStatus int
		if resp != nil {
			responseStatus = resp.StatusCode
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if reason == "connection error" {
				reason = fmt.Sprintf("upstream returned %d", resp.StatusCode)
			}
		}
		if pool != nil && currentMember != nil && resp != nil {
			pool.RecordFailure(currentMember.ID)
		}
		store.UpdateStatus(job.ID, StatusFailed)
		resultKey := recordJobResult(job, resultRecorder, StatusFailed, responseStatus, "", string(body))
		broker.Publish(job.ID, SSEEvent{
			Event: "failed",
			Data:  map[string]any{"job_id": job.ID, "reason": reason, "response_status": responseStatus, "body": string(body)},
		})
		metrics.JobFailed(job.UserID, upstream, reason)
		if !job.isWebhookDeliveryJob() {
			payload := map[string]any{
				"job_id":          job.ID,
				"status":          "failed",
				"reason":          reason,
				"response_status": responseStatus,
				"body":            string(body),
			}
			if resultKey != "" {
				payload["result_key"] = resultKey
			}
			enqueueWebhook(job.ID, job.UserID, job.WebhookURL, payload)
		}
		return jobDoneMsg{}
	}
	defer resp.Body.Close()

	if pool != nil && currentMember != nil {
		pool.RecordSuccess(currentMember.ID)
	}

	body, _ := io.ReadAll(resp.Body)
	store.UpdateStatus(job.ID, StatusCompleted)
	resultKey := recordJobResult(job, resultRecorder, StatusCompleted, resp.StatusCode, resp.Header.Get("Content-Type"), string(body))
	broker.Publish(job.ID, SSEEvent{
		Event: "completed",
		Data: map[string]any{
			"job_id":          job.ID,
			"response_status": resp.StatusCode,
			"body":            string(body),
		},
	})
	metrics.JobCompleted(job.UserID, upstream, time.Since(startedAt).Milliseconds())
	if !job.isWebhookDeliveryJob() {
		payload := map[string]any{
			"job_id":          job.ID,
			"status":          "completed",
			"response_status": resp.StatusCode,
			"body":            string(body),
		}
		if resultKey != "" {
			payload["result_key"] = resultKey
		}
		enqueueWebhook(job.ID, job.UserID, job.WebhookURL, payload)
	}

	msg := jobDoneMsg{}
	if val := pacingHeader(resp.Header, "Rps"); val != "" {
		var rps float64
		fmt.Sscanf(val, "%f", &rps)
		msg.rps = &rps
	} else if rps := orcaRps(resp.Header); rps != nil {
		// No explicit Aqueduct directive on this response -- fall back to an
		// ORCA endpoint-load-metrics header if the backend (e.g. vLLM with
		// --orca_formats) sent one. An explicit X-Aqueduct-Rps always wins;
		// this only fires when the backend hasn't opted into speaking
		// Aqueduct's own pacing headers directly.
		msg.rps = rps
	}
	if val := pacingHeader(resp.Header, "Max-Concurrent"); val != "" {
		var max int
		fmt.Sscanf(val, "%d", &max)
		msg.maxConcurrent = &max
	}
	if val := pacingHeader(resp.Header, "Account-Queue"); val != "" {
		msg.accountQueue = &val
	}
	if val := pacingHeader(resp.Header, "Slow-Start"); val == "true" || val == "false" {
		enabled := val == "true"
		msg.slowStart = &enabled
	}
	if val := pacingHeader(resp.Header, "Max-Backlog"); val != "" {
		var max int64
		if _, err := fmt.Sscanf(val, "%d", &max); err == nil && max >= 0 {
			msg.maxBacklog = &max
		}
	}
	return msg
}

func failExpiredJob(job *Job, upstream string, store JobStore, broker *Broker, metrics MetricsAdapter, enqueueWebhook webhookEnqueuer, resultRecorder JobResultRecorder) {
	const reason = "execution_deadline_exceeded"
	store.UpdateStatus(job.ID, StatusFailed)
	resultKey := recordJobResult(job, resultRecorder, StatusFailed, 0, "", "")
	broker.Publish(job.ID, SSEEvent{Event: "failed", Data: map[string]any{
		"job_id": job.ID, "reason": reason,
	}})
	metrics.JobFailed(job.UserID, upstream, reason)
	if !job.isWebhookDeliveryJob() {
		payload := map[string]any{"job_id": job.ID, "status": "failed", "reason": reason}
		if resultKey != "" {
			payload["result_key"] = resultKey
		}
		go enqueueWebhook(job.ID, job.UserID, job.WebhookURL, payload)
	}
}

func recordJobResult(job *Job, recorder JobResultRecorder, status Status, responseStatus int, contentType string, body string) string {
	if recorder == nil || job.isWebhookDeliveryJob() {
		return ""
	}
	key, ok := recorder.RecordResult(job.dedupHash(), JobResult{
		JobID:          job.ID,
		Status:         status,
		ResponseStatus: responseStatus,
		ContentType:    contentType,
		Body:           body,
		RecordedAt:     time.Now().UnixMilli(),
		Source:         "aquifer",
	})
	if !ok {
		return ""
	}
	return key
}

func pacingHeader(headers http.Header, name string) string {
	if val := headers.Get("X-Aqueduct-" + name); val != "" {
		return val
	}
	return headers.Get("X-Aquifer-" + name)
}

func makeRequest(ctx context.Context, job *Job, dispatchURL string, totalJobs, queueDepth int64, flowRate float64, activeQueues int, upstreamBacklog int64, l8 *L8Registry) (*http.Response, error) {
	// L8 signing and encryption prove Aquifer's identity to (and keep the
	// payload private for) the *receiver* of a webhook — they have no meaning
	// for forward dispatch to an arbitrary upstream API, so this only applies
	// when the job being dispatched is itself a webhook delivery (see
	// Job.isWebhookDeliveryJob).
	body := []byte(job.Body)
	var l8Headers map[string]string
	if job.isWebhookDeliveryJob() && l8 != nil {
		l8.EnsureTrust(dispatchURL)
		contentType := job.Headers["Content-Type"]
		if contentType == "" {
			contentType = "application/json"
		}
		sealed, headers, err := l8.SealDelivery(dispatchURL, body, contentType)
		if err != nil {
			return nil, err
		}
		body, l8Headers = sealed, headers
	}

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, job.Method, dispatchURL, bodyReader)
	if err != nil {
		return nil, err
	}

	for k, v := range job.Headers {
		req.Header.Set(k, v)
	}
	for k, v := range l8Headers {
		req.Header.Set(k, v)
	}

	setLoadHeader(req.Header, "Total-Jobs", fmt.Sprintf("%d", totalJobs))
	setLoadHeader(req.Header, "Queue-Depth", fmt.Sprintf("%d", queueDepth))
	setLoadHeader(req.Header, "Flow-Rate", fmt.Sprintf("%.2f", flowRate))
	setLoadHeader(req.Header, "Active-Queues", fmt.Sprintf("%d", activeQueues))
	setLoadHeader(req.Header, "Upstream-Backlog", fmt.Sprintf("%d", upstreamBacklog))

	// Opt in to ORCA endpoint-load-metrics on every dispatch. In current
	// vLLM, this is entirely request-driven -- the backend only includes
	// the endpoint-load-metrics response header if the request itself asks
	// for it via this header (verified against vLLM's actual source,
	// vllm/entrypoints/openai/chat_completion/api_router.py). Harmless to
	// send unconditionally: a backend that doesn't understand it just
	// ignores an unrecognized request header, same as the outbound load
	// headers above.
	//
	// Lowercase "text", not "TEXT": vLLM compares via metrics_format.lower()
	// (orca_metrics.py) so either case works there, but Triton's ORCA
	// support (src/orca_http.cc, orca_type == "text") is case-sensitive and
	// only accepts the lowercase literal -- sending "TEXT" makes Triton log
	// an error and write no header at all. Lowercase is the one value both
	// verified backends actually accept.
	if req.Header.Get(orcaRequestHeaderName) == "" {
		req.Header.Set(orcaRequestHeaderName, "text")
	}

	resp, err := dispatchClient.Do(req)
	if err == nil {
		l8.ObserveSchemaHash(dispatchURL, pacingHeader(resp.Header, "Schema-Hash"))
	}
	return resp, err
}

// dispatchClient is shared by every upstream and webhook dispatch so
// connections are reused. A fresh client per request fell back to Go's
// default transport, which keeps only 2 idle connections per host: at a few
// hundred requests a second to one upstream, nearly every request opened
// (and then closed) a new TCP connection, costing Aquifer CPU and the
// upstream a connection per call.
var dispatchClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: func() *http.Transport {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 1024
		t.MaxIdleConnsPerHost = 256
		return t
	}(),
}

func setLoadHeader(headers http.Header, name, value string) {
	headers.Set("X-Aqueduct-"+name, value)
	headers.Set("X-Aquifer-"+name, value)
}
