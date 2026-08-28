package aquifer

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
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

func sleepWhilePoolEmpty(d time.Duration) {
	poolEmptySleepFunc.Load().(func(time.Duration))(d)
}

type jobDoneMsg struct {
	rps           *float64
	maxConcurrent *int
	accountQueue  *string
}

// webhookEnqueuer queues a webhook delivery through the same account-queue
// pacing machinery as forward dispatch — see Registry.EnqueueWebhook.
// Threaded down from Registry through URLWorker and AccountQueue as a
// closure (matching the existing onIdle pattern) rather than a *Registry
// back-reference, so AccountQueue/execute only get the one capability they
// actually need.
type webhookEnqueuer func(originalJobID, userID, webhookURL string, payload map[string]any)

type AccountQueue struct {
	key            string
	upstream       string
	pool           *Pool // nil unless this queue dispatches into a registered pool instead of a fixed URL
	cmds           chan *Job
	done           chan jobDoneMsg
	store          JobStore
	broker         *Broker
	l8             *L8Registry
	metrics        MetricsAdapter
	enqueueWebhook webhookEnqueuer
	currentRPS     atomic.Int64 // stored as rps * 100
	backlog        atomic.Int32 // len(queue) + inFlight, live — see run()
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

func NewAccountQueue(key, upstream string, rps float64, maxConc int, pool *Pool, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, enqueueWebhook webhookEnqueuer, onIdle func(string)) *AccountQueue {
	q := &AccountQueue{
		key:            key,
		upstream:       upstream,
		pool:           pool,
		cmds:           make(chan *Job, 1000),
		done:           make(chan jobDoneMsg, 100),
		store:          store,
		broker:         broker,
		l8:             l8,
		metrics:        ensureMetrics(metrics),
		enqueueWebhook: enqueueWebhook,
	}
	go q.supervise(rps, maxConc, onIdle)
	return q
}

func (q *AccountQueue) Enqueue(job *Job) {
	q.store.SetQueueKey(job.ID, q.key)
	q.cmds <- job
}

func (q *AccountQueue) supervise(rps float64, maxConc int, onIdle func(string)) {
	for {
		panicked := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[AccountQueue] panic in %s: %v — restarting", q.key, r)
					panicked = true
				}
			}()
			q.run(rps, maxConc)
		}()

		if panicked {
			recovered := q.store.RecoverInFlight(q.key)
			for _, j := range recovered {
				log.Printf("[AccountQueue] recovered in_flight job %s after panic", j.ID)
				q.cmds <- j
			}
			continue
		}

		if len(q.cmds) == 0 {
			onIdle(q.key)
			return
		}
	}
}

func (q *AccountQueue) run(configuredRPS float64, configuredMaxConc int) {
	idle := time.NewTimer(5 * time.Minute)
	defer idle.Stop()

	positionTicker := time.NewTicker(2 * time.Second)
	defer positionTicker.Stop()

	rps := configuredRPS
	maxConc := configuredMaxConc
	lastRequestAt := time.Time{}
	inFlight := 0
	queue := make([]*Job, 0, 64)
	q.currentRPS.Store(int64(rps * 100))

	for {
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
			jitter := time.Duration(rand.Int63n(int64(interval/10) + 1))
			elapsed := time.Since(lastRequestAt)

			if elapsed < interval {
				time.Sleep(interval - elapsed + jitter)
			}

			job := queue[0]
			queue = queue[1:]
			q.metrics.QueueDepth(q.upstream, len(queue))
			inFlight++
			q.backlog.Store(int32(len(queue) + inFlight))
			lastRequestAt = time.Now()

			q.store.MarkInFlight(job.ID)
			q.metrics.JobDispatched(job.UserID, q.upstream)

			dispatchURL := job.URL
			if member != nil {
				dispatchURL = member.Address
			}

			currentRPS := rps
			go func(j *Job, url string, m *PoolMember, flowRate float64) {
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
						q.done <- jobDoneMsg{}
					}
				}()
				q.done <- execute(j, url, q.upstream, q.store, q.broker, q.l8, q.metrics, q.pool, m, flowRate, q.enqueueWebhook)
			}(job, dispatchURL, member, currentRPS)
		}

		select {
		case job := <-q.cmds:
			queue = append(queue, job)
			q.metrics.QueueDepth(q.upstream, len(queue))
			q.backlog.Store(int32(len(queue) + inFlight))
			idle.Reset(5 * time.Minute)

		case msg := <-q.done:
			inFlight--
			q.backlog.Store(int32(len(queue) + inFlight))
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
			idle.Reset(5 * time.Minute)

		case <-positionTicker.C:
			for i, j := range queue {
				q.broker.Publish(j.ID, SSEEvent{
					Event: "position",
					Data:  map[string]any{"job_id": j.ID, "position": i + 1},
				})
			}

		case <-idle.C:
			if len(queue) == 0 && inFlight == 0 {
				return
			}
			idle.Reset(5 * time.Minute)
		}
	}
}

func execute(job *Job, dispatchURL, upstream string, store JobStore, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, pool *Pool, member *PoolMember, flowRate float64, enqueueWebhook webhookEnqueuer) jobDoneMsg {
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
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			log.Printf("[AccountQueue] retry %d/%d for %s in %s", attempt, maxRetries, currentURL, backoff)
			sleepBeforeRetry(backoff)
		}

		counts := store.Counts()
		resp, err = makeRequest(context.Background(), job, currentURL, counts.TotalJobs, counts.QueueDepth, flowRate, l8)
		if err != nil {
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
		broker.Publish(job.ID, SSEEvent{
			Event: "failed",
			Data:  map[string]any{"job_id": job.ID, "reason": reason, "response_status": responseStatus, "body": string(body)},
		})
		metrics.JobFailed(job.UserID, upstream, reason)
		if !job.isWebhookDeliveryJob() {
			enqueueWebhook(job.ID, job.UserID, job.WebhookURL, map[string]any{
				"job_id":          job.ID,
				"status":          "failed",
				"reason":          reason,
				"response_status": responseStatus,
				"body":            string(body),
			})
		}
		return jobDoneMsg{}
	}
	defer resp.Body.Close()

	if pool != nil && currentMember != nil {
		pool.RecordSuccess(currentMember.ID)
	}

	body, _ := io.ReadAll(resp.Body)
	store.UpdateStatus(job.ID, StatusCompleted)
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
		enqueueWebhook(job.ID, job.UserID, job.WebhookURL, map[string]any{
			"job_id":          job.ID,
			"status":          "completed",
			"response_status": resp.StatusCode,
			"body":            string(body),
		})
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
	return msg
}

func pacingHeader(headers http.Header, name string) string {
	if val := headers.Get("X-Aqueduct-" + name); val != "" {
		return val
	}
	return headers.Get("X-Aquifer-" + name)
}

func makeRequest(ctx context.Context, job *Job, dispatchURL string, totalJobs, queueDepth int64, flowRate float64, l8 *L8Registry) (*http.Response, error) {
	var bodyReader io.Reader
	if job.Body != "" {
		bodyReader = strings.NewReader(job.Body)
	}

	req, err := http.NewRequestWithContext(ctx, job.Method, dispatchURL, bodyReader)
	if err != nil {
		return nil, err
	}

	for k, v := range job.Headers {
		req.Header.Set(k, v)
	}

	// L8 signing proves Aquifer's identity to the *receiver* of a webhook —
	// it has no meaning for forward dispatch to an arbitrary upstream API,
	// so this only applies when the job being dispatched is itself a
	// webhook delivery (see Job.isWebhookDeliveryJob).
	if job.isWebhookDeliveryJob() && l8 != nil {
		l8.EnsureTrust(dispatchURL)
		if l8.IsTrusted(dispatchURL) {
			for k, v := range l8.SignHeaders([]byte(job.Body)) {
				req.Header.Set(k, v)
			}
		}
	}

	setLoadHeader(req.Header, "Total-Jobs", fmt.Sprintf("%d", totalJobs))
	setLoadHeader(req.Header, "Queue-Depth", fmt.Sprintf("%d", queueDepth))
	setLoadHeader(req.Header, "Flow-Rate", fmt.Sprintf("%.2f", flowRate))

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

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

func setLoadHeader(headers http.Header, name, value string) {
	headers.Set("X-Aqueduct-"+name, value)
	headers.Set("X-Aquifer-"+name, value)
}
