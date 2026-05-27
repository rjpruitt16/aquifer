package main

import (
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
	minRPS     = 0.5
	maxRetries = 4
)

type jobDoneMsg struct {
	rps           *float64
	maxConcurrent *int
	accountQueue  *string
}

type AccountQueue struct {
	key        string
	upstream   string
	cmds       chan *Job
	done       chan jobDoneMsg
	store      *Store
	broker     *Broker
	l8         *L8Registry
	metrics    MetricsAdapter
	currentRPS atomic.Int64 // stored as rps * 100
}

func (q *AccountQueue) RPS() float64 {
	return float64(q.currentRPS.Load()) / 100
}

func NewAccountQueue(key, upstream string, rps float64, maxConc int, store *Store, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, onIdle func(string)) *AccountQueue {
	q := &AccountQueue{
		key:      key,
		upstream: upstream,
		cmds:     make(chan *Job, 1000),
		done:     make(chan jobDoneMsg, 100),
		store:    store,
		broker:   broker,
		l8:       l8,
		metrics:  ensureMetrics(metrics),
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
		for len(queue) > 0 && inFlight < maxConc {
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
			lastRequestAt = time.Now()

			q.store.MarkInFlight(job.ID)
			q.metrics.JobDispatched(job.UserID, q.upstream)

			currentRPS := rps
			go func(j *Job, flowRate float64) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[AccountQueue] panic executing job %s: %v", j.ID, r)
						q.store.UpdateStatus(j.ID, StatusFailed)
						q.metrics.JobFailed(j.UserID, q.upstream, "internal panic")
						deliverWebhook(j.WebhookURL, map[string]any{
							"job_id": j.ID,
							"status": "failed",
							"reason": "internal panic",
						}, q.l8, q.metrics)
						q.done <- jobDoneMsg{}
					}
				}()
				q.done <- execute(j, q.upstream, q.store, q.broker, q.l8, q.metrics, flowRate)
			}(job, currentRPS)
		}

		select {
		case job := <-q.cmds:
			queue = append(queue, job)
			q.metrics.QueueDepth(q.upstream, len(queue))
			idle.Reset(5 * time.Minute)

		case msg := <-q.done:
			inFlight--
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

func execute(job *Job, upstream string, store *Store, broker *Broker, l8 *L8Registry, metrics MetricsAdapter, flowRate float64) jobDoneMsg {
	metrics = ensureMetrics(metrics)
	startedAt := time.Now()

	broker.Publish(job.ID, SSEEvent{
		Event: "dispatching",
		Data:  map[string]any{"job_id": job.ID},
	})

	var resp *http.Response
	var err error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			log.Printf("[AccountQueue] retry %d/%d for %s in %s", attempt, maxRetries, job.URL, backoff)
			time.Sleep(backoff)
		}

		counts := store.Counts()
		resp, err = makeRequest(job, counts.TotalJobs, counts.QueueDepth, flowRate)
		if err != nil {
			continue
		}
		if resp.StatusCode >= 500 && attempt < maxRetries {
			resp.Body.Close()
			resp = nil
			continue
		}
		break
	}

	if err != nil || resp == nil {
		reason := "connection error"
		if err != nil {
			reason = err.Error()
		}
		store.UpdateStatus(job.ID, StatusFailed)
		broker.Publish(job.ID, SSEEvent{
			Event: "failed",
			Data:  map[string]any{"job_id": job.ID, "reason": reason},
		})
		metrics.JobFailed(job.UserID, upstream, reason)
		deliverWebhook(job.WebhookURL, map[string]any{
			"job_id": job.ID,
			"status": "failed",
			"reason": reason,
		}, l8, metrics)
		return jobDoneMsg{}
	}
	defer resp.Body.Close()

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
	deliverWebhook(job.WebhookURL, map[string]any{
		"job_id":          job.ID,
		"status":          "completed",
		"response_status": resp.StatusCode,
		"body":            string(body),
	}, l8, metrics)

	msg := jobDoneMsg{}
	if val := resp.Header.Get("X-Aquifer-Rps"); val != "" {
		var rps float64
		fmt.Sscanf(val, "%f", &rps)
		msg.rps = &rps
	}
	if val := resp.Header.Get("X-Aquifer-Max-Concurrent"); val != "" {
		var max int
		fmt.Sscanf(val, "%d", &max)
		msg.maxConcurrent = &max
	}
	if val := resp.Header.Get("X-Aquifer-Account-Queue"); val != "" {
		msg.accountQueue = &val
	}
	return msg
}

func makeRequest(job *Job, totalJobs, queueDepth int64, flowRate float64) (*http.Response, error) {
	var bodyReader io.Reader
	if job.Body != "" {
		bodyReader = strings.NewReader(job.Body)
	}

	req, err := http.NewRequest(job.Method, job.URL, bodyReader)
	if err != nil {
		return nil, err
	}

	for k, v := range job.Headers {
		req.Header.Set(k, v)
	}

	req.Header.Set("X-Aquifer-Total-Jobs", fmt.Sprintf("%d", totalJobs))
	req.Header.Set("X-Aquifer-Queue-Depth", fmt.Sprintf("%d", queueDepth))
	req.Header.Set("X-Aquifer-Flow-Rate", fmt.Sprintf("%.2f", flowRate))

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}
