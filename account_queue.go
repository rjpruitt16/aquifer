package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
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
	key   string
	cmds  chan *Job
	done  chan jobDoneMsg
	store *Store
}

func NewAccountQueue(key string, rps float64, maxConc int, store *Store, onIdle func(string)) *AccountQueue {
	q := &AccountQueue{
		key:   key,
		cmds:  make(chan *Job, 1000),
		done:  make(chan jobDoneMsg, 100),
		store: store,
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
			// reclaim jobs that were in-flight when the goroutine died
			recovered := q.store.RecoverInFlight(q.key)
			for _, j := range recovered {
				log.Printf("[AccountQueue] recovered in_flight job %s after panic", j.ID)
				q.cmds <- j
			}
			continue
		}

		// normal idle exit — nothing left to do
		if len(q.cmds) == 0 {
			onIdle(q.key)
			return
		}
	}
}

func (q *AccountQueue) run(configuredRPS float64, configuredMaxConc int) {
	idle := time.NewTimer(5 * time.Minute)
	defer idle.Stop()

	rps := configuredRPS
	maxConc := configuredMaxConc
	lastRequestAt := time.Time{}
	inFlight := 0
	queue := make([]*Job, 0, 64)

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
			inFlight++
			lastRequestAt = time.Now()

			q.store.MarkInFlight(job.ID)

			go func(j *Job) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[AccountQueue] panic executing job %s: %v", j.ID, r)
						q.store.UpdateStatus(j.ID, StatusFailed)
						deliverWebhook(j.WebhookURL, map[string]any{
							"job_id": j.ID,
							"status": "failed",
							"reason": "internal panic",
						})
						q.done <- jobDoneMsg{}
					}
				}()
				q.done <- execute(j, q.store)
			}(job)
		}

		select {
		case job := <-q.cmds:
			queue = append(queue, job)
			idle.Reset(5 * time.Minute)

		case msg := <-q.done:
			inFlight--
			if msg.rps != nil {
				// headers are final say — but cannot exceed what we configured
				rps = math.Max(math.Min(*msg.rps, configuredRPS), minRPS)
			} else if rps < configuredRPS {
				// gradually recover toward configured ceiling when no restriction
				rps = math.Min(rps*1.05, configuredRPS)
			}
			if msg.maxConcurrent != nil && *msg.maxConcurrent > 0 {
				maxConc = int(math.Min(float64(*msg.maxConcurrent), float64(configuredMaxConc)))
			}
			idle.Reset(5 * time.Minute)

		case <-idle.C:
			if len(queue) == 0 && inFlight == 0 {
				return
			}
			idle.Reset(5 * time.Minute)
		}
	}
}

func execute(job *Job, store *Store) jobDoneMsg {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			log.Printf("[AccountQueue] retry %d/%d for %s in %s", attempt, maxRetries, job.URL, backoff)
			time.Sleep(backoff)
		}

		resp, err = makeRequest(job)
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
		deliverWebhook(job.WebhookURL, map[string]any{
			"job_id": job.ID,
			"status": "failed",
			"reason": reason,
		})
		return jobDoneMsg{}
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	store.UpdateStatus(job.ID, StatusCompleted)
	deliverWebhook(job.WebhookURL, map[string]any{
		"job_id":          job.ID,
		"status":          "completed",
		"response_status": resp.StatusCode,
		"body":            string(body),
	})

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

func makeRequest(job *Job) (*http.Response, error) {
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

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}
