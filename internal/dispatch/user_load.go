// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package dispatch

import (
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/rjpruitt16/aquifer/internal/jobs"
)

const defaultMaxPendingWebhooksPerUser int64 = 1000

// userLoad counts each user's accepted-but-unfinished work on this instance,
// separating their own jobs from the webhook deliveries owed to them.
type userLoad struct {
	jobs     int64
	webhooks int64
}

type userLoadTracker struct {
	mu    sync.Mutex
	users map[string]*userLoad
	// accepted counts every job and webhook delivery ever added, for the
	// instance's load (load.go).
	accepted atomic.Int64
}

func newUserLoadTracker() *userLoadTracker {
	return &userLoadTracker{users: map[string]*userLoad{}}
}

func (t *userLoadTracker) add(job *jobs.Job) {
	t.accepted.Add(1)
	t.mu.Lock()
	defer t.mu.Unlock()
	load := t.users[job.UserID]
	if load == nil {
		load = &userLoad{}
		t.users[job.UserID] = load
	}
	if job.IsWebhookDelivery() {
		load.webhooks++
	} else {
		load.jobs++
	}
}

// active is how many users have jobs or webhook deliveries still owed.
func (t *userLoadTracker) active() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int64(len(t.users))
}

func (t *userLoadTracker) done(job *jobs.Job) {
	t.mu.Lock()
	defer t.mu.Unlock()
	load := t.users[job.UserID]
	if load == nil {
		return
	}
	if job.IsWebhookDelivery() {
		load.webhooks = max(load.webhooks-1, 0)
	} else {
		load.jobs = max(load.jobs-1, 0)
	}
	if load.jobs == 0 && load.webhooks == 0 {
		delete(t.users, job.UserID)
	}
}

// webhookBacklogDecision rejects new work from a user whose undelivered
// webhooks exceed AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER, with a probability
// rising linearly to 1 at twice the limit, but only while other users are
// also active: a user alone on the instance is never throttled for it.
func (t *userLoadTracker) webhookBacklogDecision(userID string, draw float64) (rejected bool, limit, backlog int64) {
	limit = configuredMaxPendingWebhooksPerUser()
	if limit <= 0 {
		return false, limit, 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if load := t.users[userID]; load != nil {
		backlog = load.webhooks
	}
	othersActive := len(t.users) > 1 || (len(t.users) == 1 && t.users[userID] == nil)
	if !othersActive || backlog <= limit {
		return false, limit, backlog
	}
	probability := math.Min(float64(backlog-limit)/float64(limit), 1)
	return draw < probability, limit, backlog
}

func configuredMaxPendingWebhooksPerUser() int64 {
	raw := os.Getenv("AQUIFER_MAX_PENDING_WEBHOOKS_PER_USER")
	if raw == "" {
		return defaultMaxPendingWebhooksPerUser
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return defaultMaxPendingWebhooksPerUser
	}
	return value
}

// UserLoad is how many of userID's jobs and completion webhooks this node
// has pending.
func (r *Registry) UserLoad(userID string) (jobs, webhooks int64) {
	r.users.mu.Lock()
	defer r.users.mu.Unlock()
	if load := r.users.users[userID]; load != nil {
		return load.jobs, load.webhooks
	}
	return 0, 0
}
