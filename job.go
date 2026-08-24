package aquifer

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusInFlight  Status = "in_flight"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID            string            `json:"id"`
	UserID        string            `json:"user_id"`
	IdempotentKey string            `json:"idempotent_key"`
	URL           string            `json:"url,omitempty"`
	PoolID        string            `json:"pool_id,omitempty"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	WebhookURL    string            `json:"webhook_url"`
	Status        Status            `json:"status"`
	CreatedAt     int64             `json:"created_at"`
}

// LedgerEntry is one row of the drain-mode idempotency ledger -- hash-only,
// never the plaintext idempotent key. See drain.go.
type LedgerEntry struct {
	HashKey string `json:"idempotent_key_hash"`
	JobID   string `json:"job_id"`
	Status  Status `json:"status"`
}

type JobRequest struct {
	UserID        string            `json:"user_id"`
	IdempotentKey string            `json:"idempotent_key"`
	URL           string            `json:"url,omitempty"`
	PoolID        string            `json:"pool_id,omitempty"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	WebhookURL    string            `json:"webhook_url"`

	// AccountQueueMode is never read from the request body — it's set by the
	// HTTP adapter from the X-Aqueduct-Account-Queue / X-Aquifer-Account-Queue
	// request header, the only source of truth for this setting. Empty means
	// "no opinion, leave the upstream's current mode unchanged."
	AccountQueueMode string `json:"-"`
}

func (r *JobRequest) Validate() string {
	switch {
	case r.UserID == "":
		return "user_id is required"
	case r.IdempotentKey == "":
		return "idempotent_key is required"
	case r.URL == "" && r.PoolID == "":
		return "either url or pool_id is required"
	case r.URL != "" && r.PoolID != "":
		return "url and pool_id are mutually exclusive — a job dispatches to one or the other"
	case r.Method == "":
		return "method is required"
	case r.WebhookURL == "":
		return "webhook_url is required"
	}
	return ""
}

func NewJob(r *JobRequest) *Job {
	return &Job{
		ID:            generateID(),
		UserID:        r.UserID,
		IdempotentKey: r.IdempotentKey,
		URL:           r.URL,
		PoolID:        r.PoolID,
		Method:        strings.ToUpper(r.Method),
		Headers:       r.Headers,
		Body:          r.Body,
		WebhookURL:    r.WebhookURL,
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}
}

// isWebhookDeliveryJob reports whether this job represents a webhook
// delivery attempt itself (constructed by Registry.EnqueueWebhook to push
// webhook delivery through the same account-queue pacing as forward
// dispatch), as opposed to a regular user-submitted job. A regular job
// always has a non-empty WebhookURL — JobRequest.Validate rejects an empty
// one — so an empty WebhookURL is a safe, already-enforced signal rather
// than a separate field: it's what execute() checks to avoid enqueueing a
// webhook-about-a-webhook, and what makeRequest checks to decide whether to
// L8-sign the outbound request.
func (j *Job) isWebhookDeliveryJob() bool {
	return j.WebhookURL == ""
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
