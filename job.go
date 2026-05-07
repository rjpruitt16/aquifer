package main

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
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	WebhookURL    string            `json:"webhook_url"`
	Status        Status            `json:"status"`
	CreatedAt     int64             `json:"created_at"`
}

type JobRequest struct {
	UserID        string            `json:"user_id"`
	IdempotentKey string            `json:"idempotent_key"`
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	WebhookURL    string            `json:"webhook_url"`
}

func (r *JobRequest) Validate() string {
	switch {
	case r.UserID == "":
		return "user_id is required"
	case r.IdempotentKey == "":
		return "idempotent_key is required"
	case r.URL == "":
		return "url is required"
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
		Method:        strings.ToUpper(r.Method),
		Headers:       r.Headers,
		Body:          r.Body,
		WebhookURL:    r.WebhookURL,
		Status:        StatusQueued,
		CreatedAt:     time.Now().UnixMilli(),
	}
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
