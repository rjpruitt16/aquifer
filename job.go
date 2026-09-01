package aquifer

import (
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
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

	// Cross-region /proxy redirect fields — see proxy.go's AttemptDirect.
	// Absent/zero on a fresh top-level request; that absence IS the signal
	// "I'm the origin, nobody redirected this to me." OriginMachineID set
	// to someone else's ID means this instance must NOT itself originate a
	// further redirect tour — it just runs the normal local direct-attempt-
	// then-queue path, exactly as if this feature didn't exist.
	OriginMachineID string   `json:"origin_machine_id,omitempty"`
	OriginRegion    string   `json:"origin_region,omitempty"`
	VisitedRegions  []string `json:"visited_regions,omitempty"`
	RerouteCount    int      `json:"reroute_count,omitempty"`
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

	// Cross-region /proxy redirect fields — see Job's own doc comment and
	// proxy.go's AttemptDirect. Only ever set on an internal redirect hop
	// (one Aquifer instance calling another's /proxy directly); a real
	// caller never sets these.
	OriginMachineID string   `json:"origin_machine_id,omitempty"`
	OriginRegion    string   `json:"origin_region,omitempty"`
	VisitedRegions  []string `json:"visited_regions,omitempty"`
	RerouteCount    int      `json:"reroute_count,omitempty"`

	// DirectOnly is set on every redirect hop except the final one (the
	// deterministic-hash-selected target that's allowed to actually queue).
	// It tells the receiving instance "try a direct dispatch, but if you
	// can't, tell me cleanly — do NOT fall back to your own local queue."
	// Without this, a tour that moved on to a second candidate after a
	// first candidate quietly queued the job locally would leave the job
	// committed in two places at once — a real duplicate-delivery bug,
	// entirely within one origin's own tour, independent of the separate
	// cross-origin race already documented as an accepted gap. See
	// region_redirect.go.
	DirectOnly bool `json:"direct_only,omitempty"`

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
	case r.URL != "" && !domainAllowed(r.URL):
		return "url domain is not in the configured allowlist (AQUIFER_ALLOWED_URL_DOMAINS)"
	}
	return ""
}

// domainAllowed reports whether url's host is permitted to be dispatched to,
// per AQUIFER_ALLOWED_URL_DOMAINS (comma-separated hostnames). Defense-in-
// depth against the open-relay/SSRF risk already documented in API.md's
// POST /jobs warning: this doesn't replace "run Aquifer on a private network,
// put authorization in front" — it's a second, narrower layer Aquifer itself
// can enforce (which destinations, not who's allowed to call). Unset/empty
// (the default) means unrestricted, identical to today's behavior — this is
// opt-in, matching every other feature flag in this codebase. Pool-routed
// jobs have no caller-supplied URL (PoolRegistry addresses are trusted
// server-side config, not request input) and are never checked here.
//
// A host is allowed if it exactly matches an entry, or is a subdomain of one
// (e.g. "api.example.com" matches an allowlisted "example.com") — standard
// allowlist semantics, not a novel scheme.
func domainAllowed(rawURL string) bool {
	allowlist := os.Getenv("AQUIFER_ALLOWED_URL_DOMAINS")
	if allowlist == "" {
		return true
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := u.Hostname()

	for _, allowed := range strings.Split(allowlist, ",") {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

func NewJob(r *JobRequest) *Job {
	return &Job{
		ID:              generateID(),
		UserID:          r.UserID,
		IdempotentKey:   r.IdempotentKey,
		URL:             r.URL,
		PoolID:          r.PoolID,
		Method:          strings.ToUpper(r.Method),
		Headers:         r.Headers,
		Body:            r.Body,
		WebhookURL:      r.WebhookURL,
		Status:          StatusQueued,
		CreatedAt:       time.Now().UnixMilli(),
		OriginMachineID: r.OriginMachineID,
		OriginRegion:    r.OriginRegion,
		VisitedRegions:  r.VisitedRegions,
		RerouteCount:    r.RerouteCount,
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
