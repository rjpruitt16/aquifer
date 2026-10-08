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
	ExecuteBefore int64             `json:"execute_before,omitempty"`

	IdempotencyScope string `json:"idempotency_scope,omitempty"`
	// MaxRetries is resolved at creation: DefaultMaxRetries unless the caller
	// set one, RetryUntilComplete (-1) for no attempt limit. Attempts counts
	// failed attempts already retried.
	MaxRetries int `json:"max_retries"`
	Attempts   int `json:"attempts,omitempty"`
	// DedupHash caches dedupHash's result. SQLite never stores the plaintext
	// idempotent key, so a job reloaded from it can only recover the hash
	// from the idempotent_key_hash column, not recompute it.
	DedupHash string `json:"-"`

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

// DrainEvent is the durable, acknowledged unit used by batched drain
// streaming. It intentionally carries the same hash-only ledger data as
// LedgerEntry, plus a monotonic local sequence so a receiver can treat
// batches idempotently and Aquifer can delete only acknowledged events.
type DrainEvent struct {
	Sequence   int64  `json:"sequence"`
	HashKey    string `json:"idempotent_key_hash"`
	JobID      string `json:"job_id"`
	Status     Status `json:"status"`
	RecordedAt int64  `json:"recorded_at"`
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
	ExecuteBefore int64             `json:"execute_before,omitempty"`

	// IdempotencyScope "shared" dedups on idempotent_key alone, across every
	// user_id, so concurrent callers asking for the same resource coalesce
	// onto one upstream call and one cached result. Empty or "user" keeps
	// the default per-user scope. Requires AQUIFER_SHARED_IDEMPOTENCY_ENABLED.
	IdempotencyScope string `json:"idempotency_scope,omitempty"`

	// MaxRetries caps retries of retryable failures (connection errors, 5xx,
	// 408, 429). Nil means DefaultMaxRetries; -1 retries until the job
	// succeeds, bounded by execute_before or the queued-job TTL. The
	// X-Aqueduct-Max-Retries request header overrides this field.
	MaxRetries *int `json:"max_retries,omitempty"`

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
	case r.ExecuteBefore < 0:
		return "execute_before must be a positive Unix timestamp in milliseconds"
	case r.UserID == "":
		return "user_id is required"
	case strings.ContainsRune(r.UserID, 0):
		return "user_id must not contain NUL characters"
	case r.IdempotentKey == "":
		return "idempotent_key is required"
	case r.MaxRetries != nil && (*r.MaxRetries < RetryUntilComplete || *r.MaxRetries > MaxAllowedRetries):
		return "max_retries must be -1 (retry until complete) or between 0 and 100"
	case r.IdempotencyScope != "" && r.IdempotencyScope != IdempotencyScopeUser && r.IdempotencyScope != IdempotencyScopeShared:
		return `idempotency_scope must be "user" or "shared"`
	case r.IdempotencyScope == IdempotencyScopeShared && !sharedIdempotencyEnabled():
		return `idempotency_scope "shared" requires AQUIFER_SHARED_IDEMPOTENCY_ENABLED=true`
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

const (
	DefaultMaxRetries  = 4
	RetryUntilComplete = -1
	MaxAllowedRetries  = 100
)

const (
	IdempotencyScopeUser   = "user"
	IdempotencyScopeShared = "shared"
)

func sharedIdempotencyEnabled() bool {
	return envBool("AQUIFER_SHARED_IDEMPOTENCY_ENABLED", false)
}

// dedupHash is the one place an idempotency identity is derived. The shared
// form can't collide with a per-user one: a per-user key would need a user_id
// containing NUL, which Validate rejects.
func dedupHash(userID, idempotentKey, scope string) string {
	if scope == IdempotencyScopeShared {
		return hashKey("shared\x00" + idempotentKey)
	}
	return hashKey(userID + ":" + idempotentKey)
}

func (j *Job) dedupHash() string {
	if j.DedupHash != "" {
		return j.DedupHash
	}
	return dedupHash(j.UserID, j.IdempotentKey, j.IdempotencyScope)
}

func NewJob(r *JobRequest) *Job {
	return &Job{
		ID:               generateID(),
		UserID:           r.UserID,
		IdempotentKey:    r.IdempotentKey,
		IdempotencyScope: r.IdempotencyScope,
		MaxRetries:       resolveMaxRetries(r.MaxRetries),
		DedupHash:        dedupHash(r.UserID, r.IdempotentKey, r.IdempotencyScope),
		URL:              r.URL,
		PoolID:           r.PoolID,
		Method:           strings.ToUpper(r.Method),
		Headers:          r.Headers,
		Body:             r.Body,
		WebhookURL:       r.WebhookURL,
		Status:           StatusQueued,
		CreatedAt:        time.Now().UnixMilli(),
		ExecuteBefore:    r.ExecuteBefore,
		OriginMachineID:  r.OriginMachineID,
		OriginRegion:     r.OriginRegion,
		VisitedRegions:   r.VisitedRegions,
		RerouteCount:     r.RerouteCount,
	}
}

func resolveMaxRetries(requested *int) int {
	if requested == nil {
		return DefaultMaxRetries
	}
	return *requested
}

// retryDeadline is when a job stops being retried: its execute_before if
// set, otherwise the end of the queued-job TTL. This is the bound that keeps
// RetryUntilComplete from retrying forever.
func (j *Job) retryDeadline() time.Time {
	if j.ExecuteBefore > 0 {
		return time.UnixMilli(j.ExecuteBefore)
	}
	return time.UnixMilli(j.CreatedAt).Add(ttlQueued)
}

func (j *Job) hasRetriesLeft() bool {
	return j.MaxRetries == RetryUntilComplete || j.Attempts < j.MaxRetries
}

// ExecutionExpired reports whether this job's caller-supplied dispatch
// deadline has passed. A zero deadline means the job may wait indefinitely.
func (j *Job) ExecutionExpired(now time.Time) bool {
	return j != nil && j.ExecuteBefore > 0 && now.UnixMilli() >= j.ExecuteBefore
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
