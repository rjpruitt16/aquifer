package aquifer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultRedirectGateCooldownSeconds = 500

// defaultRedirectTargetURL is the real, production URL builder for a
// redirect hop — the region-prefixed internal DNS form,
// <region>.$FLY_APP_NAME.internal, confirmed against Fly's own docs to
// resolve directly to that region's machines over the private 6PN
// network, bypassing the edge proxy entirely. Set as Aquifer.redirectTargetURL
// by NewAquifer; see that field's doc comment for why it's overridable.
func defaultRedirectTargetURL(region string) string {
	appName := os.Getenv("FLY_APP_NAME")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return fmt.Sprintf("http://%s.%s.internal:%s/proxy", region, appName, port)
}

// redirectGate answers "is attempting cross-region redirect itself worth
// trying right now" — distinct from URLWorker's per-domain breaker, which
// answers "is this one upstream domain healthy." Trips after a redirect
// tour finds no reachable alternate region at all; while tripped, /proxy
// skips redirect entirely and behaves exactly as it does without this
// feature — a safe, already-understood fallback state. Decided
// independently per-instance, no fleet coordination: each instance
// converges on the same conclusion within a request or two on its own.
type redirectGate struct {
	mu    sync.Mutex
	until time.Time
}

func (g *redirectGate) Open() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.until.IsZero() && time.Now().Before(g.until)
}

func (g *redirectGate) Trip(cooldown time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.until = time.Now().Add(cooldown)
}

func redirectGateCooldown() time.Duration {
	return time.Duration(envInt64("AQUIFER_REDIRECT_GATE_COOLDOWN_SECONDS", defaultRedirectGateCooldownSeconds)) * time.Second
}

var (
	selfMachineIDOnce  sync.Once
	selfMachineIDValue string
)

// selfMachineID identifies this instance for OriginMachineID purposes only
// — it's not a security boundary (see region_redirect.go's DirectOnly/
// OriginMachineID doc comments), just a way for a receiving instance to
// know a request already belongs to someone else's redirect tour. FLY_MACHINE_ID
// on Fly; hostname, then a random ID, as local-dev fallbacks.
func selfMachineID() string {
	selfMachineIDOnce.Do(func() {
		if id := os.Getenv("FLY_MACHINE_ID"); id != "" {
			selfMachineIDValue = id
			return
		}
		if host, err := os.Hostname(); err == nil && host != "" {
			selfMachineIDValue = host
			return
		}
		selfMachineIDValue = generateID()
	})
	return selfMachineIDValue
}

// rendezvousPick deterministically selects one candidate for a given key —
// rendezvous/highest-random-weight hashing. Two independent callers
// computing this over the same candidates and key always get the same
// answer, with no coordination between them. Used to choose which region
// gets first crack at owning a job's durable queue if no region can
// dispatch it directly: two origins racing the same idempotent_key (e.g. a
// caller's own retry-after-timeout landing on a different region) converge
// on the same final target as long as their known-live-region sets agree
// — narrowing, not closing, the documented cross-origin duplicate-delivery
// gap (Aquifer's idempotency check itself stays per-instance).
func rendezvousPick(candidates []string, key string) string {
	if len(candidates) == 0 {
		return ""
	}
	best := candidates[0]
	bestScore := rendezvousScore(best, key)
	for _, c := range candidates[1:] {
		if score := rendezvousScore(c, key); score > bestScore {
			best = c
			bestScore = score
		}
	}
	return best
}

func rendezvousScore(candidate, key string) string {
	sum := sha256.Sum256([]byte(candidate + ":" + key))
	// Comparing equal-length hex strings lexicographically matches
	// comparing the underlying bytes — no need to parse into a big.Int.
	return hex.EncodeToString(sum[:])
}

// orderedRedirectCandidates builds the tour order for a job: live regions
// minus self and anything already visited, with the rendezvous-preferred
// region always first (see rendezvousPick) and the rest randomized. There's
// no real latency/distance data available to do genuine nearest-first
// ordering, and a single fixed order for "second choice" candidates risks
// every origin piling onto the same region during a correlated outage —
// randomizing the remainder avoids that while still giving every origin
// racing the same job a shared first choice.
func orderedRedirectCandidates(live, visited []string, self, idempotentKey string) []string {
	visitedSet := make(map[string]bool, len(visited))
	for _, v := range visited {
		visitedSet[v] = true
	}

	var eligible []string
	for _, region := range live {
		if region == self || visitedSet[region] {
			continue
		}
		eligible = append(eligible, region)
	}
	if len(eligible) == 0 {
		return nil
	}

	preferred := rendezvousPick(eligible, idempotentKey)
	rest := make([]string, 0, len(eligible)-1)
	for _, region := range eligible {
		if region != preferred {
			rest = append(rest, region)
		}
	}
	rand.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })

	return append([]string{preferred}, rest...)
}

// attemptRedirect is called from AttemptDirect's local-fallback points
// (proxy.go) — never for a request already inside someone else's redirect
// tour, checked as this function's first line. It tours other known-live
// regions directly over Fly's private network before this instance falls
// back to its own local queue, mirroring what AttemptDirect already does
// for the local upstream, just aimed at sibling Aquifer instances.
//
// Returns ok=false when redirect isn't configured, is gated, or every
// candidate was tried without success — the caller proceeds with its own
// existing local-fallback behavior, completely unchanged. Redirect can
// only ever produce a BETTER outcome than today's baseline (a sibling
// served it instead of a local queue), never a worse one — a deliberate
// v1 simplification over "error the call on total exhaustion," which
// would need to reuse the admission-rejection response contract more
// invasively; left as a documented follow-up rather than risked here.
func (a *Aquifer) attemptRedirect(ctx context.Context, job *Job, accountQueueHeader string, timeout time.Duration) (ProxyOutcome, bool) {
	if job.OriginMachineID != "" {
		return ProxyOutcome{}, false
	}

	adapter := a.regionAdapterOrDefault()
	live := adapter.LiveRegions()
	if len(live) == 0 {
		return ProxyOutcome{}, false
	}

	if a.redirectGate.Open() {
		return ProxyOutcome{}, false
	}

	self := adapter.SelfRegion()
	job.OriginMachineID = selfMachineID()
	job.OriginRegion = self

	candidates := orderedRedirectCandidates(live, job.VisitedRegions, self, job.IdempotentKey)
	if len(candidates) == 0 {
		return ProxyOutcome{}, false
	}

	anyReached := false

	// Phase 1: try every live candidate for a fast direct success only —
	// none are allowed to commit to their own local queue yet (DirectOnly),
	// so trying several in sequence can never leave the job durably
	// committed in more than one place.
	for _, region := range candidates {
		outcome, reached := a.tryRedirectHop(ctx, job, region, accountQueueHeader, timeout, true)
		if reached {
			anyReached = true
		}
		if outcome != nil {
			a.store.DeleteJob(job.ID) // our own local row is now moot
			return *outcome, true
		}
	}

	// Phase 2: nobody could succeed directly — make exactly one committing
	// call, to the same deterministically-chosen candidate every origin
	// computing this function over the same inputs would also pick first
	// (candidates[0] by construction — see orderedRedirectCandidates).
	final := candidates[0]
	outcome, reached := a.tryRedirectHop(ctx, job, final, accountQueueHeader, timeout, false)
	if reached {
		anyReached = true
	}
	if outcome != nil {
		a.store.DeleteJob(job.ID)
		return *outcome, true
	}

	if anyReached {
		a.redirectGate.Trip(redirectGateCooldown())
	}
	return ProxyOutcome{}, false
}

// tryRedirectHop dials one candidate region directly over Fly's private 6PN
// network (<region>.$FLY_APP_NAME.internal — confirmed against Fly's own
// docs to resolve to that region's machines, bypassing the edge proxy
// entirely, no header needed for addressing) and interprets its response.
//
// A nil outcome with reached=true means the target gave a real, clean
// answer that just isn't usable (a DirectOnly rejection, or its own
// admission control saying no) — nothing was committed there, safe to try
// the next candidate. reached=false means the target couldn't be
// contacted at all (DNS, refused, timeout).
func (a *Aquifer) tryRedirectHop(ctx context.Context, job *Job, region, accountQueueHeader string, timeout time.Duration, directOnly bool) (*ProxyOutcome, bool) {
	hopReq := JobRequest{
		UserID:           job.UserID,
		IdempotentKey:    job.IdempotentKey,
		URL:              job.URL,
		Method:           job.Method,
		Headers:          job.Headers,
		Body:             job.Body,
		WebhookURL:       job.WebhookURL,
		OriginMachineID:  job.OriginMachineID,
		OriginRegion:     job.OriginRegion,
		VisitedRegions:   append(append([]string{}, job.VisitedRegions...), region),
		RerouteCount:     job.RerouteCount + 1,
		DirectOnly:       directOnly,
		AccountQueueMode: accountQueueHeader,
	}
	body, err := json.Marshal(hopReq)
	if err != nil {
		return nil, false
	}

	url := a.redirectTargetURL(region)

	// directOnly=true calls can never produce a streaming response (that's
	// the whole point of the flag), so they're safe to bound with the same
	// short per-hop timeout AttemptDirect's own local attempt uses. The
	// single directOnly=false (committing) call is different: if it turns
	// into an SSE stream, that stream needs to live as long as the real
	// caller's own connection does, not a short per-hop dial deadline — so
	// it uses ctx directly, exactly like server.go's own local
	// streamEvents relies on r.Context()'s natural lifetime rather than an
	// artificial timeout.
	reqCtx := ctx
	if directOnly {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(httpReq)
	if err != nil {
		return nil, false
	}

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		// Target accepted it into its own durable queue — relay its
		// stream live. server.go's proxyJob owns closing resp.Body once
		// the stream ends.
		return &ProxyOutcome{RelayFrom: resp}, true
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &ProxyOutcome{Direct: true, Status: resp.StatusCode, Header: resp.Header, Body: respBody}, true
	}

	// A clean, well-formed rejection (DirectOnly's 503, an admission 429,
	// etc.) — reached the target, it gave a real answer, just not a usable
	// one.
	return nil, true
}
