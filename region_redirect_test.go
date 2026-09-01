package aquifer

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRegionAdapter is a test double for RegionAdapter — a plain, static
// live-region list, no polling. Real polling behavior (FlyRegionAdapter)
// has its own tests in region_adapter_test.go; these tests are about
// AttemptDirect/attemptRedirect's orchestration, not region discovery.
type fakeRegionAdapter struct {
	live []string
	self string
}

func (f fakeRegionAdapter) LiveRegions() []string { return f.live }
func (f fakeRegionAdapter) SelfRegion() string     { return f.self }

func TestRendezvousPickIsDeterministic(t *testing.T) {
	candidates := []string{"iad", "lhr", "sjc", "nrt"}
	first := rendezvousPick(candidates, "some-idempotent-key")

	// Same inputs, different slice order -- must still pick the same
	// winner, since two independent origins won't necessarily enumerate
	// their known-live regions in the same order.
	reordered := []string{"nrt", "sjc", "lhr", "iad"}
	second := rendezvousPick(reordered, "some-idempotent-key")

	if first != second {
		t.Fatalf("expected the same pick regardless of input order, got %q vs %q", first, second)
	}

	// A different key can legitimately pick a different region -- just
	// confirm it's still deterministic for ITS OWN key.
	third := rendezvousPick(candidates, "some-idempotent-key")
	if third != first {
		t.Fatalf("expected repeat calls with the same inputs to agree, got %q vs %q", first, third)
	}
}

func TestRendezvousPickEmptyCandidates(t *testing.T) {
	if got := rendezvousPick(nil, "key"); got != "" {
		t.Fatalf("expected empty string for no candidates, got %q", got)
	}
}

func TestOrderedRedirectCandidatesExcludesSelfAndVisited(t *testing.T) {
	live := []string{"iad", "lhr", "sjc", "nrt"}
	visited := []string{"sjc"}
	candidates := orderedRedirectCandidates(live, visited, "iad", "some-key")

	for _, c := range candidates {
		if c == "iad" {
			t.Fatalf("expected self (iad) excluded from candidates, got %v", candidates)
		}
		if c == "sjc" {
			t.Fatalf("expected already-visited (sjc) excluded from candidates, got %v", candidates)
		}
	}
	if len(candidates) != 2 {
		t.Fatalf("expected exactly 2 remaining candidates (lhr, nrt), got %v", candidates)
	}
}

func TestOrderedRedirectCandidatesPreferredIsAlwaysFirst(t *testing.T) {
	live := []string{"iad", "lhr", "sjc", "nrt", "fra"}
	preferred := rendezvousPick(live, "some-key")

	candidates := orderedRedirectCandidates(live, nil, "", "some-key")
	if len(candidates) == 0 || candidates[0] != preferred {
		t.Fatalf("expected the rendezvous-preferred region (%q) first, got %v", preferred, candidates)
	}
}

func TestOrderedRedirectCandidatesEmptyWhenNothingEligible(t *testing.T) {
	candidates := orderedRedirectCandidates([]string{"iad"}, nil, "iad", "key")
	if candidates != nil {
		t.Fatalf("expected nil when the only live region is self, got %v", candidates)
	}
}

func TestRedirectGateOpenClosedAndCooldown(t *testing.T) {
	g := &redirectGate{}
	if g.Open() {
		t.Fatalf("expected a fresh gate to be closed")
	}

	g.Trip(50 * time.Millisecond)
	if !g.Open() {
		t.Fatalf("expected the gate to be open immediately after tripping")
	}

	time.Sleep(60 * time.Millisecond)
	if g.Open() {
		t.Fatalf("expected the gate to close again after its cooldown elapsed")
	}
}

func TestSelfMachineIDIsStableAndReadsFlyMachineID(t *testing.T) {
	// selfMachineIDOnce/selfMachineIDValue are package-level and cached
	// after first use in the real binary, but Setenv + a direct reset
	// here confirms the env-var-read path itself, isolated from whatever
	// other tests may have already triggered the sync.Once in this
	// process.
	selfMachineIDOnce = sync.Once{}
	t.Setenv("FLY_MACHINE_ID", "test-machine-123")

	first := selfMachineID()
	second := selfMachineID()

	if first != "test-machine-123" {
		t.Fatalf("expected FLY_MACHINE_ID to be used, got %q", first)
	}
	if first != second {
		t.Fatalf("expected repeat calls to return the same cached value, got %q vs %q", first, second)
	}
}

// buildRedirectTestPair constructs two independent, fully real Aquifer +
// Server pairs -- "origin" and "target" -- with origin's redirectTargetURL
// overridden to dial target's real httptest.Server instead of real Fly
// .internal DNS (which can't resolve in a unit test environment; see
// region_adapter_test.go's identical justification for FlyRegionAdapter).
// origin's RegionAdapter reports exactly one live region, "target-region",
// which resolves to target's server.
func buildRedirectTestPair(t *testing.T) (origin *Aquifer, originSrv *httptest.Server, target *Aquifer) {
	t.Helper()

	origin, _ = testAquiferWithLimits(t, AdmissionLimits{})
	target, _ = testAquiferWithLimits(t, AdmissionLimits{})

	targetServer := NewServer(target)
	targetHTTP := httptest.NewServer(targetServer.Routes())
	t.Cleanup(targetHTTP.Close)

	origin.SetRegionAdapter(fakeRegionAdapter{live: []string{"target-region"}, self: "origin-region"})
	origin.redirectTargetURL = func(region string) string {
		if region == "target-region" {
			return targetHTTP.URL + "/proxy"
		}
		return "http://127.0.0.1:1/proxy" // unreachable, shouldn't be dialed in these tests
	}

	originServer := NewServer(origin)
	originHTTP := httptest.NewServer(originServer.Routes())
	t.Cleanup(originHTTP.Close)

	return origin, originHTTP, target
}

func TestRedirectSucceedsDirectlyOnTargetRegion(t *testing.T) {
	origin, originHTTP, target := buildRedirectTestPair(t)

	// A single, real, always-healthy upstream both origin and target would
	// dispatch the SAME job.URL to. To make origin fail locally while
	// target succeeds against that identical URL, pre-trip origin's own
	// per-domain breaker for it directly -- origin's AttemptDirect then
	// takes the domain_degraded fallback path without ever calling the
	// upstream at all, exactly as it would for a real recently-overloaded
	// domain. target has never seen this URL, so its own breaker is
	// closed and its real attempt succeeds.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", "target-region")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("served by target region"))
	}))
	defer upstream.Close()

	throwawayJob := NewJob(&JobRequest{UserID: "user-1", IdempotentKey: "throwaway", URL: upstream.URL, Method: "GET"})
	origin.registry.workerFor(throwawayJob).TripBreaker(time.Minute)
	_ = target

	reqBody, _ := json.Marshal(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "redirect-direct-success-key",
		URL:           upstream.URL,
		Method:        "GET",
		WebhookURL:    "https://example.com/callback",
	})

	resp, err := http.Post(originHTTP.URL+"/proxy", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("request to origin failed: %v", err)
	}
	defer resp.Body.Close()

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 relayed from the target region, got %d: %s", resp.StatusCode, body[:n])
	}
	if resp.Header.Get("X-Served-By") != "target-region" {
		t.Fatalf("expected the target region's response header relayed verbatim, got headers: %v", resp.Header)
	}
	if string(body[:n]) != "served by target region" {
		t.Fatalf("expected the target region's body relayed verbatim, got: %s", body[:n])
	}
	if got := resp.Header.Get("X-Aquifer-Served-By-Region"); got != "target-region" {
		t.Fatalf("expected X-Aquifer-Served-By-Region: target-region so the client can tell this was rerouted, got %q", got)
	}
}

func TestRedirectFallsBackToTargetsQueueWhenNoRegionCanDispatchDirectly(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	origin, originHTTP, target := buildRedirectTestPair(t)

	// This upstream fails everywhere -- neither origin nor target can
	// dispatch it directly, so the phase-2 committing hop should land on
	// target's own durable queue, and origin should relay that live SSE
	// stream back to the caller.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	throwawayJob := NewJob(&JobRequest{UserID: "user-1", IdempotentKey: "throwaway2", URL: upstream.URL, Method: "GET"})
	origin.registry.workerFor(throwawayJob).TripBreaker(time.Minute)
	_ = target

	reqBody, _ := json.Marshal(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "redirect-fallback-queue-key",
		URL:           upstream.URL,
		Method:        "GET",
		WebhookURL:    "https://example.com/callback",
	})

	resp, err := http.Post(originHTTP.URL+"/proxy", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("request to origin failed: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected origin to relay target's SSE stream, got Content-Type %q", ct)
	}

	buf := make([]byte, 4096)
	n, _ := io.ReadFull(io.LimitReader(resp.Body, 4096), buf)
	out := string(buf[:n])
	if !strings.Contains(out, "event: proxy_fallback") {
		t.Fatalf("expected the relayed stream to include target's own proxy_fallback event, got: %s", out)
	}
	if !strings.Contains(out, "event: queued") {
		t.Fatalf("expected the relayed stream to include target's own queued event, got: %s", out)
	}

	reroutedIdx := strings.Index(out, "event: rerouted")
	if reroutedIdx == -1 {
		t.Fatalf("expected origin to announce the reroute before relaying target's stream, got: %s", out)
	}
	if !strings.Contains(out, `"region":"target-region"`) {
		t.Fatalf("expected the rerouted event to name which region the job actually landed on, got: %s", out)
	}
	if fallbackIdx := strings.Index(out, "event: proxy_fallback"); reroutedIdx > fallbackIdx {
		t.Fatalf("expected rerouted to be announced BEFORE target's own proxy_fallback event, not after, got: %s", out)
	}
}

func TestRedirectDoesNotOriginateFromAnAlreadyRedirectedRequest(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	origin, originHTTP, _ := buildRedirectTestPair(t)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	throwawayJob := NewJob(&JobRequest{UserID: "user-1", IdempotentKey: "throwaway3", URL: upstream.URL, Method: "GET"})
	origin.registry.workerFor(throwawayJob).TripBreaker(time.Minute)

	// This request already carries OriginMachineID -- as if it were itself
	// a redirect hop from some OTHER instance. origin must not try to
	// redirect it further, even though its own local attempt will also
	// fail (breaker tripped above) -- it must fall straight through to
	// its own local queue+stream, exactly like today's existing fallback
	// behavior with the feature absent entirely.
	reqBody, _ := json.Marshal(JobRequest{
		UserID:          "user-1",
		IdempotentKey:   "already-redirected-key",
		URL:             upstream.URL,
		Method:          "GET",
		WebhookURL:      "https://example.com/callback",
		OriginMachineID: "some-other-machine",
		OriginRegion:    "some-other-region",
	})

	resp, err := http.Post(originHTTP.URL+"/proxy", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("request to origin failed: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected origin's OWN local fallback stream (not a relay), got Content-Type %q", ct)
	}

	buf := make([]byte, 4096)
	n, _ := io.ReadFull(io.LimitReader(resp.Body, 4096), buf)
	out := string(buf[:n])
	if !strings.Contains(out, `"reason":"domain_degraded"`) {
		t.Fatalf("expected origin's own domain_degraded fallback reason (not a redirected outcome), got: %s", out)
	}
}

func TestRedirectExhaustionReturnsHardErrorNotLocalQueue(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })
	t.Setenv("AQUIFER_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS", "123")

	origin, _ := testAquiferWithLimits(t, AdmissionLimits{})
	origin.SetRegionAdapter(fakeRegionAdapter{live: []string{"target-region"}, self: "origin-region"})
	// Deliberately unreachable -- nothing listens here, so every hop in
	// both phases comes back reached=false, forcing total exhaustion.
	origin.redirectTargetURL = func(region string) string { return "http://127.0.0.1:1/proxy" }

	originServer := NewServer(origin)
	originHTTP := httptest.NewServer(originServer.Routes())
	t.Cleanup(originHTTP.Close)

	// origin's own local upstream is also broken, so AttemptDirect takes
	// the domain_degraded fallback path and attemptRedirect actually runs
	// -- exactly the real-world trigger, not a synthetic direct call.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer upstream.Close()
	throwawayJob := NewJob(&JobRequest{UserID: "user-1", IdempotentKey: "throwaway4", URL: upstream.URL, Method: "GET"})
	origin.registry.workerFor(throwawayJob).TripBreaker(time.Minute)

	reqBody, _ := json.Marshal(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "redirect-exhausted-key",
		URL:           upstream.URL,
		Method:        "GET",
		WebhookURL:    "https://example.com/callback",
	})

	resp, err := http.Post(originHTTP.URL+"/proxy", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("request to origin failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on total redirect exhaustion, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "123" {
		t.Fatalf("expected Retry-After to use AQUIFER_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS (123), got %q", got)
	}

	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), `"limit_reason":"redirect_exhausted"`) {
		t.Fatalf("expected limit_reason=redirect_exhausted in the error body, got: %s", body[:n])
	}
}
