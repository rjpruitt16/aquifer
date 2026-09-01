package aquifer

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultFlyPollIntervalSeconds = 30
	flyHealthCheckTimeout         = 5 * time.Second
)

// FlyRegionAdapter polls sibling regions over Fly's private 6PN network to
// know which are currently live, backing /proxy's cross-region redirect
// (proxy.go's AttemptDirect). Region enumeration is explicit config only
// (AQUIFER_FLY_REGIONS) -- Aquifer never silently polls regions a deployer
// didn't say to use.
type FlyRegionAdapter struct {
	appName      string
	selfRegion   string
	port         string
	regions      []string
	pollInterval time.Duration
	httpClient   *http.Client

	// healthCheckURL builds the URL checked for a given region. Set to the
	// real .internal DNS builder by NewFlyRegionAdapter; overridable in
	// tests (which can't resolve real Fly private-network DNS) to point at
	// local httptest servers instead -- same "injectable for testability,
	// real by default" pattern as account_queue.go's retrySleepFunc.
	healthCheckURL func(region string) string

	mu   sync.RWMutex
	live []string
}

// NewFlyRegionAdapter constructs the adapter and starts its background
// polling loop. Returns nil if AQUIFER_FLY_REGIONS isn't set -- the feature
// stays off unless explicitly configured, matching every other opt-in
// feature in this codebase. Callers should treat a nil return as "use
// NoopRegionAdapter" (ensureRegionAdapter already does this for a nil
// RegionAdapter passed to Aquifer.SetRegionAdapter).
func NewFlyRegionAdapter() *FlyRegionAdapter {
	regionsEnv := os.Getenv("AQUIFER_FLY_REGIONS")
	if regionsEnv == "" {
		return nil
	}

	var regions []string
	for _, r := range strings.Split(regionsEnv, ",") {
		r = strings.TrimSpace(r)
		if r != "" {
			regions = append(regions, r)
		}
	}
	if len(regions) == 0 {
		return nil
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	pollInterval := time.Duration(envInt64("AQUIFER_FLY_POLL_INTERVAL_SECONDS", defaultFlyPollIntervalSeconds)) * time.Second

	appName := os.Getenv("FLY_APP_NAME")
	a := &FlyRegionAdapter{
		appName:      appName,
		selfRegion:   os.Getenv("FLY_REGION"),
		port:         port,
		regions:      regions,
		pollInterval: pollInterval,
		httpClient:   &http.Client{Timeout: flyHealthCheckTimeout},
	}
	a.healthCheckURL = func(region string) string {
		return fmt.Sprintf("http://%s.%s.internal:%s/health", region, appName, port)
	}

	// Populate immediately (bounded by flyHealthCheckTimeout since checks
	// run concurrently, not len(regions)*timeout) rather than leaving
	// LiveRegions() empty for up to a full pollInterval after boot.
	a.pollOnce()
	go a.pollLoop()
	return a
}

func (a *FlyRegionAdapter) LiveRegions() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]string, len(a.live))
	copy(out, a.live)
	return out
}

func (a *FlyRegionAdapter) SelfRegion() string {
	return a.selfRegion
}

func (a *FlyRegionAdapter) pollLoop() {
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()
	for range ticker.C {
		a.pollOnce()
	}
}

// pollOnce checks every configured region concurrently -- a real
// improvement over sequential polling (cheap in Go; one slow/unreachable
// region no longer delays finding out about the rest) -- and atomically
// replaces the live list with this cycle's results.
func (a *FlyRegionAdapter) pollOnce() {
	var wg sync.WaitGroup
	results := make(chan string, len(a.regions))

	for _, region := range a.regions {
		if region == a.selfRegion {
			continue
		}
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			if a.regionHealthy(region) {
				results <- region
			}
		}(region)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var live []string
	for region := range results {
		live = append(live, region)
	}

	a.mu.Lock()
	a.live = live
	a.mu.Unlock()
}

// regionHealthy checks one region. In production this hits the
// region-prefixed internal DNS form -- <region>.$FLY_APP_NAME.internal,
// confirmed against Fly's own docs to resolve directly to that region's
// machines over the private 6PN network, bypassing the edge proxy entirely.
// No header needed for addressing, unlike the public-proxy fly-prefer-region
// trick some reference implementations use. See healthCheckURL's doc
// comment for how tests substitute a reachable target.
func (a *FlyRegionAdapter) regionHealthy(region string) bool {
	resp, err := a.httpClient.Get(a.healthCheckURL(region))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
