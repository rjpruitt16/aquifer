package aquifer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNoopRegionAdapterIsEmptyByDefault(t *testing.T) {
	var a NoopRegionAdapter
	if got := a.LiveRegions(); got != nil {
		t.Fatalf("expected nil live regions, got %v", got)
	}
	if got := a.SelfRegion(); got != "" {
		t.Fatalf("expected empty self region, got %q", got)
	}
}

func TestEnsureRegionAdapterDefaultsNilToNoop(t *testing.T) {
	adapter := ensureRegionAdapter(nil)
	if _, ok := adapter.(NoopRegionAdapter); !ok {
		t.Fatalf("expected nil to default to NoopRegionAdapter, got %T", adapter)
	}
}

func TestEnsureRegionAdapterPassesThroughNonNil(t *testing.T) {
	fly := &FlyRegionAdapter{}
	adapter := ensureRegionAdapter(fly)
	if adapter != RegionAdapter(fly) {
		t.Fatalf("expected the given adapter to pass through unchanged")
	}
}

func TestNewFlyRegionAdapterReturnsNilWithoutRegionsConfigured(t *testing.T) {
	t.Setenv("AQUIFER_FLY_REGIONS", "")
	if a := NewFlyRegionAdapter(); a != nil {
		t.Fatalf("expected nil when AQUIFER_FLY_REGIONS is unset, got %+v", a)
	}
}

func TestNewFlyRegionAdapterReturnsNilForWhitespaceOnlyRegions(t *testing.T) {
	t.Setenv("AQUIFER_FLY_REGIONS", " , ,")
	if a := NewFlyRegionAdapter(); a != nil {
		t.Fatalf("expected nil when AQUIFER_FLY_REGIONS has no real entries, got %+v", a)
	}
}

func TestFlyRegionAdapterLiveRegionsReturnsDefensiveCopy(t *testing.T) {
	a := &FlyRegionAdapter{live: []string{"iad", "lhr"}}
	got := a.LiveRegions()
	got[0] = "mutated"
	if a.live[0] != "iad" {
		t.Fatalf("expected LiveRegions() to return a copy, internal state was mutated: %v", a.live)
	}
}

// fakeRegionServer is a stand-in for a real region's /health endpoint --
// real .internal DNS can't be resolved in a unit test environment (the
// plan's own verification section scopes real cross-region behavior to a
// deployed multi-region test, not local unit tests). This tests
// pollOnce's actual concurrency/aggregation/self-exclusion logic against
// real HTTP servers standing in for regions.
func fakeRegionServer(healthy bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
}

func TestFlyRegionAdapterPollOncePopulatesLiveRegionsExcludingUnhealthyAndSelf(t *testing.T) {
	healthyIAD := fakeRegionServer(true)
	defer healthyIAD.Close()
	unhealthyLHR := fakeRegionServer(false)
	defer unhealthyLHR.Close()

	servers := map[string]string{
		"iad": healthyIAD.URL,
		"lhr": unhealthyLHR.URL,
		"sjc": healthyIAD.URL, // reuse the healthy server for a second "region"
	}

	a := &FlyRegionAdapter{
		selfRegion: "sjc", // must be excluded even though its server is healthy
		regions:    []string{"iad", "lhr", "sjc"},
		httpClient: &http.Client{Timeout: 2 * time.Second},
		healthCheckURL: func(region string) string {
			return servers[region] + "/health"
		},
	}

	a.pollOnce()

	live := a.LiveRegions()
	if len(live) != 1 || live[0] != "iad" {
		t.Fatalf("expected only [iad] live (lhr unhealthy, sjc is self), got %v", live)
	}
}

func TestFlyRegionAdapterPollOnceOrdersLiveRegionsByLatencyAscending(t *testing.T) {
	fast := fakeRegionServer(true)
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	servers := map[string]string{
		"fast-region": fast.URL,
		"slow-region": slow.URL,
	}

	a := &FlyRegionAdapter{
		regions:    []string{"slow-region", "fast-region"}, // deliberately not already in latency order
		httpClient: &http.Client{Timeout: 2 * time.Second},
		healthCheckURL: func(region string) string {
			return servers[region] + "/health"
		},
	}

	a.pollOnce()

	live := a.LiveRegions()
	if len(live) != 2 || live[0] != "fast-region" || live[1] != "slow-region" {
		t.Fatalf("expected LiveRegions() ordered nearest-first by measured RTT [fast-region slow-region], got %v", live)
	}
}

func TestFlyRegionAdapterPollOnceTreatsUnreachableAsUnhealthy(t *testing.T) {
	a := &FlyRegionAdapter{
		regions:    []string{"xyz"},
		httpClient: &http.Client{Timeout: 500 * time.Millisecond},
		healthCheckURL: func(region string) string {
			return "http://127.0.0.1:1/health" // nothing listens here
		},
	}

	a.pollOnce()

	if live := a.LiveRegions(); len(live) != 0 {
		t.Fatalf("expected an unreachable region to be treated as not live, got %v", live)
	}
}

func TestNewFlyRegionAdapterParsesRegionListAndPort(t *testing.T) {
	t.Setenv("AQUIFER_FLY_REGIONS", " iad , lhr ,, sjc ")
	t.Setenv("PORT", "9090")
	t.Setenv("FLY_APP_NAME", "my-app")
	t.Setenv("FLY_REGION", "iad")

	a := NewFlyRegionAdapter()
	if a == nil {
		t.Fatalf("expected a real adapter with regions configured")
	}
	if got := strings.Join(a.regions, ","); got != "iad,lhr,sjc" {
		t.Fatalf("expected parsed/trimmed region list [iad lhr sjc], got %v", a.regions)
	}
	if a.SelfRegion() != "iad" {
		t.Fatalf("expected self region iad, got %q", a.SelfRegion())
	}
	url := a.healthCheckURL("lhr")
	if url != "http://lhr.my-app.internal:9090/health" {
		t.Fatalf("expected the real region-prefixed .internal URL, got %q", url)
	}
}
