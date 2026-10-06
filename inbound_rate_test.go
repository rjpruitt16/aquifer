package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// feed simulates one second of inserts at the given latency, then ticks.
func feed(c *InboundRateController, inserts int, latency time.Duration) InboundRateSnapshot {
	for i := 0; i < inserts; i++ {
		c.Observe("caller", latency, true)
	}
	c.tick(time.Now())
	return *c.Snapshot()
}

func TestInboundRateStartsAtPercentOfBenchmark(t *testing.T) {
	c := newInboundRateController(400, 40)
	if got := c.Snapshot().AdvertisedRPS; got != 160 {
		t.Fatalf("expected 40%% of 400 = 160, got %v", got)
	}
}

func TestInboundRateRampsFastBelowBenchmarkAndCarefullyAbove(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 160, time.Millisecond); snap.AdvertisedRPS != 240 || snap.LastAdjustment != "raise" {
		t.Fatalf("healthy and fully used below the benchmark should grow 1.5x to 240, got %+v", snap)
	}
	c.rate = 400
	if snap := feed(c, 400, time.Millisecond); snap.AdvertisedRPS != 420 {
		t.Fatalf("at the threshold (initially the benchmark) growth should slow to 5%% (420), got %+v", snap)
	}
}

func TestInboundRateCutsWhenInsertLatencyRises(t *testing.T) {
	c := newInboundRateController(400, 40)
	feed(c, 160, time.Millisecond) // establishes a ~1ms baseline
	before := c.Snapshot().AdvertisedRPS
	snap := feed(c, 200, 20*time.Millisecond)
	if snap.LastAdjustment != "cut: insert latency" || snap.AdvertisedRPS != round2(before*0.7) || snap.Cuts != 1 {
		t.Fatalf("expected a 30%% cut on a latency spike from %v, got %+v", before, snap)
	}
}

func TestInboundRateDoesNotInflateUnusedHeadroom(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 50, time.Millisecond); snap.AdvertisedRPS != 160 || snap.LastAdjustment != "hold" {
		t.Fatalf("callers using under 80%% of the rate must not raise it, got %+v", snap)
	}
}

func TestInboundRateHoldsWhenCallersOvershoot(t *testing.T) {
	c := newInboundRateController(400, 40)
	if snap := feed(c, 300, time.Millisecond); snap.LastAdjustment != "hold: callers over advertised rate" || snap.AdvertisedRPS != 160 {
		t.Fatalf("callers lagging a lower rate must not spiral it down, got %+v", snap)
	}
}

func TestInboundRateCutsOncePerCooldownAndLearnsTheKnee(t *testing.T) {
	c := newInboundRateController(400, 40)
	c.rate = 500
	feed(c, 400, time.Millisecond)
	c.rate = 500
	first := feed(c, 450, 20*time.Millisecond)
	if first.AdvertisedRPS != 350 || c.threshold != 450 {
		t.Fatalf("expected a cut to 350 with a learned knee of 450, got %+v threshold=%v", first, c.threshold)
	}
	for i := 0; i < 9; i++ {
		if snap := feed(c, 350, 20*time.Millisecond); snap.AdvertisedRPS != 350 {
			t.Fatalf("tick %d: within one congestion episode it must hold after the first cut, got %+v", i, snap)
		}
	}
	if snap := feed(c, 350, 20*time.Millisecond); snap.AdvertisedRPS != 245 {
		t.Fatalf("an episode lasting 10 ticks cuts again, got %+v", snap)
	}
}

func TestInboundRateSplitsAcrossActiveCallers(t *testing.T) {
	c := newInboundRateController(400, 40)
	for _, id := range []string{"a", "b", "c", "d"} {
		c.Observe(id, time.Millisecond, true)
	}
	if got := c.AdvertisedRPS("a"); got != 40 {
		t.Fatalf("160 split over 4 active callers should be 40 each, got %v", got)
	}
}

func TestInboundRateOffByDefault(t *testing.T) {
	if c := NewInboundRateController(); c != nil {
		t.Fatal("inbound pacing must be off unless AQUIFER_INBOUND_RPS_ENABLED is set")
	}
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	body, _ := json.Marshal(sampleJobRequest("u", "off-by-default"))
	rec := httptest.NewRecorder()
	NewServer(app).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if rec.Header().Get("X-Aqueduct-Rps") != "" {
		t.Fatal("no X-Aqueduct-Rps header should be sent while disabled")
	}
}

func TestInboundRateHeaderOnJobs(t *testing.T) {
	t.Setenv("AQUIFER_INBOUND_RPS_ENABLED", "true")
	app, _ := testAquiferWithLimits(t, AdmissionLimits{})
	body, _ := json.Marshal(sampleJobRequest("u", "header-on"))
	rec := httptest.NewRecorder()
	NewServer(app).Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if got := rec.Header().Get("X-Aqueduct-Rps"); got != "160.00" {
		t.Fatalf("expected the lone caller to be advertised the full 160 rps, got %q", got)
	}
}

func TestInboundRateCascadeKeepsTheFirstKnee(t *testing.T) {
	c := newInboundRateController(400, 40)
	c.rate = 500
	feed(c, 400, time.Millisecond)
	c.rate = 500
	feed(c, 450, 20*time.Millisecond) // first cut: knee 450
	for i := 0; i < 6; i++ {
		feed(c, 300, 20*time.Millisecond) // still congested: more cuts
	}
	if c.threshold != 450 {
		t.Fatalf("cuts during one congestion episode must keep the first knee (450), got %v", c.threshold)
	}
}
