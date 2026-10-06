package aquifer

import (
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// InboundRateController decides the X-Aqueduct-Rps Aquifer advertises to
// its own callers (including other Aquifers), the server side of pacing.
//
// The expensive part of a submission is the durable insert, so the
// controller watches inserts per second and insert latency:
//   - it starts at AQUIFER_INBOUND_START_PERCENT (default 40) of
//     AQUIFER_INBOUND_BENCHMARK_RPS (default 400 inserts/s);
//   - it cuts 30% when p95 insert latency rises well above its healthy
//     baseline, at most once per 3s so callers (a round trip behind) can
//     react before it cuts again;
//   - each cut remembers a threshold just under the failing rate (TCP's
//     ssthresh); the benchmark is the initial threshold;
//   - it raises only while latency is healthy and callers are using at
//     least 80% of the rate: 1.5x per second below the threshold, 5% above
//     it, so it approaches the knee slowly instead of sawtoothing past it.
//
// Each active caller (user_id seen in the last few seconds) is advertised an
// equal share of the total.
type InboundRateController struct {
	benchmark float64
	floor     float64
	ceiling   float64

	mu   sync.Mutex
	rate float64
	// threshold is the learned knee (TCP's ssthresh): growth is fast below
	// it and careful near or above it. It starts at the benchmark and is
	// reset just under the rate that last caused latency to degrade.
	threshold float64
	cooldown  int
	// congested is true from the first cut until latency is healthy again;
	// only that first cut sets the threshold, so a cascade of cuts while
	// admitted work drains doesn't drag the learned knee down with it.
	congested bool
	episode   int
	inserts   int
	latencies []time.Duration
	callers   map[string]time.Time
	baseline  time.Duration
	last      InboundRateSnapshot

	stop chan struct{}
	done chan struct{}
}

type InboundRateSnapshot struct {
	AdvertisedRPS  float64 `json:"advertised_rps"`
	PerCallerRPS   float64 `json:"per_caller_rps"`
	ActiveCallers  int     `json:"active_callers"`
	InsertsPerSec  int     `json:"inserts_per_sec"`
	P95InsertMs    float64 `json:"p95_insert_ms"`
	BaselineMs     float64 `json:"baseline_insert_ms"`
	BenchmarkRPS   float64 `json:"benchmark_rps"`
	ThresholdRPS   float64 `json:"threshold_rps"`
	Cuts           int64   `json:"cuts"`
	Raises         int64   `json:"raises"`
	LastAdjustment string  `json:"last_adjustment"`
}

const (
	inboundTick             = time.Second
	inboundCallerTTL        = 3 * time.Second
	inboundMinSamples       = 20
	inboundLatencySlack     = 2 * time.Millisecond
	inboundMaxSamples       = 8192
	inboundCut              = 0.7
	inboundFastGrowth       = 1.5
	inboundCarefulGrowth    = 1.05
	inboundCutCooldownTicks = 3
	// Latency lags a cut by however long admitted work takes to drain, so
	// within one congestion episode cut once, then hold, cutting again only
	// if the episode outlasts this many ticks.
	inboundEpisodeRecutTicks = 10
	inboundOvershootFactor   = 1.5
	inboundUsedFraction      = 0.8
)

// NewInboundRateController returns nil unless AQUIFER_INBOUND_RPS_ENABLED is
// true, so no loop runs and no header is sent by default.
func NewInboundRateController() *InboundRateController {
	if !envBool("AQUIFER_INBOUND_RPS_ENABLED", false) {
		return nil
	}
	benchmark := envFloat("AQUIFER_INBOUND_BENCHMARK_RPS", 400)
	startPercent := envFloat("AQUIFER_INBOUND_START_PERCENT", 40)
	c := newInboundRateController(benchmark, startPercent)
	go c.loop()
	return c
}

func newInboundRateController(benchmark, startPercent float64) *InboundRateController {
	if benchmark <= 0 {
		benchmark = 400
	}
	c := &InboundRateController{
		benchmark: benchmark,
		floor:     1,
		ceiling:   benchmark * 10,
		callers:   map[string]time.Time{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	c.rate = math.Max(benchmark*startPercent/100, c.floor)
	c.threshold = benchmark
	c.last = c.snapshotLocked(time.Now(), 0, 0, "start")
	return c
}

func (c *InboundRateController) Close() {
	if c == nil {
		return
	}
	close(c.stop)
	<-c.done
}

// Observe records one submission: who sent it, how long the durable insert
// (or duplicate check) took, and whether it actually inserted a new job.
func (c *InboundRateController) Observe(userID string, latency time.Duration, inserted bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if userID != "" {
		c.callers[userID] = time.Now()
	}
	if inserted {
		c.inserts++
	}
	if len(c.latencies) < inboundMaxSamples {
		c.latencies = append(c.latencies, latency)
	}
}

// AdvertisedRPS is the value for X-Aqueduct-Rps sent to userID.
func (c *InboundRateController) AdvertisedRPS(userID string) float64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if userID != "" {
		c.callers[userID] = time.Now()
	}
	return c.rate / float64(max(c.activeCallersLocked(time.Now()), 1))
}

func (c *InboundRateController) Snapshot() *InboundRateSnapshot {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := c.last
	return &snap
}

func (c *InboundRateController) loop() {
	defer close(c.done)
	ticker := time.NewTicker(inboundTick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.tick(time.Now())
		case <-c.stop:
			return
		}
	}
}

func (c *InboundRateController) tick(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	inserts := c.inserts
	samples := c.latencies
	c.inserts = 0
	c.latencies = nil

	p95 := percentile(samples, 0.95)
	enough := len(samples) >= inboundMinSamples
	if enough {
		switch {
		case c.baseline == 0 || p95 < c.baseline:
			c.baseline = p95
		default:
			// Drift up slowly so a stale best-case minimum (or slower
			// hardware than at startup) can't pin the controller low forever.
			c.baseline = time.Duration(float64(c.baseline)*0.99 + float64(p95)*0.01)
		}
	}

	healthy := !enough || p95 <= time.Duration(float64(c.baseline)*1.5)+inboundLatencySlack
	overshoot := float64(inserts) > c.rate*inboundOvershootFactor
	used := float64(inserts) >= c.rate*inboundUsedFraction

	adjustment := "hold"
	if healthy {
		c.congested = false
	}
	if c.cooldown > 0 {
		c.cooldown--
	}
	if c.congested {
		c.episode++
	}
	switch {
	case !healthy && (!c.congested || c.episode >= inboundEpisodeRecutTicks) && c.cooldown == 0:
		if !c.congested {
			c.threshold = math.Max(c.rate*0.9, c.floor)
			c.congested = true
		}
		c.episode = 0
		c.rate = math.Max(c.rate*inboundCut, c.floor)
		c.cooldown = inboundCutCooldownTicks
		adjustment = "cut: insert latency"
	case !healthy:
		adjustment = "hold: waiting for last cut"
	case overshoot:
		// Callers over their share are admission control's problem; cutting
		// here spirals to the floor whenever callers lag a rate change.
		adjustment = "hold: callers over advertised rate"
	case used:
		c.congested = false
		growth := inboundFastGrowth
		if c.rate >= c.threshold {
			growth = inboundCarefulGrowth
		}
		c.rate = math.Min(c.rate*growth, c.ceiling)
		adjustment = "raise"
	}

	c.last = c.snapshotLocked(now, inserts, p95, adjustment)
}

func (c *InboundRateController) snapshotLocked(now time.Time, inserts int, p95 time.Duration, adjustment string) InboundRateSnapshot {
	active := c.activeCallersLocked(now)
	snap := InboundRateSnapshot{
		AdvertisedRPS:  round2(c.rate),
		PerCallerRPS:   round2(c.rate / float64(max(active, 1))),
		ActiveCallers:  active,
		InsertsPerSec:  inserts,
		P95InsertMs:    round2(float64(p95) / float64(time.Millisecond)),
		BaselineMs:     round2(float64(c.baseline) / float64(time.Millisecond)),
		BenchmarkRPS:   c.benchmark,
		ThresholdRPS:   round2(c.threshold),
		Cuts:           c.last.Cuts,
		Raises:         c.last.Raises,
		LastAdjustment: adjustment,
	}
	switch {
	case strings.HasPrefix(adjustment, "cut"):
		snap.Cuts++
	case adjustment == "raise":
		snap.Raises++
	}
	return snap
}

func (c *InboundRateController) activeCallersLocked(now time.Time) int {
	active := 0
	for id, seen := range c.callers {
		if now.Sub(seen) > inboundCallerTTL {
			delete(c.callers, id)
			continue
		}
		active++
	}
	return active
}

func percentile(samples []time.Duration, q float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	return sorted[min(int(float64(len(sorted)-1)*q+0.5), len(sorted)-1)]
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func envFloat(name string, fallback float64) float64 {
	v, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
