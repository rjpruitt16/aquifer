package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rjpruitt16/aquifer"
)

type workerBehavior string

const (
	behaviorStable     workerBehavior = "stable"
	behaviorDynamic    workerBehavior = "dynamic"
	behaviorFlapping   workerBehavior = "flapping"
	behaviorRecovering workerBehavior = "recovering"
	behaviorBad        workerBehavior = "bad"
	behaviorFragile    workerBehavior = "fragile"

	regularMaxRetries = 4
)

type workerState struct {
	id          int
	behavior    workerBehavior
	capacityRPS float64

	mu             sync.Mutex
	totalRequests  int
	lastSecond     int
	overloadScore  float64
	crashedUntil   int
	perSecond      map[int]int
	statusBySecond map[int]map[int]int
	advertisedRPS  map[int]float64
}

type scenarioState struct {
	start   time.Time
	workers []*workerState

	completed atomic.Int64
	failed    atomic.Int64
	rows      []scenarioRow
	rowsMu    sync.Mutex
}

type scenarioRow struct {
	Second      int
	Completed   int64
	Failed      int64
	WorkerID    int
	Requests    int
	HTTP200     int
	HTTP429     int
	HTTP500     int
	Advertised  float64
	Reputation  float64
	Overload    float64
	Crashed     bool
	Behavior    workerBehavior
	CapacityRPS float64
}

func main() {
	var (
		workers    = flag.Int("workers", 10, "number of logical workers to register")
		jobs       = flag.Int("jobs", 500, "number of jobs to enqueue")
		duration   = flag.Duration("duration", 30*time.Second, "how long to print observations")
		initialRPS = flag.Float64("rps", 50, "Aquifer configured dispatch ceiling")
		mode       = flag.String("mode", "aquifer", "mode: aquifer or regular")
		poolID     = flag.String("pool", "scenario-workers", "pool id")
		scenario   = flag.String("scenario", "mixed", "scenario: steady, weighted, flapping, backpressure, recovering, mixed, harsh")
		enqueueRPS = flag.Float64("enqueue-rps", 0, "optional job enqueue rate; 0 enqueues as fast as possible")
		heartbeat  = flag.Duration("heartbeat", 2*time.Second, "worker registration heartbeat interval; 0 disables heartbeat simulation")
		csvPath    = flag.String("csv", "", "optional CSV path for per-second worker observations")
		verbose    = flag.Bool("verbose", false, "show Aquifer retry/webhook logs")
	)
	flag.Parse()

	if *workers <= 0 {
		log.Fatal("workers must be > 0")
	}
	if *jobs <= 0 {
		log.Fatal("jobs must be > 0")
	}
	if !*verbose {
		log.SetOutput(io.Discard)
	}

	state := newScenarioState(*workers, *scenario)
	workerServer := httptest.NewServer(http.HandlerFunc(state.handleWorker))
	defer workerServer.Close()

	webhookServer := httptest.NewServer(http.HandlerFunc(state.handleWebhook))
	defer webhookServer.Close()

	var app *aquifer.Aquifer
	var cleanup func()
	switch *mode {
	case "aquifer":
		app, cleanup = startAquifer(*initialRPS, *workers, *poolID, workerServer.URL, state.workers, *heartbeat)
		defer cleanup()
	case "regular":
		// No Aquifer queue, no reputation, no dynamic pacing. This is a
		// simple round-robin load balancer model with retries on 5xx.
	default:
		log.Fatalf("unknown mode %q", *mode)
	}

	fmt.Printf("mode=%s scenario=%s workers=%d jobs=%d configured_rps=%.1f pool=%s\n", *mode, *scenario, *workers, *jobs, *initialRPS, *poolID)
	fmt.Printf("workers: %s\n", state.workerSummary())
	fmt.Println()

	done := make(chan struct{})
	go state.printLoop(app, *poolID, *duration, done)

	switch *mode {
	case "aquifer":
		enqueueJobs(app, *poolID, webhookServer.URL, *jobs, *enqueueRPS)
	case "regular":
		go state.runRegularLB(workerServer.URL, *jobs, *initialRPS, *duration)
	}
	<-done
	if *csvPath != "" {
		if err := state.writeCSV(*csvPath); err != nil {
			log.Fatalf("write csv: %v", err)
		}
		fmt.Printf("\nwrote csv=%s\n", *csvPath)
	}
	state.printSummary(*jobs)
}

func startAquifer(initialRPS float64, workers int, poolID, workerBaseURL string, workerStates []*workerState, heartbeat time.Duration) (*aquifer.Aquifer, func()) {
	dir, err := os.MkdirTemp("", "aquifer-scenario-*")
	if err != nil {
		log.Fatal(err)
	}

	cfg := &aquifer.Config{Defaults: aquifer.RateConfig{RPS: initialRPS, MaxConcurrent: workers}}
	runtime := aquifer.NewRuntime(aquifer.RuntimeOptions{
		DBPath: filepath.Join(dir, "aquifer.db"),
		Config: cfg,
		AdmissionLimits: &aquifer.AdmissionLimits{
			MaxBodyBytes: 0,
			DBMaxBytes:   0,
		},
	})

	for _, w := range workerStates {
		address := fmt.Sprintf("%s/worker/%d", workerBaseURL, w.id)
		if err := runtime.Aquifer.RegisterPoolMember(poolID, fmt.Sprintf("worker-%02d", w.id), address, w.capacityRPS, 30); err != nil {
			log.Fatal(err)
		}
	}

	stopHeartbeats := make(chan struct{})
	if heartbeat > 0 {
		go heartbeatLoop(runtime.Aquifer, poolID, workerBaseURL, workerStates, heartbeat, stopHeartbeats)
	}

	return runtime.Aquifer, func() {
		if heartbeat > 0 {
			close(stopHeartbeats)
		}
		runtime.Pools.Stop()
		runtime.Store.Close()
		os.RemoveAll(dir)
	}
}

func newScenarioState(count int, scenario string) *scenarioState {
	state := &scenarioState{start: time.Now(), workers: make([]*workerState, 0, count)}
	for i := 1; i <= count; i++ {
		behavior := behaviorStable
		capacity := 5.0

		switch scenario {
		case "steady":
			behavior = behaviorStable
		case "weighted":
			capacity = float64(1 + i%5)
		case "flapping":
			if i == count {
				behavior = behaviorFlapping
			}
		case "backpressure":
			if i%3 == 0 {
				behavior = behaviorDynamic
			}
		case "recovering":
			if i == count {
				behavior = behaviorRecovering
			}
		case "mixed":
			switch {
			case i == count:
				behavior = behaviorRecovering
			case i == count-1:
				behavior = behaviorFlapping
			case i%4 == 0:
				behavior = behaviorDynamic
			}
		case "harsh":
			switch {
			case i == count:
				behavior = behaviorRecovering
				capacity = 3
			case i == count-1:
				behavior = behaviorFlapping
				capacity = 3
			case i%3 == 0:
				behavior = behaviorFragile
				capacity = 3
			case i%4 == 0:
				behavior = behaviorDynamic
				capacity = 4
			default:
				capacity = 4
			}
		default:
			log.Fatalf("unknown scenario %q", scenario)
		}

		state.workers = append(state.workers, &workerState{
			id:             i,
			behavior:       behavior,
			capacityRPS:    capacity,
			perSecond:      make(map[int]int),
			statusBySecond: make(map[int]map[int]int),
			advertisedRPS:  make(map[int]float64),
		})
	}
	return state
}

func (s *scenarioState) handleWorker(w http.ResponseWriter, r *http.Request) {
	var id int
	if _, err := fmt.Sscanf(r.URL.Path, "/worker/%d", &id); err != nil {
		http.NotFound(w, r)
		return
	}
	if id < 1 || id > len(s.workers) {
		http.NotFound(w, r)
		return
	}

	worker := s.workers[id-1]
	second := int(time.Since(s.start).Seconds()) + 1
	status, advertisedRPS, delay := worker.respond(second)
	if delay > 0 {
		time.Sleep(delay)
	}

	if advertisedRPS > 0 {
		w.Header().Set("X-Aqueduct-Rps", fmt.Sprintf("%.2f", advertisedRPS))
	}
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"worker":%d,"behavior":%q,"second":%d}`, worker.id, worker.behavior, second)
}

func (w *workerState) respond(second int) (int, float64, time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if second != w.lastSecond {
		for missed := w.lastSecond + 1; missed < second; missed++ {
			w.decayOverload()
		}
		w.decayOverload()
		w.lastSecond = second
	}

	w.totalRequests++
	w.perSecond[second]++
	if w.statusBySecond[second] == nil {
		w.statusBySecond[second] = make(map[int]int)
	}

	status := http.StatusOK
	advertisedRPS := 0.0
	delay := time.Duration(0)
	countThisSecond := w.perSecond[second]
	if countThisSecond > int(w.capacityRPS) {
		w.overloadScore += float64(countThisSecond-int(w.capacityRPS)) * w.overloadSensitivity()
	}

	switch w.behavior {
	case behaviorStable:
		if countThisSecond > int(w.capacityRPS*1.4) {
			advertisedRPS = maxFloat(1, w.capacityRPS*0.7)
		}
	case behaviorDynamic:
		advertisedRPS = maxFloat(1, w.capacityRPS+float64(rand.Intn(5)-2))
		if countThisSecond > int(advertisedRPS*1.5) {
			status = http.StatusTooManyRequests
		}
	case behaviorFlapping:
		if second >= 5 && second <= 10 {
			status = http.StatusInternalServerError
			advertisedRPS = 1
		}
	case behaviorRecovering:
		if second <= 5 {
			status = http.StatusInternalServerError
			advertisedRPS = 1
		} else if second <= 10 {
			advertisedRPS = 2
		} else {
			advertisedRPS = w.capacityRPS
		}
	case behaviorBad:
		status = http.StatusInternalServerError
		advertisedRPS = 1
	case behaviorFragile:
		advertisedRPS = maxFloat(1, w.capacityRPS-w.overloadScore/2)
	}

	if w.crashedUntil >= second {
		status = http.StatusInternalServerError
		advertisedRPS = 1
		delay = maxDuration(delay, 250*time.Millisecond)
	}

	if w.overloadScore >= 8 {
		status = http.StatusInternalServerError
		advertisedRPS = 1
		w.crashedUntil = maxInt(w.crashedUntil, second+2)
		delay = maxDuration(delay, 250*time.Millisecond)
	} else if w.overloadScore >= 5 {
		status = http.StatusInternalServerError
		advertisedRPS = 1
		delay = maxDuration(delay, 150*time.Millisecond)
	} else if w.overloadScore >= 3 {
		advertisedRPS = maxFloat(1, w.capacityRPS*0.5)
		delay = maxDuration(delay, 100*time.Millisecond)
	}

	if countThisSecond > int(w.capacityRPS*2) {
		delay = maxDuration(delay, 75*time.Millisecond)
	}

	w.statusBySecond[second][status]++
	w.advertisedRPS[second] = advertisedRPS
	return status, advertisedRPS, delay
}

func (w *workerState) decayOverload() {
	if w.overloadScore <= 0 {
		w.overloadScore = 0
		return
	}
	w.overloadScore *= 0.65
	if w.overloadScore < 0.1 {
		w.overloadScore = 0
	}
}

func (w *workerState) overloadSensitivity() float64 {
	switch w.behavior {
	case behaviorFragile:
		return 1.4
	case behaviorDynamic:
		return 1.0
	case behaviorFlapping, behaviorRecovering:
		return 1.2
	default:
		return 0.7
	}
}

func (s *scenarioState) handleWebhook(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		switch payload["status"] {
		case "completed":
			s.completed.Add(1)
		case "failed":
			s.failed.Add(1)
		}
	}
	w.WriteHeader(http.StatusOK)
}

func enqueueJobs(app *aquifer.Aquifer, poolID, webhookURL string, jobs int, enqueueRPS float64) {
	var delay time.Duration
	if enqueueRPS > 0 {
		delay = time.Duration(float64(time.Second) / enqueueRPS)
	}

	for i := 0; i < jobs; i++ {
		_, err := app.Enqueue(aquifer.JobRequest{
			UserID:        "scenario-user",
			IdempotentKey: fmt.Sprintf("scenario-job-%d-%d", time.Now().UnixNano(), i),
			PoolID:        poolID,
			Method:        http.MethodPost,
			WebhookURL:    webhookURL,
		})
		if err != nil {
			log.Printf("enqueue %d: %v", i, err)
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func heartbeatLoop(app *aquifer.Aquifer, poolID, workerBaseURL string, workers []*workerState, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, w := range workers {
				address := fmt.Sprintf("%s/worker/%d", workerBaseURL, w.id)
				_ = app.RegisterPoolMember(poolID, fmt.Sprintf("worker-%02d", w.id), address, w.capacityRPS, int(interval.Seconds()))
			}
		case <-stop:
			return
		}
	}
}

func (s *scenarioState) runRegularLB(workerBaseURL string, jobs int, rps float64, duration time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	interval := time.Duration(float64(time.Second) / rps)
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var rr atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				if regularDispatch(ctx, workerBaseURL, len(s.workers), &rr) {
					s.completed.Add(1)
				} else {
					s.failed.Add(1)
				}
			}()
		}
	}
	wg.Wait()
}

func regularDispatch(ctx context.Context, workerBaseURL string, workers int, rr *atomic.Int64) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	for attempt := 0; attempt <= regularMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(time.Duration(1<<uint(attempt-1)) * time.Second):
			}
		}

		idx := int(rr.Add(1)-1)%workers + 1
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/worker/%d", workerBaseURL, idx), nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			continue
		}
		return true
	}
	return false
}

func (s *scenarioState) printLoop(app *aquifer.Aquifer, poolID string, duration time.Duration, done chan<- struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(done)

	printHeader(len(s.workers))
	stop := time.After(duration)
	for {
		select {
		case <-ticker.C:
			second := int(time.Since(s.start).Seconds())
			s.printSecond(app, poolID, second)
		case <-stop:
			return
		}
	}
}

func printHeader(workers int) {
	fmt.Printf("%4s %9s %6s %6s", "sec", "completed", "failed", "pool")
	for i := 1; i <= workers; i++ {
		fmt.Printf(" w%-2d", i)
	}
	fmt.Println()
}

func (s *scenarioState) printSecond(app *aquifer.Aquifer, poolID string, second int) {
	reputations := map[string]float64{}
	if app != nil {
		reputations = reputationByWorker(app.Health(), poolID)
	}
	fmt.Printf("%4d %9d %6d %6s", second, s.completed.Load(), s.failed.Load(), poolID)
	for _, w := range s.workers {
		reqs, statuses, advertised, overload, crashed := w.snapshot(second)
		rep := reputations[fmt.Sprintf("worker-%02d", w.id)]
		cell := fmt.Sprintf("%d", reqs)
		if statuses[500] > 0 {
			cell = fmt.Sprintf("%d!", reqs)
		} else if statuses[429] > 0 {
			cell = fmt.Sprintf("%d~", reqs)
		}
		if advertised > 0 {
			cell = fmt.Sprintf("%s/%.0f", cell, advertised)
		}
		if rep > 0 && rep < 1 {
			cell = fmt.Sprintf("%s/r%.2f", cell, rep)
		}
		if crashed {
			cell = fmt.Sprintf("%s/cr", cell)
		} else if overload >= 3 {
			cell = fmt.Sprintf("%s/o%.0f", cell, overload)
		}
		fmt.Printf(" %-9s", cell)
		s.recordRow(scenarioRow{
			Second:      second,
			Completed:   s.completed.Load(),
			Failed:      s.failed.Load(),
			WorkerID:    w.id,
			Requests:    reqs,
			HTTP200:     statuses[200],
			HTTP429:     statuses[429],
			HTTP500:     statuses[500],
			Advertised:  advertised,
			Reputation:  rep,
			Overload:    overload,
			Crashed:     crashed,
			Behavior:    w.behavior,
			CapacityRPS: w.capacityRPS,
		})
	}
	fmt.Println()
}

func (s *scenarioState) recordRow(row scenarioRow) {
	s.rowsMu.Lock()
	defer s.rowsMu.Unlock()
	s.rows = append(s.rows, row)
}

func (s *scenarioState) writeCSV(path string) error {
	s.rowsMu.Lock()
	defer s.rowsMu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "second,completed,failed,worker_id,requests,http_200,http_429,http_500,advertised_rps,reputation,overload,crashed,behavior,capacity_rps")
	for _, row := range s.rows {
		fmt.Fprintf(
			f,
			"%d,%d,%d,%d,%d,%d,%d,%d,%.2f,%.4f,%.2f,%t,%s,%.2f\n",
			row.Second,
			row.Completed,
			row.Failed,
			row.WorkerID,
			row.Requests,
			row.HTTP200,
			row.HTTP429,
			row.HTTP500,
			row.Advertised,
			row.Reputation,
			row.Overload,
			row.Crashed,
			row.Behavior,
			row.CapacityRPS,
		)
	}
	return nil
}

func (s *scenarioState) printSummary(jobs int) {
	s.rowsMu.Lock()
	defer s.rowsMu.Unlock()

	totalRequests := 0
	total200 := 0
	total429 := 0
	total500 := 0
	drainedAt := 0
	for _, row := range s.rows {
		totalRequests += row.Requests
		total200 += row.HTTP200
		total429 += row.HTTP429
		total500 += row.HTTP500
		if drainedAt == 0 && int(row.Completed+row.Failed) >= jobs {
			drainedAt = row.Second
		}
	}

	fmt.Printf(
		"\nsummary completed=%d failed=%d remaining=%d drained_at=%ds worker_requests=%d http_200=%d http_429=%d http_500=%d\n",
		s.completed.Load(),
		s.failed.Load(),
		jobs-int(s.completed.Load()+s.failed.Load()),
		drainedAt,
		totalRequests,
		total200,
		total429,
		total500,
	)
}

func (w *workerState) snapshot(second int) (int, map[int]int, float64, float64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	statuses := make(map[int]int, len(w.statusBySecond[second]))
	for k, v := range w.statusBySecond[second] {
		statuses[k] = v
	}
	return w.perSecond[second], statuses, w.advertisedRPS[second], w.overloadScore, w.crashedUntil >= second
}

func reputationByWorker(health map[string]any, poolID string) map[string]float64 {
	out := make(map[string]float64)
	pools, ok := health["pools"].(map[string]any)
	if !ok {
		return out
	}
	pool, ok := pools[poolID].(map[string]any)
	if !ok {
		return out
	}
	members, ok := pool["members"].([]map[string]any)
	if ok {
		for _, m := range members {
			id, _ := m["id"].(string)
			rep, _ := m["reputation"].(float64)
			out[id] = rep
		}
		return out
	}

	rawMembers, ok := pool["members"].([]any)
	if !ok {
		return out
	}
	for _, raw := range rawMembers {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["id"].(string)
		rep, _ := m["reputation"].(float64)
		out[id] = rep
	}
	return out
}

func (s *scenarioState) workerSummary() string {
	parts := make([]string, 0, len(s.workers))
	for _, w := range s.workers {
		parts = append(parts, fmt.Sprintf("w%d=%s/%.0frps", w.id, w.behavior, w.capacityRPS))
	}
	return fmt.Sprint(parts)
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
