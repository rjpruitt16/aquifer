// driver: the soak test's load generator, misbehaving upstream and webhook
// receiver, job ledger, chaos scheduler and observer, in one process. See
// benchmark/soak/README.md.
//
// Ports: :8080 (private) serves /work and /hook to Aquifer; :8081 (public)
// serves the read-only status pages.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func logf(format string, args ...any) { log.Printf(format, args...) }

var jobIDRe = regexp.MustCompile(`"job_id"\s*:\s*"([^"]+)"`)

type config struct {
	Target      string        `json:"target"`
	Supervisor  string        `json:"supervisor"`
	Self        string        `json:"self"`
	DataDir     string        `json:"-"`
	Rate        int           `json:"steady_rate"`
	BurstRate   int           `json:"burst_rate"`
	BurstEvery  time.Duration `json:"-"`
	BurstFor    time.Duration `json:"-"`
	RampHourUTC int           `json:"ramp_hour_utc"`
	RampSteps   []int         `json:"ramp_steps"`
	RampStep    time.Duration `json:"-"`
	KillMin     time.Duration `json:"-"`
	KillMax     time.Duration `json:"-"`
	RebootEvery time.Duration `json:"-"`
	Users       int           `json:"users"`
	Chaos       bool          `json:"chaos"`
}

// window counts one minute of activity.
type window struct {
	accepted, shed, clientErr, saturated atomic.Int64
	completed, failed, dups              atomic.Int64
	accept, e2e                          hist
}

type driver struct {
	cfg      config
	ledger   *ledger
	upstream *endpoint
	webhook  *endpoint
	events   *eventLog
	reports  *reports
	win      window
	client   *http.Client
	sem      chan struct{}

	rate   atomic.Int64 // jobs/s currently offered
	paused atomic.Bool  // set by POST /control/pause, e.g. around a target deploy
	phase  atomic.Value // "steady", "burst", "ramp"
	busy   sync.Mutex   // held by burst and ramp so they never overlap

	obs *observer
}

func main() {
	var cfg config
	var rampSteps string
	flag.StringVar(&cfg.Target, "target", "http://aquifer-soak.internal:8080", "Aquifer base URL")
	flag.StringVar(&cfg.Supervisor, "supervisor", "http://aquifer-soak.internal:9090", "supervisor control URL")
	flag.StringVar(&cfg.Self, "self", "http://aquifer-soak-driver.internal:8080", "base URL Aquifer uses to reach this driver")
	flag.StringVar(&cfg.DataDir, "data", "/data", "where the ledger, snapshots and events are kept")
	flag.IntVar(&cfg.Rate, "rate", 500, "steady jobs/s")
	flag.IntVar(&cfg.BurstRate, "burst-rate", 2000, "jobs/s during the hourly burst")
	flag.DurationVar(&cfg.BurstEvery, "burst-every", time.Hour, "time between bursts")
	flag.DurationVar(&cfg.BurstFor, "burst-for", time.Minute, "burst length")
	flag.IntVar(&cfg.RampHourUTC, "ramp-hour", 4, "UTC hour of the daily ceiling ramp (-1 disables)")
	flag.StringVar(&rampSteps, "ramp-steps", "500,750,1000,1250,1500,2000,2500,3000", "ramp steps, jobs/s")
	flag.DurationVar(&cfg.RampStep, "ramp-step", time.Minute, "length of each ramp step")
	flag.DurationVar(&cfg.KillMin, "kill-min", 3*time.Hour, "shortest time between kill -9s")
	flag.DurationVar(&cfg.KillMax, "kill-max", 5*time.Hour, "longest time between kill -9s")
	flag.DurationVar(&cfg.RebootEvery, "reboot-every", 36*time.Hour, "time between full machine restarts (0 disables)")
	flag.IntVar(&cfg.Users, "users", 50, "distinct user_ids")
	flag.BoolVar(&cfg.Chaos, "chaos", true, "misbehaving upstream/webhook receiver and kills")
	flag.Parse()
	for _, s := range strings.Split(rampSteps, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
			cfg.RampSteps = append(cfg.RampSteps, n)
		}
	}
	os.MkdirAll(cfg.DataDir, 0o755)

	d := &driver{
		cfg:      cfg,
		ledger:   openLedger(cfg.DataDir),
		upstream: newEndpoint("upstream", upstreamModes),
		webhook:  newEndpoint("webhook", webhookModes),
		events:   openEventLog(cfg.DataDir),
		reports:  openReports(cfg.DataDir),
		client: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
			MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, IdleConnTimeout: 60 * time.Second,
		}},
		sem: make(chan struct{}, 4096),
	}
	d.rate.Store(int64(cfg.Rate))
	d.phase.Store("steady")
	d.obs = newObserver(d)
	d.events.add("driver_started", map[string]any{"config": cfg})

	priv := http.NewServeMux()
	priv.HandleFunc("/.well-known/l8", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	priv.HandleFunc("/work", d.handleWork)
	priv.HandleFunc("/hook", d.handleHook)
	go func() { log.Fatal(http.ListenAndServe(":8080", priv)) }()

	pub := http.NewServeMux()
	d.obs.routes(pub)
	go func() { log.Fatal(http.ListenAndServe(":8081", pub)) }()

	stop := make(chan struct{})
	waitForTarget(cfg.Target)
	if cfg.Chaos {
		go d.upstream.schedule(10*time.Minute, d.events, stop)
		go d.webhook.schedule(10*time.Minute, d.events, stop)
		go d.killLoop(stop)
	}
	go d.burstLoop(stop)
	go d.rampLoop(stop)
	go d.checkLoop(stop)
	go d.obs.run(stop)
	go d.saveLoop(stop)
	go d.load(stop)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)
	if err := d.ledger.save(); err != nil {
		logf("%v", err)
	}
	d.events.add("driver_stopped", nil)
}

func waitForTarget(target string) {
	for {
		resp, err := http.Get(target + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		logf("waiting for %s: %v", target, err)
		time.Sleep(5 * time.Second)
	}
}

// load offers jobs open-loop at the current rate: arrivals don't wait for
// earlier requests, so a slow target sees the backlog a real client would
// create.
func (d *driver) load(stop <-chan struct{}) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	last := time.Now()
	var owed float64
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			if d.paused.Load() {
				last, owed = now, 0
				continue
			}
			owed += now.Sub(last).Seconds() * float64(d.rate.Load())
			last = now
			for ; owed >= 1; owed-- {
				select {
				case d.sem <- struct{}{}:
					go d.submit()
				default:
					d.win.saturated.Add(1) // the generator itself is the bottleneck
				}
			}
		}
	}
}

func (d *driver) submit() {
	defer func() { <-d.sem }()
	seq := d.ledger.nextSeq()
	body := fmt.Sprintf(`{"user_id":"soak-%d","idempotent_key":"soak-%d","url":"%s/work?s=%d","method":"POST","webhook_url":"%s/hook?s=%d"}`,
		seq%uint64(d.cfg.Users), seq, d.cfg.Self, seq, d.cfg.Self, seq)
	start := time.Now()
	resp, err := d.client.Post(d.cfg.Target+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		d.ledger.clientError(seq)
		d.win.clientErr.Add(1)
		return
	}
	rb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	now := time.Now()
	switch resp.StatusCode {
	case 201:
		m := jobIDRe.FindSubmatch(rb)
		if m == nil {
			d.ledger.clientError(seq)
			d.win.clientErr.Add(1)
			return
		}
		d.ledger.accepted(seq, string(m[1]), now)
		d.win.accepted.Add(1)
		d.win.accept.add(now.Sub(start))
	case 429, 503:
		d.ledger.shed()
		d.win.shed.Add(1)
	default:
		d.ledger.clientError(seq)
		d.win.clientErr.Add(1)
	}
}

// burstLoop offers burst-rate load for burst-for, every burst-every.
func (d *driver) burstLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(d.cfg.BurstEvery):
		}
		if d.paused.Load() || !d.busy.TryLock() {
			continue // paused, or a ramp is running
		}
		d.events.add("burst_start", map[string]any{"rate": d.cfg.BurstRate, "for_s": int(d.cfg.BurstFor.Seconds())})
		d.phase.Store("burst")
		d.rate.Store(int64(d.cfg.BurstRate))
		time.Sleep(d.cfg.BurstFor)
		d.rate.Store(int64(d.cfg.Rate))
		d.phase.Store("steady")
		d.events.add("burst_end", nil)
		d.busy.Unlock()
	}
}

type rampStep struct {
	Offered   int     `json:"offered"`
	Accepted  float64 `json:"accepted_per_s"`
	Shed      float64 `json:"shed_per_s"`
	Completed float64 `json:"completed_per_s"`
	OK        bool    `json:"sustained"`
}

// rampLoop runs once a day at ramp-hour UTC: holds the misbehaving endpoints
// healthy, steps the rate up, and records the highest rate Aquifer sustained.
// A falling ceiling over the week is drift that short benchmarks miss.
func (d *driver) rampLoop(stop <-chan struct{}) {
	if d.cfg.RampHourUTC < 0 {
		return
	}
	for {
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), d.cfg.RampHourUTC, 0, 0, 0, time.UTC)
		if !next.After(now) {
			next = next.Add(24 * time.Hour)
		}
		select {
		case <-stop:
			return
		case <-time.After(time.Until(next)):
		}
		d.busy.Lock()
		d.upstream.forced.Store(true)
		d.webhook.forced.Store(true)
		d.phase.Store("ramp")
		d.events.add("ramp_start", map[string]any{"steps": d.cfg.RampSteps})
		var steps []rampStep
		ceiling := 0
		for _, rate := range d.cfg.RampSteps {
			before := d.counts()
			d.rate.Store(int64(rate))
			time.Sleep(d.cfg.RampStep)
			after := d.counts()
			secs := d.cfg.RampStep.Seconds()
			st := rampStep{Offered: rate,
				Accepted:  float64(after[0]-before[0]) / secs,
				Shed:      float64(after[1]-before[1]) / secs,
				Completed: float64(after[2]-before[2]) / secs}
			st.OK = st.Accepted >= 0.97*float64(rate) && st.Completed >= 0.9*st.Accepted
			steps = append(steps, st)
			if !st.OK {
				break
			}
			ceiling = rate
		}
		d.rate.Store(int64(d.cfg.Rate))
		time.Sleep(2 * time.Minute) // let the backlog drain before chaos resumes
		d.upstream.forced.Store(false)
		d.webhook.forced.Store(false)
		d.phase.Store("steady")
		d.obs.recordRamp(ceiling, steps)
		d.events.add("ramp_end", map[string]any{"ceiling": ceiling, "steps": steps})
		d.busy.Unlock()
	}
}

// counts returns running totals of accepted, shed and completed jobs.
func (d *driver) counts() [3]uint64 {
	v := d.ledger.view()
	return [3]uint64{v.Counters.Accepted, v.Counters.Shed, v.Counters.Completed + v.Counters.Failed}
}

// killLoop sends kill -9 every kill-min..kill-max, and every reboot-every
// restarts the whole machine instead.
func (d *driver) killLoop(stop <-chan struct{}) {
	lastReboot := time.Now()
	for {
		wait := d.cfg.KillMin + time.Duration(rand.Int64N(int64(d.cfg.KillMax-d.cfg.KillMin)+1))
		select {
		case <-stop:
			return
		case <-time.After(wait):
		}
		d.busy.Lock() // never during a burst or ramp, so their numbers stay clean
		if d.paused.Load() {
			d.busy.Unlock()
			continue
		}
		action := "kill"
		if d.cfg.RebootEvery > 0 && time.Since(lastReboot) >= d.cfg.RebootEvery {
			action = "reboot"
			lastReboot = time.Now()
		}
		at := time.Now()
		resp, err := d.client.Post(d.cfg.Supervisor+"/"+action, "application/json", nil)
		if err == nil {
			resp.Body.Close()
		}
		if err != nil || resp.StatusCode != 200 {
			d.events.add(action+"_failed", map[string]any{"error": fmt.Sprint(err)})
		} else {
			d.ledger.recordRestart(at)
			d.events.add(action, map[string]any{"at_unix_ms": at.UnixMilli()})
		}
		time.Sleep(30 * time.Second)
		d.busy.Unlock()
	}
}

// checkLoop looks up jobs with no webhook after 5 minutes.
func (d *driver) checkLoop(stop <-chan struct{}) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		batch := d.ledger.stragglers(5*time.Minute, 5000)
		work := make(chan uint64)
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for seq := range work {
					d.ledger.resolve(seq, d.jobStatus(batch[seq].JobID), time.Now())
				}
			}()
		}
		for seq := range batch {
			work <- seq
		}
		close(work)
		wg.Wait()
	}
}

func (d *driver) jobStatus(id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.Target+"/jobs/"+id, nil)
	resp, err := d.client.Do(req)
	if err != nil {
		return "error"
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 404:
		return ""
	case 200:
		var b struct {
			Status string `json:"status"`
		}
		if json.NewDecoder(resp.Body).Decode(&b) != nil || b.Status == "" {
			return "error"
		}
		return b.Status
	default:
		return "error"
	}
}

func (d *driver) saveLoop(stop <-chan struct{}) {
	tick := time.NewTicker(5 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			if err := d.ledger.save(); err != nil {
				d.events.add("ledger_save_failed", map[string]any{"error": err.Error()})
			}
		}
	}
}

// eventLog keeps notable events (mode changes, kills, bursts, ramps) in
// memory for /status and appends them to events.jsonl on the volume.
type eventLog struct {
	mu   sync.Mutex
	f    *os.File
	tail []event
}

type event struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	Data any       `json:"data,omitempty"`
}

func openEventLog(dir string) *eventLog {
	f, err := os.OpenFile(dir+"/events.jsonl", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logf("events: %v", err)
	}
	return &eventLog{f: f}
}

func (e *eventLog) add(kind string, data any) {
	ev := event{At: time.Now().UTC(), Kind: kind, Data: data}
	line, _ := json.Marshal(ev)
	logf("event %s", line)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tail = append(e.tail, ev)
	if len(e.tail) > 500 {
		e.tail = e.tail[len(e.tail)-500:]
	}
	if e.f != nil {
		e.f.Write(append(line, '\n'))
	}
}

func (e *eventLog) recent(n int) []event {
	e.mu.Lock()
	defer e.mu.Unlock()
	if n > len(e.tail) {
		n = len(e.tail)
	}
	return append([]event(nil), e.tail[len(e.tail)-n:]...)
}

func fetchJSON(c *http.Client, url string) (map[string]any, error) {
	resp, err := c.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %d %s", url, resp.StatusCode, b)
	}
	return out, nil
}
