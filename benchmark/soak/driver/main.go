// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

// driver: the soak test's load generator, misbehaving target API and webhook
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
	Target       string        `json:"target"`
	Supervisor   string        `json:"supervisor"`
	Self         string        `json:"self"`
	DataDir      string        `json:"-"`
	Rate         int           `json:"steady_rate"`
	BurstRate    int           `json:"burst_rate"`
	BurstEvery   time.Duration `json:"-"`
	BurstFor     time.Duration `json:"-"`
	RampHourUTC  int           `json:"ramp_hour_utc"`
	RampSteps    []int         `json:"ramp_steps"`
	RampStep     time.Duration `json:"-"`
	KillMin      time.Duration `json:"-"`
	KillMax      time.Duration `json:"-"`
	RebootEvery  time.Duration `json:"-"`
	Users        int           `json:"users"`
	Chaos        bool          `json:"chaos"`
	PeakHourUTC  int           `json:"combined_peak_hour_utc"`
	Phases       string        `json:"storm_phases,omitempty"`
	Receivers    int           `json:"storm_receivers,omitempty"`
	BodyKB       int           `json:"storm_body_kb,omitempty"`
	NoWebhook    bool          `json:"storm_no_webhook,omitempty"`
	Nodes        string        `json:"cluster_nodes,omitempty"`
	Scenarios    string        `json:"cluster_scenarios,omitempty"`
	Valkey       bool          `json:"cluster_valkey,omitempty"`
	Backends     string        `json:"gateway_backends,omitempty"`
	WebhookChaos bool          `json:"webhook_chaos"`
	Name         string        `json:"name"`
	WS           wsConfig      `json:"websocket"`
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

	runID      string // prefixes WebSocket session IDs, new per driver process
	wsUpstream *endpoint
	storm      *storm   // nil outside a storm test, see storm.go
	cluster    *cluster // nil outside a cluster failure test, see cluster.go
	wsLedger   *wsLedger
	wsWin      wsWindow
	wsWanted   atomic.Int64 // sessions wanted right now
	wsPhase    atomic.Value // "steady", "burst", "ramp"
	wsBusy     sync.Mutex   // held by WebSocket bursts and ramps
	wsSessions sync.Map     // index -> *wsSession

	obs *observer

	// Why submissions counted as client errors failed, by kind, so a
	// spike can be told apart: the driver's own connection failures, a
	// status it doesn't expect, or an answer without a job id.
	errorKinds   map[string]uint64
	errorKindsMu sync.Mutex
}

func (d *driver) clientError(seq uint64, kind string) {
	d.ledger.clientError(seq)
	d.win.clientErr.Add(1)
	d.errorKindsMu.Lock()
	if d.errorKinds == nil {
		d.errorKinds = map[string]uint64{}
	}
	// Bounded, whatever errors turn up: past maxErrorKinds, new kinds are
	// counted together.
	if _, seen := d.errorKinds[kind]; !seen && len(d.errorKinds) >= maxErrorKinds {
		kind = "other"
	}
	d.errorKinds[kind]++
	d.errorKindsMu.Unlock()
}

func (d *driver) clientErrorKinds() map[string]uint64 {
	d.errorKindsMu.Lock()
	defer d.errorKindsMu.Unlock()
	out := make(map[string]uint64, len(d.errorKinds))
	for k, v := range d.errorKinds {
		out[k] = v
	}
	return out
}

const maxErrorKinds = 50

// errorKind is a transport error reduced to a countable kind: addresses,
// quoted URLs and other numbers (stream ids, byte counts) left out.
var (
	addrRe   = regexp.MustCompile(`\[?[0-9a-f:.]+\]?:\d+|"[^"]*"`)
	numberRe = regexp.MustCompile(`\d+`)
)

func errorKind(err error) string {
	kind := numberRe.ReplaceAllString(addrRe.ReplaceAllString(err.Error(), "ADDR"), "N")
	if len(kind) > 120 {
		kind = kind[:120]
	}
	return "transport: " + kind
}

// envOr lets the deploy config set a flag's default, so one image serves
// both the Aquifer and the Spillway soak.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	var cfg config
	var rampSteps, wsRampSteps string
	flag.StringVar(&cfg.Name, "name", envOr("SOAK_NAME", "Aquifer"), "name of the system under test, for the status page")
	flag.StringVar(&cfg.Target, "target", envOr("SOAK_TARGET", "http://aquifer-soak.internal:8080"), "target base URL")
	flag.StringVar(&cfg.Supervisor, "supervisor", envOr("SOAK_SUPERVISOR", "http://aquifer-soak.internal:9090"), "supervisor control URL")
	flag.StringVar(&cfg.Self, "self", envOr("SOAK_SELF", "http://aquifer-soak-driver.internal:8080"), "base URL the target uses to reach this driver")
	flag.StringVar(&cfg.DataDir, "data", envOr("SOAK_DATA", "/data"), "where the ledger, snapshots and events are kept")
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
	flag.IntVar(&cfg.PeakHourUTC, "peak-hour", 16, "UTC hour (at :15) of the daily combined HTTP and WebSocket peak (-1 disables)")
	flag.BoolVar(&cfg.WS.Enabled, "ws", envOr("SOAK_WS", "true") == "true", "WebSocket sessions through the target's /websocket")
	flag.IntVar(&cfg.WS.Sessions, "ws-sessions", 200, "steady WebSocket sessions")
	flag.IntVar(&cfg.WS.BurstSessions, "ws-burst-sessions", 1000, "WebSocket sessions during the half-hourly burst")
	flag.DurationVar(&cfg.WS.CmdEvery, "ws-cmd-every", time.Second, "time between commands on each session")
	flag.IntVar(&cfg.WS.Events, "ws-events", 3, "events the backend sends per command")
	flag.StringVar(&wsRampSteps, "ws-ramp-steps", "200,400,800,1200,1600,2000,2500", "WebSocket ramp steps, sessions")
	flag.StringVar(&cfg.Phases, "phases", "", "storm test: offered rates in order, e.g. 15m@500,2h@1500,45m@300, replacing the daily bursts and ramps; load stops after the last")
	flag.IntVar(&cfg.Receivers, "receivers", 0, "storm test: spread jobs over this many customer endpoints, one port each from 9000")
	flag.IntVar(&cfg.BodyKB, "body-kb", 0, "storm test: send a random 0..N KB request body with each job")
	flag.BoolVar(&cfg.NoWebhook, "no-webhook", false, "storm test: jobs carry no webhook_url; a job is done when its receiver first answers 2xx, or when Aquifer reports it failed (needs -receivers)")
	flag.StringVar(&cfg.Nodes, "nodes", envOr("SOAK_NODES", ""), "cluster test: id=target|supervisor,... spread load over these Aquifer nodes instead of -target")
	flag.StringVar(&cfg.Scenarios, "scenarios", "", "cluster test: failures in order, after:action[:nodes][:for], e.g. 30m:kill:a,25m:hold:b:5m,25m:freeze:c:20s,25m:valkey:5m,20m:end")
	flag.StringVar(&cfg.Backends, "backends", envOr("SOAK_BACKENDS", ""), "gateway mode: id=target|supervisor,... the instances behind a gateway (Canalis) named by -nodes. Load goes only to -nodes; job status is checked here; scenarios can name these too")
	flag.BoolVar(&cfg.WebhookChaos, "webhook-chaos", true, "let the webhook receiver misbehave with -chaos (false keeps it healthy, so every job's webhook is the record of it finishing)")
	flag.BoolVar(&cfg.Valkey, "valkey", false, "cluster test: run the nodes' Valkey here (valkey-server on :6379) so a scenario can take it down")
	flag.Parse()
	if cfg.Backends != "" && cfg.Nodes == "" {
		log.Fatal("-backends (SOAK_BACKENDS) needs -nodes (SOAK_NODES): the gateway nodes load goes to")
	}
	var nodes, backends []*clusterNode
	var plan []scenario
	if cfg.Nodes != "" {
		var err error
		if nodes, err = parseNodes(cfg.Nodes); err != nil {
			log.Fatal(err)
		}
		if cfg.Backends != "" {
			if backends, err = parseBackends(cfg.Backends); err != nil {
				log.Fatal(err)
			}
		}
		if plan, err = parseScenarios(cfg.Scenarios, append(append([]*clusterNode{}, nodes...), backends...)); err != nil {
			log.Fatal(err)
		}
		// Status pages and per-minute target stats follow the first node.
		cfg.Target, cfg.Supervisor = nodes[0].Target, nodes[0].Supervisor
	}
	var phases []stormPhase
	if cfg.Phases != "" {
		var err error
		if phases, err = parsePhases(cfg.Phases); err != nil {
			log.Fatal(err)
		}
	}
	cfg.WS.CmdEveryMS = cfg.WS.CmdEvery.Milliseconds()
	for _, s := range strings.Split(wsRampSteps, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
			cfg.WS.RampSteps = append(cfg.WS.RampSteps, n)
		}
	}
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
		sem:        make(chan struct{}, 4096),
		runID:      fmt.Sprintf("r%d", time.Now().Unix()),
		wsUpstream: newEndpoint("ws_upstream", wsUpstreamModes),
	}
	if cfg.NoWebhook && cfg.Receivers <= 0 {
		log.Fatal("-no-webhook needs -receivers: the receivers are what record delivery")
	}
	if len(phases) > 0 || cfg.Receivers > 0 || cfg.BodyKB > 0 {
		d.storm = newStorm(cfg, phases)
	}
	d.ledger.failedIsTerminal = cfg.NoWebhook
	if len(nodes) > 0 {
		d.cluster = newCluster(nodes, plan, cfg.Valkey)
		d.cluster.addBackends(backends)
		if d.cluster.valkey != nil {
			if err := d.cluster.valkey.start(); err != nil {
				log.Fatalf("valkey: %v", err)
			}
		}
	}
	var discardedWS *WSCounters
	d.wsLedger, discardedWS = openWSLedger(cfg.DataDir)
	d.rate.Store(int64(cfg.Rate))
	d.phase.Store("steady")
	d.wsPhase.Store("steady")
	if cfg.WS.Enabled {
		d.wsWanted.Store(int64(cfg.WS.Sessions))
	}
	d.obs = newObserver(d)
	d.events.add("driver_started", map[string]any{"config": cfg})
	if discardedWS != nil {
		d.events.add("ws_ledger_reset", map[string]any{"version": wsLedgerVersion, "discarded": discardedWS})
	}

	priv := http.NewServeMux()
	priv.HandleFunc("/.well-known/l8", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) })
	priv.HandleFunc("/work", d.handleWork)
	priv.HandleFunc("/hook", d.handleHook)
	priv.HandleFunc("/socket", d.handleWSUpstream)
	go func() { log.Fatal(http.ListenAndServe(":8080", priv)) }()
	if d.storm != nil && d.storm.receivers > 0 {
		d.serveReceivers()
	}

	pub := http.NewServeMux()
	d.obs.routes(pub)
	go func() { log.Fatal(http.ListenAndServe(":8081", pub)) }()

	stop := make(chan struct{})
	if d.cluster != nil {
		for _, n := range d.cluster.nodes {
			waitForTarget(n.Target)
		}
	} else {
		waitForTarget(cfg.Target)
	}
	if cfg.Chaos {
		// A storm's chaos is its receivers (and the kill); the callback
		// Aquifer reports results to stays healthy so it doesn't blur them.
		if d.storm == nil {
			go d.upstream.schedule(10*time.Minute, d.events, stop)
			if cfg.WebhookChaos {
				go d.webhook.schedule(10*time.Minute, d.events, stop)
			}
		}
		if cfg.WS.Enabled {
			go d.wsUpstream.schedule(10*time.Minute, d.events, stop)
		}
		go d.killLoop(stop)
	}
	if cfg.WS.Enabled {
		go d.wsManager(stop)
		go d.wsExpireLoop(stop)
		go d.wsBurstLoop(stop)
		go d.peakLoop(stop)
	}
	if d.cluster != nil && len(d.cluster.plan) > 0 {
		go d.runScenarios(stop)
	}
	if d.storm != nil && len(d.storm.phases) > 0 {
		go d.runPhases(stop)
	} else {
		go d.burstLoop(stop)
		go d.rampLoop(stop)
	}
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
	if err := d.wsLedger.save(); err != nil {
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
	webhook := fmt.Sprintf(`,"webhook_url":"%s/hook?s=%d"`, d.cfg.Self, seq)
	if d.cfg.NoWebhook {
		webhook = ""
	}
	body := fmt.Sprintf(`{"user_id":"soak-%d","idempotent_key":"soak-%d","url":"%s","method":"POST"%s%s}`,
		seq%uint64(d.cfg.Users), seq, d.storm.jobURL(d.cfg.Self, seq), webhook, d.storm.body())
	start := time.Now()
	status, owner, rb, err := d.post(body)
	if err != nil {
		d.clientError(seq, errorKind(err))
		return
	}
	now := time.Now()
	if id, ok := lineAccept(status, rb); ok && d.cluster.isGateway() {
		d.ledger.acceptedOn(seq, id, lineNode, now)
		d.cluster.lined.Add(1)
		d.win.accepted.Add(1)
		if outcome := lineOutcome(rb); outcome != "" {
			// It finished while we listened: no webhook will follow.
			switch {
			case d.ledger.finishedOnStream(seq, outcome, now):
				d.win.dups.Add(1)
			case outcome == "failed":
				d.win.failed.Add(1)
			default:
				d.win.completed.Add(1)
			}
			d.win.e2e.add(now.Sub(start))
			return
		}
		d.win.accept.add(now.Sub(start))
		return
	}
	// 200 is the existing job for a key already accepted: in a cluster
	// test, the answer to a 504 retried with the same key.
	if status == 200 && d.cluster == nil {
		status = -1
	}
	switch status {
	case 200, 201:
		m := jobIDRe.FindSubmatch(rb)
		if m == nil {
			kind := fmt.Sprintf("status %d without job_id", status)
			if bytes.Contains(rb, []byte("canalis_timeout")) {
				kind += " (canalis_timeout)"
			}
			d.clientError(seq, kind)
			return
		}
		d.ledger.acceptedOn(seq, string(m[1]), owner, now)
		d.win.accepted.Add(1)
		d.win.accept.add(now.Sub(start))
	case 429, 503:
		d.ledger.shed()
		d.win.shed.Add(1)
	default:
		d.clientError(seq, fmt.Sprintf("status %d", status))
	}
}

// burstLoop offers burst-rate load for burst-for at the top of every hour
// (every burst-every when that isn't an hour, as in local runs).
func (d *driver) burstLoop(stop <-chan struct{}) {
	for {
		wait := d.cfg.BurstEvery
		if wait == time.Hour {
			wait = untilNext(0)
		}
		select {
		case <-stop:
			return
		case <-time.After(wait):
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
// healthy, steps the load up, and records the highest step the target
// sustained. With WebSockets on, odd days of the year ramp WebSocket sessions
// instead of HTTP jobs. A falling ceiling over the week is drift that short
// benchmarks miss.
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
		d.wsBusy.Lock()
		d.upstream.forced.Store(true)
		d.webhook.forced.Store(true)
		d.wsUpstream.forced.Store(true)
		if d.cfg.WS.Enabled && next.YearDay()%2 == 1 {
			d.events.add("ws_ramp_start", map[string]any{"steps_sessions": d.cfg.WS.RampSteps})
			ceiling, steps := d.wsRamp()
			time.Sleep(2 * time.Minute)
			d.upstream.forced.Store(false)
			d.webhook.forced.Store(false)
			d.wsUpstream.forced.Store(false)
			d.obs.recordRamp("websocket_sessions", ceiling, steps)
			d.events.add("ws_ramp_end", map[string]any{"ceiling_sessions": ceiling, "steps": steps})
			d.wsBusy.Unlock()
			d.busy.Unlock()
			continue
		}
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
		d.wsUpstream.forced.Store(false)
		d.phase.Store("steady")
		d.obs.recordRamp("http_jobs_per_s", ceiling, steps)
		d.events.add("ramp_end", map[string]any{"ceiling": ceiling, "steps": steps})
		d.wsBusy.Unlock()
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
		d.wsBusy.Lock()
		if d.paused.Load() {
			d.wsBusy.Unlock()
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
			d.ledger.recordRestart(restartTime(action, at))
			d.events.add(action, map[string]any{"at_unix_ms": at.UnixMilli()})
		}
		time.Sleep(30 * time.Second)
		d.wsBusy.Unlock()
		d.busy.Unlock()
	}
}

// rebootKillDelay matches the supervisor's handleReboot, which answers
// /reboot and only then, 200ms later, kills Aquifer and exits. A kill is
// sent before /kill answers, so only a reboot needs the offset.
const rebootKillDelay = 200 * time.Millisecond

// restartTime is when the target actually went down for a kill or reboot
// requested at `at`. The ledger excuses jobs accepted just before that
// moment as flush-window losses, so recording a reboot at the request time
// counted jobs accepted in its last 200ms as lost (aquifer#67).
func restartTime(action string, at time.Time) time.Time {
	if action == "reboot" {
		return at.Add(rebootKillDelay)
	}
	return at
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
					status := d.jobStatus(seq, batch[seq])
					if d.ledger.resolve(seq, status, time.Now()) == "failed" && d.storm != nil {
						d.storm.recordFailed(seq)
					}
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

// post sends one submission: to -target, or to a random node in a cluster
// test. owner is the node that kept the job (X-Aquifer-Cluster-Owner).
func (d *driver) post(body string) (status int, owner string, rb []byte, err error) {
	if d.cluster != nil {
		return d.clusterPost(body)
	}
	resp, err := d.client.Post(d.cfg.Target+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, "", nil, err
	}
	rb, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, "", rb, nil
}

// jobStatus asks the node that kept the job. Any other node would 404.
func (d *driver) jobStatus(seq uint64, p pending) string {
	if d.cluster.isGateway() {
		return d.gatewayJobStatus(seq, p)
	}
	target := d.cfg.Target
	if d.cluster != nil {
		target = d.cluster.targetFor(p.Node)
	}
	return d.jobStatusAt(target, p.JobID)
}

func (d *driver) jobStatusAt(target, id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, target+"/jobs/"+id, nil)
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
			if err := d.wsLedger.save(); err != nil {
				d.events.add("ws_ledger_save_failed", map[string]any{"error": err.Error()})
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
