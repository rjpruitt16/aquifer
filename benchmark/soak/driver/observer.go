// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// minute is one minute of observations: the driver's own counts plus the
// target's process, store and queue numbers.
type minute struct {
	At           time.Time      `json:"at"`
	Phase        string         `json:"phase"`
	Offered      int64          `json:"offered_per_s"`
	Accepted     float64        `json:"accepted_per_s"`
	Shed         float64        `json:"shed_per_s"`
	ClientErrors float64        `json:"client_errors_per_s"`
	Saturated    float64        `json:"generator_saturated_per_s,omitempty"`
	Completed    float64        `json:"completed_per_s"`
	Failed       float64        `json:"failed_per_s"`
	Duplicates   float64        `json:"duplicates_per_s"`
	AcceptP50    float64        `json:"accept_p50_ms"`
	AcceptP99    float64        `json:"accept_p99_ms"`
	E2EP50       float64        `json:"e2e_p50_ms"`
	E2EP99       float64        `json:"e2e_p99_ms"`
	Outstanding  int            `json:"outstanding"`
	UpstreamMode string         `json:"upstream_mode"`
	WebhookMode  string         `json:"webhook_mode"`
	Target       map[string]any `json:"target"`
	TargetError  string         `json:"target_error,omitempty"`
	QueueBacklog float64        `json:"queue_backlog"`
	ActiveQueues float64        `json:"active_queues"`
	WS           *wsMinute      `json:"websocket,omitempty"`
	// Nodes is each cluster or gateway node's supervisor stats (rss_mb,
	// open_fds, store_mb, restarts), so a leak on one isn't hidden by
	// Target following only the first.
	Nodes       map[string]nodeStats `json:"nodes,omitempty"`
	accept, e2e histCounts
	wsCmd       histCounts
}

type wsMinute struct {
	Phase        string  `json:"phase"`
	Wanted       int64   `json:"sessions_wanted"`
	Open         int64   `json:"sessions_open"`
	Sent         float64 `json:"commands_sent_per_s"`
	Completed    float64 `json:"commands_completed_per_s"`
	Duplicates   float64 `json:"duplicate_events_per_s"`
	Disconnects  float64 `json:"disconnects_per_s"`
	Rejected     float64 `json:"handshakes_failed_per_s"`
	CmdP50       float64 `json:"command_p50_ms"`
	CmdP99       float64 `json:"command_p99_ms"`
	UpstreamMode string  `json:"upstream_mode"`
}

// hour summarizes 60 minutes; these are what the monitoring agent compares
// across the week.
type hour struct {
	At              time.Time           `json:"at"`
	Minutes         int                 `json:"minutes"`
	Accepted        float64             `json:"accepted_per_s"`
	Shed            float64             `json:"shed_per_s"`
	ClientErrors    float64             `json:"client_errors_per_s"`
	Completed       float64             `json:"completed_per_s"`
	Failed          float64             `json:"failed_per_s"`
	AcceptP50       float64             `json:"accept_p50_ms"`
	AcceptP99       float64             `json:"accept_p99_ms"`
	E2EP50          float64             `json:"e2e_p50_ms"`
	E2EP99          float64             `json:"e2e_p99_ms"`
	MaxOutstanding  int                 `json:"max_outstanding"`
	MaxQueueBacklog float64             `json:"max_queue_backlog"`
	RSSMB           float64             `json:"rss_mb_end"`
	RSSMBMax        float64             `json:"rss_mb_max"`
	HeapMB          float64             `json:"heap_inuse_mb_end"`
	Goroutines      float64             `json:"goroutines_end"`
	GoroutinesMax   float64             `json:"goroutines_max"`
	OpenFDs         float64             `json:"open_fds_end"`
	PebbleMB        float64             `json:"pebble_mb_end"`
	PebbleFiles     float64             `json:"pebble_files_end"`
	VolumeFreePct   float64             `json:"volume_free_pct_end"`
	TargetRestarts  float64             `json:"target_restarts_total"`
	TargetDownMins  int                 `json:"minutes_target_unreachable"`
	Ledger          Counters            `json:"ledger_totals"`
	UpstreamDownS   int64               `json:"upstream_down_s_total"`
	WebhookDownS    int64               `json:"webhook_down_s_total"`
	WS              *wsHour             `json:"websocket,omitempty"`
	Nodes           map[string]nodeHour `json:"nodes,omitempty"`
}

type nodeStats struct {
	RSSMB    float64 `json:"rss_mb"`
	OpenFDs  float64 `json:"open_fds"`
	StoreMB  float64 `json:"store_mb"`
	Restarts float64 `json:"restarts"`
	Down     bool    `json:"supervisor_unreachable,omitempty"`
}

// nodeHour is one node's hour: end-of-hour values (for day-over-day
// comparison at the same hour) and the hour's peaks.
type nodeHour struct {
	RSSMBEnd   float64 `json:"rss_mb_end"`
	RSSMBMax   float64 `json:"rss_mb_max"`
	OpenFDsEnd float64 `json:"open_fds_end"`
	OpenFDsMax float64 `json:"open_fds_max"`
	StoreMBEnd float64 `json:"store_mb_end"`
	Restarts   float64 `json:"restarts_total"`
	DownMins   int     `json:"minutes_supervisor_unreachable,omitempty"`
	// DownAtEnd: the hour's last sample couldn't reach the node, so the
	// _end values are from earlier in the hour.
	DownAtEnd bool `json:"down_at_end,omitempty"`
}

type wsHour struct {
	MaxOpen     int64      `json:"max_sessions_open"`
	Sent        float64    `json:"commands_sent_per_s"`
	Completed   float64    `json:"commands_completed_per_s"`
	Disconnects float64    `json:"disconnects_per_s"`
	CmdP50      float64    `json:"command_p50_ms"`
	CmdP99      float64    `json:"command_p99_ms"`
	DownS       int64      `json:"upstream_down_s_total"`
	Ledger      WSCounters `json:"ledger_totals"`
}

type day struct {
	Date           string     `json:"date"`
	Hours          int        `json:"hours"`
	RSSMBFirst     float64    `json:"rss_mb_first"`
	RSSMBLast      float64    `json:"rss_mb_last"`
	RSSMBMax       float64    `json:"rss_mb_max"`
	GoroutinesLast float64    `json:"goroutines_last"`
	GoroutinesMax  float64    `json:"goroutines_max"`
	PebbleMBFirst  float64    `json:"pebble_mb_first"`
	PebbleMBLast   float64    `json:"pebble_mb_last"`
	E2EP99Median   float64    `json:"e2e_p99_ms_median_of_hours"`
	AcceptP99Med   float64    `json:"accept_p99_ms_median_of_hours"`
	Accepted       uint64     `json:"accepted"`
	Lost           uint64     `json:"lost"`
	LostFlush      uint64     `json:"lost_within_flush_window"`
	Duplicates     uint64     `json:"duplicates"`
	NoWebhook      uint64     `json:"finished_without_webhook"`
	Ceiling        int        `json:"ramp_ceiling_per_s,omitempty"`
	RampKind       string     `json:"ramp_kind,omitempty"`
	RampSteps      []rampStep `json:"ramp_steps,omitempty"`
	WSCompleted    uint64     `json:"ws_commands_completed,omitempty"`
	WSLost         uint64     `json:"ws_lost_commands,omitempty"`
	WSDupEvents    uint64     `json:"ws_duplicate_events,omitempty"`
}

type observer struct {
	d       *driver
	mu      sync.Mutex
	minutes []minute
	hours   []hour
	days    []day
	ramps   map[string]ramp
	dir     string
}

type ramp struct {
	Kind    string
	Ceiling int
	Steps   []rampStep
}

func newObserver(d *driver) *observer {
	o := &observer{d: d, dir: d.cfg.DataDir, ramps: map[string]ramp{}}
	readJSONL(filepath.Join(o.dir, "hourly.jsonl"), func(b []byte) {
		var h hour
		if json.Unmarshal(b, &h) == nil {
			o.hours = append(o.hours, h)
		}
	})
	readJSONL(filepath.Join(o.dir, "daily.jsonl"), func(b []byte) {
		var dd day
		if json.Unmarshal(b, &dd) == nil {
			o.days = append(o.days, dd)
		}
	})
	return o
}

func readJSONL(path string, fn func([]byte)) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		fn(sc.Bytes())
	}
}

func appendJSONL(path string, v any) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		logf("%s: %v", path, err)
		return
	}
	defer f.Close()
	b, _ := json.Marshal(v)
	f.Write(append(b, '\n'))
}

func (o *observer) recordRamp(kind string, ceiling int, steps []rampStep) {
	o.mu.Lock()
	o.ramps[time.Now().UTC().Format("2006-01-02")] = ramp{kind, ceiling, steps}
	o.mu.Unlock()
}

func (o *observer) run(stop <-chan struct{}) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	last := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			o.observe(now, now.Sub(last).Seconds())
			last = now
		}
	}
}

func (o *observer) observe(now time.Time, secs float64) {
	d := o.d
	w := &d.win
	m := minute{At: now.UTC().Truncate(time.Second), Phase: d.phase.Load().(string), Offered: d.rate.Load(),
		Accepted:     float64(w.accepted.Swap(0)) / secs,
		Shed:         float64(w.shed.Swap(0)) / secs,
		ClientErrors: float64(w.clientErr.Swap(0)) / secs,
		Saturated:    float64(w.saturated.Swap(0)) / secs,
		Completed:    float64(w.completed.Swap(0)) / secs,
		Failed:       float64(w.failed.Swap(0)) / secs,
		Duplicates:   float64(w.dups.Swap(0)) / secs,
		accept:       w.accept.take(),
		e2e:          w.e2e.take(),
		UpstreamMode: d.upstream.modeName(),
		WebhookMode:  d.webhook.modeName(),
	}
	m.AcceptP50, m.AcceptP99 = m.accept.ms(0.5), m.accept.ms(0.99)
	m.E2EP50, m.E2EP99 = m.e2e.ms(0.5), m.e2e.ms(0.99)
	m.Outstanding = d.ledger.view().Outstanding
	if d.cfg.WS.Enabled {
		ww := &d.wsWin
		m.wsCmd = ww.cmd.take()
		m.WS = &wsMinute{Phase: d.wsPhase.Load().(string), Wanted: d.wsWanted.Load(), Open: d.wsLedger.open.Load(),
			Sent:         float64(ww.sent.Swap(0)) / secs,
			Completed:    float64(ww.completed.Swap(0)) / secs,
			Duplicates:   float64(ww.dups.Swap(0)) / secs,
			Disconnects:  float64(ww.disconnects.Swap(0)) / secs,
			Rejected:     float64(ww.rejected.Swap(0)) / secs,
			UpstreamMode: d.wsUpstream.modeName()}
		m.WS.CmdP50, m.WS.CmdP99 = m.wsCmd.ms(0.5), m.wsCmd.ms(0.99)
	}

	c := &http.Client{Timeout: 15 * time.Second}
	if st, err := fetchJSON(c, d.cfg.Supervisor+"/stats"); err == nil {
		m.Target = st
	} else {
		m.TargetError = "supervisor: " + err.Error()
	}
	if d.cluster != nil {
		m.Nodes = nodeSnapshots(d.cluster.allNodes())
	}
	if h, err := fetchJSON(c, d.cfg.Target+"/health"); err == nil {
		if q, ok := h["queues"].(map[string]any); ok {
			m.QueueBacklog, _ = q["backlog"].(float64)
			m.ActiveQueues, _ = q["active"].(float64)
		} else if line, ok := h["line"].(map[string]any); ok {
			// Canalis: its line is the backlog that matters.
			m.QueueBacklog, _ = line["jobs"].(float64)
		}
	} else if m.TargetError == "" {
		m.TargetError = "aquifer: " + err.Error()
	}

	o.mu.Lock()
	o.minutes = append(o.minutes, m)
	if len(o.minutes) > 180 {
		o.minutes = o.minutes[len(o.minutes)-180:]
	}
	var done *hour
	if len(o.minutes) >= 2 {
		prev := o.minutes[len(o.minutes)-2].At
		if prev.Truncate(time.Hour) != m.At.Truncate(time.Hour) {
			h := o.summarizeHour(prev.Truncate(time.Hour))
			o.hours = append(o.hours, h)
			done = &h
		}
	}
	o.mu.Unlock()

	if done != nil {
		appendJSONL(filepath.Join(o.dir, "hourly.jsonl"), done)
		if done.At.Hour() == 23 {
			o.closeDay(done.At.Format("2006-01-02"))
		}
	}
}

func num(m map[string]any, k string) float64 {
	v, _ := m[k].(float64)
	return v
}

// summarizeHour must be called with o.mu held.
func (o *observer) summarizeHour(start time.Time) hour {
	h := hour{At: start}
	var acc, e2e, wsCmd histCounts
	var ws wsHour
	wsMinutes := 0
	for _, m := range o.minutes {
		if m.At.Truncate(time.Hour) != start {
			continue
		}
		h.Minutes++
		if m.WS != nil {
			wsMinutes++
			ws.MaxOpen = max(ws.MaxOpen, m.WS.Open)
			ws.Sent += m.WS.Sent
			ws.Completed += m.WS.Completed
			ws.Disconnects += m.WS.Disconnects
			wsCmd.merge(m.wsCmd)
		}
		h.Accepted += m.Accepted
		h.Shed += m.Shed
		h.ClientErrors += m.ClientErrors
		h.Completed += m.Completed
		h.Failed += m.Failed
		acc.merge(m.accept)
		e2e.merge(m.e2e)
		h.MaxOutstanding = max(h.MaxOutstanding, m.Outstanding)
		h.MaxQueueBacklog = max(h.MaxQueueBacklog, m.QueueBacklog)
		if m.TargetError != "" {
			h.TargetDownMins++
		}
		if t := m.Target; t != nil {
			h.RSSMB = num(t, "rss_bytes") / (1 << 20)
			h.RSSMBMax = max(h.RSSMBMax, h.RSSMB)
			h.HeapMB = num(t, "go_heapinuse") / (1 << 20)
			h.Goroutines = num(t, "goroutines")
			h.GoroutinesMax = max(h.GoroutinesMax, h.Goroutines)
			h.OpenFDs = num(t, "open_fds")
			h.PebbleMB = num(t, "pebble_bytes") / (1 << 20)
			h.PebbleFiles = num(t, "pebble_files")
			if total := num(t, "volume_total_bytes"); total > 0 {
				h.VolumeFreePct = 100 * num(t, "volume_free_bytes") / total
			}
			h.TargetRestarts = num(t, "restarts")
		}
		for id, s := range m.Nodes {
			if h.Nodes == nil {
				h.Nodes = map[string]nodeHour{}
			}
			nh := h.Nodes[id]
			nh.DownAtEnd = s.Down
			if s.Down {
				nh.DownMins++
			} else {
				nh.RSSMBEnd, nh.OpenFDsEnd, nh.StoreMBEnd, nh.Restarts = s.RSSMB, s.OpenFDs, s.StoreMB, s.Restarts
				nh.RSSMBMax = max(nh.RSSMBMax, s.RSSMB)
				nh.OpenFDsMax = max(nh.OpenFDsMax, s.OpenFDs)
			}
			h.Nodes[id] = nh
		}
	}
	if h.Minutes > 0 {
		n := float64(h.Minutes)
		h.Accepted, h.Shed, h.ClientErrors, h.Completed, h.Failed = h.Accepted/n, h.Shed/n, h.ClientErrors/n, h.Completed/n, h.Failed/n
	}
	h.AcceptP50, h.AcceptP99 = acc.ms(0.5), acc.ms(0.99)
	h.E2EP50, h.E2EP99 = e2e.ms(0.5), e2e.ms(0.99)
	h.Ledger = o.d.ledger.view().Counters
	h.UpstreamDownS = o.d.upstream.downSeconds()
	h.WebhookDownS = o.d.webhook.downSeconds()
	if wsMinutes > 0 {
		n := float64(wsMinutes)
		ws.Sent, ws.Completed, ws.Disconnects = ws.Sent/n, ws.Completed/n, ws.Disconnects/n
		ws.CmdP50, ws.CmdP99 = wsCmd.ms(0.5), wsCmd.ms(0.99)
		ws.DownS = o.d.wsUpstream.downSeconds()
		ws.Ledger = o.d.wsLedger.view()
		h.WS = &ws
	}
	return h
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	return v[len(v)/2]
}

func (o *observer) closeDay(date string) {
	o.mu.Lock()
	var hs []hour
	for _, h := range o.hours {
		if h.At.Format("2006-01-02") == date {
			hs = append(hs, h)
		}
	}
	// Totals are cumulative; the day's share is the change since the last
	// hour before it.
	var base Counters
	var wsBase WSCounters
	for _, h := range o.hours {
		if h.At.Format("2006-01-02") < date {
			base = h.Ledger
			if h.WS != nil {
				wsBase = h.WS.Ledger
			}
		}
	}
	dd := day{Date: date, Hours: len(hs)}
	if len(hs) > 0 {
		first, last := hs[0], hs[len(hs)-1]
		dd.RSSMBFirst, dd.RSSMBLast = first.RSSMB, last.RSSMB
		dd.PebbleMBFirst, dd.PebbleMBLast = first.PebbleMB, last.PebbleMB
		dd.GoroutinesLast = last.Goroutines
		var e2e, acc []float64
		for _, h := range hs {
			dd.RSSMBMax = max(dd.RSSMBMax, h.RSSMBMax)
			dd.GoroutinesMax = max(dd.GoroutinesMax, h.GoroutinesMax)
			e2e = append(e2e, h.E2EP99)
			acc = append(acc, h.AcceptP99)
		}
		dd.E2EP99Median, dd.AcceptP99Med = median(e2e), median(acc)
		c := last.Ledger
		dd.Accepted = c.Accepted - base.Accepted
		dd.Lost = c.Lost - base.Lost
		dd.LostFlush = c.LostInFlushWindow - base.LostInFlushWindow
		dd.Duplicates = c.Duplicates - base.Duplicates
		dd.NoWebhook = c.TerminalNoWebhook - base.TerminalNoWebhook
		if last.WS != nil {
			w := last.WS.Ledger
			dd.WSCompleted = w.Completed - wsBase.Completed
			dd.WSLost = w.Lost - wsBase.Lost
			dd.WSDupEvents = w.DupEvents - wsBase.DupEvents
		}
	}
	if r, ok := o.ramps[date]; ok {
		dd.Ceiling, dd.RampKind, dd.RampSteps = r.Ceiling, r.Kind, r.Steps
	}
	o.days = append(o.days, dd)
	o.mu.Unlock()
	appendJSONL(filepath.Join(o.dir, "daily.jsonl"), dd)
}

// alerts are the problems the observer can judge from one look. Trends
// (slow growth, drift) are left to whoever compares the hourly history.
func (o *observer) alerts(v ledgerView, last *minute) []string {
	var out []string
	c := v.Counters
	if c.Lost > 0 {
		out = append(out, fmt.Sprintf("%d accepted jobs lost (not explained by the 100ms flush window); see recent_lost", c.Lost))
	}
	if c.LostAfterInstanceRestart > 0 {
		out = append(out, fmt.Sprintf("%d jobs the instance acknowledged were gone after it restarted (its flush window: see its aquifer_wal_sync_lag_seconds)", c.LostAfterInstanceRestart))
	}
	if c.Stuck > 0 {
		out = append(out, fmt.Sprintf("%d jobs still unfinished after 24h", c.Stuck))
	}
	if c.UnknownWebhooks > 0 {
		out = append(out, fmt.Sprintf("%d webhooks for jobs the driver never submitted", c.UnknownWebhooks))
	}
	if o.d.cfg.WS.Enabled {
		w := o.d.wsLedger.view()
		if w.Lost > 0 {
			out = append(out, fmt.Sprintf("%d WebSocket commands recorded by the target but not completed within 5 minutes (%d of their messages arrived later)", w.Lost, w.AfterExpiry))
		}
		if w.Unknown > 0 {
			out = append(out, fmt.Sprintf("%d WebSocket messages for commands this driver never sent", w.Unknown))
		}
	}
	if v.OldestAgeS > 3600 {
		out = append(out, fmt.Sprintf("oldest unfinished job is %dm old", v.OldestAgeS/60))
	}
	if last != nil {
		if last.TargetError != "" {
			out = append(out, "target unreachable in the last minute: "+last.TargetError)
		}
		if last.Saturated > 0 {
			out = append(out, "load generator saturated: the driver, not Aquifer, limited the offered rate")
		}
		if t := last.Target; t != nil {
			if total := num(t, "volume_total_bytes"); total > 0 && num(t, "volume_free_bytes")/total < 0.2 {
				out = append(out, "target volume under 20% free")
			}
		}
	}
	return out
}

func (o *observer) routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /status", o.handleStatus)
	mux.HandleFunc("GET /status/hourly", o.handleHourly)
	mux.HandleFunc("GET /status/minutes", o.handleMinutes)
	mux.HandleFunc("GET /status/daily", o.handleDaily)
	mux.HandleFunc("GET /events", o.handleEvents)
	mux.HandleFunc("GET /lost", o.handleLost)
	mux.HandleFunc("GET /logs", o.handleLogs)
	mux.HandleFunc("POST /control/{action}", o.handleControl)
	mux.HandleFunc("GET /reports", o.d.reports.handleList)
	mux.HandleFunc("POST /reports", o.d.reports.handlePost)
	mux.HandleFunc("GET /{$}", o.handleIndex)
}

// lostReaders lets one GET /lost run at a time.
var lostReaders = make(chan struct{}, 1)

// handleLost serves every lost job as JSON lines, not just recent_lost.
// The page is public, so it streams the file (memory stays flat however
// long it grows), under a write deadline, one reader at a time.
func (o *observer) handleLost(w http.ResponseWriter, r *http.Request) {
	select {
	case lostReaders <- struct{}{}:
		defer func() { <-lostReaders }()
	default:
		http.Error(w, "busy; try again", http.StatusServiceUnavailable)
		return
	}
	http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
	w.Header().Set("Content-Type", "application/x-ndjson")
	f, err := os.Open(o.d.ledger.lostF)
	if err != nil {
		return // none lost yet
	}
	defer f.Close()
	io.Copy(w, f)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func queryInt(r *http.Request, k string, def, maxV int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(k))
	if err != nil || n <= 0 {
		return def
	}
	return min(n, maxV)
}

func (o *observer) handleStatus(w http.ResponseWriter, r *http.Request) {
	v := o.d.ledger.view()
	o.mu.Lock()
	var last *minute
	if n := len(o.minutes); n > 0 {
		m := o.minutes[n-1]
		last = &m
	}
	var lastHour *hour
	if n := len(o.hours); n > 0 {
		h := o.hours[n-1]
		lastHour = &h
	}
	days := append([]day(nil), o.days...)
	o.mu.Unlock()
	writeJSON(w, map[string]any{
		"now":                time.Now().UTC(),
		"run_started":        v.RunStarted,
		"running_for_h":      int(time.Since(v.RunStarted).Hours()),
		"alerts":             o.alerts(v, last),
		"phase":              o.d.phase.Load(),
		"paused":             o.d.paused.Load(),
		"offered_per_s":      o.d.rate.Load(),
		"upstream_mode":      o.d.upstream.modeName(),
		"webhook_mode":       o.d.webhook.modeName(),
		"ledger":             v,
		"websocket":          o.wsStatus(),
		"storm":              o.d.storm.status(),
		"cluster":            o.d.cluster.status(o.d),
		"last_minute":        last,
		"last_hour":          lastHour,
		"daily":              days,
		"recent_events":      o.d.events.recent(30),
		"client_error_kinds": o.d.clientErrorKinds(),
		"open_files":         openFiles(),
		"latest_report":      o.d.reports.latest(),
		"config":             o.d.cfg,
		"more": map[string]string{
			"hourly":  "/status/hourly?n=168",
			"minutes": "/status/minutes?n=180",
			"daily":   "/status/daily",
			"events":  "/events?n=200",
			"logs":    "/logs?n=500&grep=",
			"reports": "/reports?n=20",
		},
	})
}

// handleControl pauses or resumes the load (token required), e.g. to let
// in-flight jobs finish before redeploying the target, so a deploy's
// restart doesn't look like lost jobs in the ledger.
func (o *observer) handleControl(w http.ResponseWriter, r *http.Request) {
	if !o.d.reports.authorized(w, r) {
		return
	}
	switch r.PathValue("action") {
	case "pause":
		o.d.paused.Store(true)
		o.d.events.add("load_paused", nil)
	case "resume":
		o.d.paused.Store(false)
		o.d.events.add("load_resumed", nil)
	default:
		http.Error(w, "action must be pause or resume", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"paused": o.d.paused.Load(), "outstanding": o.d.ledger.view().Outstanding})
}

func (o *observer) handleHourly(w http.ResponseWriter, r *http.Request) {
	n := queryInt(r, "n", 168, 10000)
	o.mu.Lock()
	hs := o.hours[max(0, len(o.hours)-n):]
	out := append([]hour(nil), hs...)
	o.mu.Unlock()
	writeJSON(w, out)
}

func (o *observer) handleMinutes(w http.ResponseWriter, r *http.Request) {
	n := queryInt(r, "n", 60, 180)
	o.mu.Lock()
	out := append([]minute(nil), o.minutes[max(0, len(o.minutes)-n):]...)
	o.mu.Unlock()
	writeJSON(w, out)
}

func (o *observer) handleDaily(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	out := append([]day(nil), o.days...)
	o.mu.Unlock()
	writeJSON(w, out)
}

func (o *observer) handleEvents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, o.d.events.recent(queryInt(r, "n", 100, 500)))
}

// handleLogs relays the target's recent log lines from the supervisor, so a
// monitor can dig into a problem without Fly credentials.
func (o *observer) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	u := fmt.Sprintf("%s/logs?n=%d&grep=%s", o.d.cfg.Supervisor, queryInt(r, "n", 500, 5000), url.QueryEscape(q.Get("grep")))
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Get(u)
	if err != nil {
		http.Error(w, "supervisor unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.Copy(w, resp.Body)
}

func (o *observer) handleIndex(w http.ResponseWriter, r *http.Request) {
	v := o.d.ledger.view()
	o.mu.Lock()
	var last *minute
	if n := len(o.minutes); n > 0 {
		m := o.minutes[n-1]
		last = &m
	}
	o.mu.Unlock()
	c := v.Counters
	var b strings.Builder
	fmt.Fprintf(&b, "%s soak test, running %s (since %s)\n\n", o.d.cfg.Name, time.Since(v.RunStarted).Round(time.Minute), v.RunStarted.Format(time.RFC3339))
	if a := o.alerts(v, last); len(a) > 0 {
		b.WriteString("ALERTS\n")
		for _, s := range a {
			b.WriteString("  - " + s + "\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("No alerts.\n\n")
	}
	fmt.Fprintf(&b, "accepted %d  shed %d  client errors %d\n", c.Accepted, c.Shed, c.ClientErrors)
	fmt.Fprintf(&b, "webhooks: completed %d  failed %d  duplicates %d\n", c.Completed, c.Failed, c.Duplicates)
	fmt.Fprintf(&b, "finished without webhook %d  lost %d  lost in flush window %d  stuck %d\n", c.TerminalNoWebhook, c.Lost, c.LostInFlushWindow, c.Stuck)
	fmt.Fprintf(&b, "outstanding %d (oldest %ds)  kills/restarts caused %d\n", v.Outstanding, v.OldestAgeS, v.Restarts)
	if last != nil {
		fmt.Fprintf(&b, "\nlast minute (%s): offered %d/s, accepted %.0f/s, completed %.0f/s, shed %.1f/s, accept p99 %.1fms, e2e p99 %.0fms\n",
			last.Phase, last.Offered, last.Accepted, last.Completed, last.Shed, last.AcceptP99, last.E2EP99)
		fmt.Fprintf(&b, "upstream %s, webhook receiver %s\n", last.UpstreamMode, last.WebhookMode)
		if ws := last.WS; ws != nil {
			w := o.d.wsLedger.view()
			fmt.Fprintf(&b, "\nwebsocket (%s): %d/%d sessions open, %.0f commands/s sent, %.0f/s completed, command p99 %.0fms, backend %s\n",
				ws.Phase, ws.Open, ws.Wanted, ws.Sent, ws.Completed, ws.CmdP99, ws.UpstreamMode)
			fmt.Fprintf(&b, "websocket totals: completed %d  lost %d  unrecorded %d  resent %d  duplicate events %d (%d after a resend)  out of order %d  replay gaps %d  disconnects %d\n",
				w.Completed, w.Lost, w.Unrecorded, w.Resent, w.DupEvents, w.DupAfterResend, w.OutOfOrder, w.ReplayGaps, w.Disconnects)
		}
		if t := last.Target; t != nil {
			fmt.Fprintf(&b, "target: rss %.0fMB, heap %.0fMB, goroutines %.0f, fds %.0f, pebble %.0fMB in %.0f files, restarts %.0f\n",
				num(t, "rss_bytes")/(1<<20), num(t, "go_heapinuse")/(1<<20), num(t, "goroutines"), num(t, "open_fds"),
				num(t, "pebble_bytes")/(1<<20), num(t, "pebble_files"), num(t, "restarts"))
		}
	}
	if rep := o.d.reports.latest(); rep != nil {
		fmt.Fprintf(&b, "\nlatest monitor report (%s, %s): %s\n", rep.At.Format(time.RFC3339), rep.Severity, rep.Summary)
	}
	b.WriteString("\nJSON: /status  /status/hourly  /status/minutes  /status/daily  /events  /logs  /reports\n")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, b.String())
}

func (o *observer) wsStatus() any {
	if !o.d.cfg.WS.Enabled {
		return nil
	}
	return map[string]any{
		"phase":           o.d.wsPhase.Load(),
		"paused":          o.d.paused.Load(),
		"sessions_wanted": o.d.wsWanted.Load(),
		"sessions_open":   o.d.wsLedger.open.Load(),
		"backend_mode":    o.d.wsUpstream.modeName(),
		"ledger":          o.d.wsLedger.view(),
	}
}

// nodeSnapshots reads every node's supervisor stats at once, so one stalled
// node can't hold up the minute's sample: a node that doesn't answer within
// the deadline is marked down.
func nodeSnapshots(nodes []*clusterNode) map[string]nodeStats {
	c := &http.Client{Timeout: 5 * time.Second}
	out := make(map[string]nodeStats, len(nodes))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *clusterNode) {
			defer wg.Done()
			s := nodeStats{Down: true}
			if st, err := fetchJSON(c, n.Supervisor+"/stats"); err == nil {
				s = nodeStats{
					RSSMB:    num(st, "rss_bytes") / (1 << 20),
					OpenFDs:  num(st, "open_fds"),
					StoreMB:  num(st, "pebble_bytes") / (1 << 20),
					Restarts: num(st, "restarts"),
				}
			}
			mu.Lock()
			out[n.ID] = s
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return out
}
