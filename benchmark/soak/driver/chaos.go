package main

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// A misbehaving endpoint picks a new mode every slot (10 minutes). Weights
// are percentages. "down" lasts only part of the slot, then the endpoint
// recovers for the rest of it.
type mode struct {
	Name       string
	Weight     int
	SlowMin    time.Duration
	SlowMax    time.Duration
	ErrPct     int // share of requests answered with ErrStatus
	ErrStatus  int
	RetryAfter int // seconds, sent with 429/503 when > 0
	DownMin    time.Duration
	DownMax    time.Duration
}

var upstreamModes = []mode{
	{Name: "healthy", Weight: 70},
	{Name: "slow", Weight: 10, SlowMin: 500 * time.Millisecond, SlowMax: 3 * time.Second},
	{Name: "errors_503", Weight: 8, ErrPct: 30, ErrStatus: 503},
	{Name: "rate_limited_429", Weight: 7, ErrPct: 30, ErrStatus: 429, RetryAfter: 3},
	{Name: "down", Weight: 5, ErrPct: 100, ErrStatus: 503, DownMin: 2 * time.Minute, DownMax: 5 * time.Minute},
}

var webhookModes = []mode{
	{Name: "healthy", Weight: 85},
	{Name: "slow", Weight: 8, SlowMin: 200 * time.Millisecond, SlowMax: 2 * time.Second},
	{Name: "errors_503", Weight: 5, ErrPct: 20, ErrStatus: 503},
	{Name: "down", Weight: 2, ErrPct: 100, ErrStatus: 503, DownMin: time.Minute, DownMax: 3 * time.Minute},
}

type endpoint struct {
	name     string
	modes    []mode
	cur      atomic.Pointer[mode]
	downTill atomic.Int64 // unix ms; while now < downTill the mode applies, after it the endpoint is healthy
	forced   atomic.Bool  // held healthy (during the daily ramp)
	hits     atomic.Int64
	errs     atomic.Int64

	mu         sync.Mutex
	downMillis int64 // total time spent down, for the report
}

func newEndpoint(name string, modes []mode) *endpoint {
	e := &endpoint{name: name, modes: modes}
	e.cur.Store(&modes[0])
	return e
}

func (e *endpoint) pick() *mode {
	n := rand.IntN(100)
	for i := range e.modes {
		if n < e.modes[i].Weight {
			return &e.modes[i]
		}
		n -= e.modes[i].Weight
	}
	return &e.modes[0]
}

// active returns the mode in force right now.
func (e *endpoint) active() *mode {
	if e.forced.Load() {
		return &e.modes[0]
	}
	m := e.cur.Load()
	if m.DownMax > 0 && time.Now().UnixMilli() >= e.downTill.Load() {
		return &e.modes[0]
	}
	return m
}

func (e *endpoint) modeName() string {
	if e.forced.Load() {
		return "healthy (held for ramp)"
	}
	return e.active().Name
}

// schedule switches modes every slot until stop closes.
func (e *endpoint) schedule(slot time.Duration, events *eventLog, stop <-chan struct{}) {
	for {
		m := e.pick()
		if m.DownMax > 0 {
			d := m.DownMin + time.Duration(rand.Int64N(int64(m.DownMax-m.DownMin)+1))
			e.downTill.Store(time.Now().Add(d).UnixMilli())
			e.mu.Lock()
			e.downMillis += d.Milliseconds()
			e.mu.Unlock()
			events.add(e.name+"_mode", map[string]any{"mode": m.Name, "for_s": int(d.Seconds())})
		} else if m.Name != e.cur.Load().Name {
			events.add(e.name+"_mode", map[string]any{"mode": m.Name})
		}
		e.cur.Store(m)
		select {
		case <-time.After(slot):
		case <-stop:
			return
		}
	}
}

// misbehave applies the current mode to one request. It returns true when it
// already answered (with an error status) and the caller should stop.
func (e *endpoint) misbehave(w http.ResponseWriter) bool {
	e.hits.Add(1)
	m := e.active()
	if m.SlowMax > 0 {
		time.Sleep(m.SlowMin + time.Duration(rand.Int64N(int64(m.SlowMax-m.SlowMin)+1)))
	}
	if m.ErrPct > 0 && rand.IntN(100) < m.ErrPct {
		e.errs.Add(1)
		if m.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(m.RetryAfter))
		}
		w.WriteHeader(m.ErrStatus)
		return true
	}
	return false
}

func (e *endpoint) downSeconds() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.downMillis / 1000
}

// Both endpoints answer with generous pacing headers so the soak measures
// Aquifer, not a self-imposed rate limit.
func paceHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Aqueduct-Rps", "100000")
	w.Header().Set("X-Aqueduct-Max-Concurrent", "1024")
}

func (d *driver) handleWork(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body)
	paceHeaders(w)
	if d.upstream.misbehave(w) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}

func (d *driver) handleHook(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	paceHeaders(w)
	if d.webhook.misbehave(w) {
		return
	}
	seq, err := strconv.ParseUint(r.URL.Query().Get("s"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	var payload struct {
		Status string `json:"status"`
	}
	json.Unmarshal(body, &payload)
	e2e, hasE2E, dup := d.ledger.webhook(seq, payload.Status, time.Now())
	switch {
	case dup:
		d.win.dups.Add(1)
	case payload.Status == "failed":
		d.win.failed.Add(1)
	default:
		d.win.completed.Add(1)
	}
	if hasE2E {
		d.win.e2e.add(e2e)
	}
	w.WriteHeader(200)
}
