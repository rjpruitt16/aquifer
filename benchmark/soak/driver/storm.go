// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// The storm test: a fixed schedule of offered rates (-phases) instead of the
// soak's daily cycle, jobs spread over many customer endpoints (-receivers)
// that each misbehave in a fixed way for the whole run, and request bodies
// of random size (-body-kb). All three are off unless set, so the soak runs
// exactly as before.
//
// Each receiver listens on its own port. Aquifer queues and paces by
// scheme://host:port, so to Aquifer every port is a separate destination,
// the way thousands of merchant endpoints are to a webhook sender. Job
// results still come back on the main /hook, the sender's own callback.

type stormPhase struct {
	For  time.Duration `json:"for"`
	Rate int           `json:"rate"`
}

func parsePhases(s string) ([]stormPhase, error) {
	var phases []stormPhase
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dur, rate, ok := strings.Cut(part, "@")
		if !ok {
			return nil, fmt.Errorf("phase %q: want duration@rate, e.g. 2h@1500", part)
		}
		d, err := time.ParseDuration(dur)
		if err != nil {
			return nil, fmt.Errorf("phase %q: %v", part, err)
		}
		r, err := strconv.Atoi(rate)
		if err != nil {
			return nil, fmt.Errorf("phase %q: %v", part, err)
		}
		phases = append(phases, stormPhase{For: d, Rate: r})
	}
	return phases, nil
}

// Receiver classes, by receiver index modulo 20: 1 dead, 2 flaky, 3 slow,
// 14 healthy (5% / 10% / 15% / 70%).
const (
	classHealthy = iota
	classSlow
	classFlaky
	classDead
	numClasses
)

var classNames = [numClasses]string{"healthy", "slow", "flaky", "dead"}

func receiverClass(i int) int {
	switch r := i % 20; {
	case r == 0:
		return classDead
	case r <= 2:
		return classFlaky
	case r <= 5:
		return classSlow
	default:
		return classHealthy
	}
}

const receiverBasePort = 9000

// classStats is one receiver class within one phase.
type classStats struct {
	hits, errs, completed, failed atomic.Int64
	e2e                           hist
}

type storm struct {
	phases    []stormPhase
	receivers int
	bodyKB    int
	host      string // the driver's host as Aquifer reaches it

	started atomic.Int64 // unix ms the schedule began, 0 before
	cur     atomic.Int64 // index of the current phase; len(phases) once done
	stats   [][numClasses]classStats
	filler  string
}

func newStorm(cfg config, phases []stormPhase) *storm {
	s := &storm{phases: phases, receivers: cfg.Receivers, bodyKB: cfg.BodyKB}
	// One slot per phase plus one for after the schedule ends.
	s.stats = make([][numClasses]classStats, len(phases)+1)
	if u, err := url.Parse(cfg.Self); err == nil {
		s.host = u.Hostname()
	}
	if s.bodyKB > 0 {
		s.filler = strings.Repeat("x", s.bodyKB*1024)
	}
	return s
}

func (s *storm) phaseIndex() int {
	if s == nil {
		return 0
	}
	return int(s.cur.Load())
}

// jobURL is where a job goes: a receiver's port when there are receivers,
// otherwise the driver's own /work.
func (s *storm) jobURL(self string, seq uint64) string {
	if s == nil || s.receivers <= 0 {
		return fmt.Sprintf("%s/work?s=%d", self, seq)
	}
	port := receiverBasePort + int(seq%uint64(s.receivers))
	return fmt.Sprintf("http://%s/work?s=%d", net.JoinHostPort(s.host, strconv.Itoa(port)), seq)
}

func (s *storm) classOfSeq(seq uint64) int {
	if s == nil || s.receivers <= 0 {
		return -1
	}
	return receiverClass(int(seq % uint64(s.receivers)))
}

// body is a JSON string field with a random 0..bodyKB KB payload, or "".
func (s *storm) body() string {
	if s == nil || s.bodyKB <= 0 {
		return ""
	}
	return `,"body":"` + s.filler[:rand.IntN(len(s.filler)+1)] + `"`
}

// recordHook counts a job's webhook against its receiver's class.
func (s *storm) recordHook(seq uint64, status string, e2e time.Duration, hasE2E, dup bool) {
	c := s.classOfSeq(seq)
	if c < 0 || dup {
		return
	}
	st := &s.stats[s.phaseIndex()][c]
	if status == "failed" {
		st.failed.Add(1)
	} else {
		st.completed.Add(1)
	}
	if hasE2E {
		st.e2e.add(e2e)
	}
}

// runPhases steps through the schedule, then stops offering load. The
// generator keeps running, so the driver keeps observing the drain.
func (d *driver) runPhases(stop <-chan struct{}) {
	s := d.storm
	s.started.Store(time.Now().UnixMilli())
	for i, p := range s.phases {
		s.cur.Store(int64(i))
		d.rate.Store(int64(p.Rate))
		d.phase.Store(fmt.Sprintf("storm_%d", i))
		d.events.add("storm_phase", map[string]any{"index": i, "rate": p.Rate, "for_s": int(p.For.Seconds())})
		select {
		case <-stop:
			return
		case <-time.After(p.For):
		}
	}
	s.cur.Store(int64(len(s.phases)))
	d.rate.Store(0)
	d.phase.Store("storm_done")
	d.events.add("storm_done", nil)
}

// serveReceivers starts one listener per receiver. Each answers /work the
// way its class misbehaves, without pacing headers: a merchant endpoint
// doesn't advertise a rate.
func (d *driver) serveReceivers() {
	for i := 0; i < d.storm.receivers; i++ {
		class := receiverClass(i)
		mux := http.NewServeMux()
		mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			st := &d.storm.stats[d.storm.phaseIndex()][class]
			st.hits.Add(1)
			switch class {
			case classDead:
				st.errs.Add(1)
				w.WriteHeader(503)
				return
			case classFlaky:
				if rand.IntN(100) < 30 {
					st.errs.Add(1)
					w.WriteHeader(503)
					return
				}
			case classSlow:
				time.Sleep(time.Second + time.Duration(rand.Int64N(int64(7*time.Second))))
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"ok":true}`))
		})
		addr := ":" + strconv.Itoa(receiverBasePort+i)
		go func() {
			if err := http.ListenAndServe(addr, mux); err != nil {
				logf("receiver %s: %v", addr, err)
			}
		}()
	}
}

// stormStatus is the storm section of /status: the schedule, where it is,
// and per phase and receiver class what the receivers saw and how long jobs
// took end to end.
func (s *storm) status() map[string]any {
	if s == nil {
		return nil
	}
	cur := s.phaseIndex()
	var startedAt any
	if ms := s.started.Load(); ms > 0 {
		startedAt = time.UnixMilli(ms).UTC()
	}
	phases := make([]map[string]any, 0, len(s.stats))
	for i := range s.stats {
		if i > cur {
			break
		}
		name := "after"
		rate := 0
		if i < len(s.phases) {
			name = fmt.Sprintf("storm_%d", i)
			rate = s.phases[i].Rate
		}
		classes := map[string]any{}
		for c := 0; c < numClasses; c++ {
			st := &s.stats[i][c]
			e2e := st.e2e.peek()
			classes[classNames[c]] = map[string]any{
				"receiver_hits":   st.hits.Load(),
				"receiver_errors": st.errs.Load(),
				"completed":       st.completed.Load(),
				"failed":          st.failed.Load(),
				"e2e_p50_ms":      e2e.ms(0.50),
				"e2e_p99_ms":      e2e.ms(0.99),
			}
		}
		phases = append(phases, map[string]any{"phase": name, "rate": rate, "classes": classes})
	}
	counts := map[string]int{}
	for i := 0; i < s.receivers; i++ {
		counts[classNames[receiverClass(i)]]++
	}
	return map[string]any{
		"started_at":      startedAt,
		"schedule":        s.phases,
		"current_phase":   cur,
		"done":            cur >= len(s.phases),
		"receivers":       counts,
		"body_kb_max":     s.bodyKB,
		"phases_by_class": phases,
	}
}

// peek returns the counts so far without resetting.
func (h *hist) peek() histCounts {
	h.mu.Lock()
	defer h.mu.Unlock()
	return histCounts{B: h.b, N: h.n}
}
