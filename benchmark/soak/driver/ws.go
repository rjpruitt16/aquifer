// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

// WebSocket load. Sessions connect through the target's GET /websocket to
// this driver's own aqueduct.v1 backend (/socket on the private port), which
// misbehaves on its own schedule like /work and /hook do.
//
// Each session sends a command every ws-cmd-every. The backend answers every
// command with one ack and ws-events events, all with IDs derived from the
// command's message_id, so a resent command produces the same IDs and the
// backend needs no state to be idempotent. A command is complete once its ack
// and every event have arrived.
//
// The session keeps the stream_id of the last message it processed and
// reconnects from it. aqueduct.v1 does not replay commands after an
// ambiguous upstream failure, so unanswered commands are resent, with the
// same message_id, exactly when one may have been lost: after the session
// reconnects (once the replay is done), and when the target reports a new
// backend connection (status "connected" after the first). A slow answer on
// a healthy connection is never resent: run 2 first resent on a 15s timer,
// and under load that became a retry storm of its own. A command still
// incomplete 5 minutes after it was first sent is counted as lost if the
// target confirmed recording it, unrecorded otherwise.

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const wsSubprotocol = "aqueduct.v1"

var wsUpstreamModes = []mode{
	{Name: "healthy", Weight: 75},
	{Name: "slow", Weight: 10, SlowMin: 50 * time.Millisecond, SlowMax: 500 * time.Millisecond},
	{Name: "dropping", Weight: 10}, // established sockets close at random
	{Name: "down", Weight: 5, ErrPct: 100, ErrStatus: 503, RetryAfter: 2, DownMin: time.Minute, DownMax: 3 * time.Minute},
}

type wsConfig struct {
	Enabled       bool          `json:"enabled"`
	Sessions      int           `json:"steady_sessions"`
	BurstSessions int           `json:"burst_sessions"`
	CmdEvery      time.Duration `json:"-"`
	CmdEveryMS    int64         `json:"command_every_ms"`
	Events        int           `json:"events_per_command"`
	RampSteps     []int         `json:"ramp_steps_sessions"`
}

// wsLedgerVersion changes when the meaning of the counters changes. A saved
// ledger from another version is logged as an event and replaced, so one
// run's totals never mix two behaviours. 2: resends only after a reconnect.
// 3: each session gets a new ID (a reused one replayed the old session's
// transcript, counted as unknown messages), and messages for commands
// already counted lost are counted separately.
const wsLedgerVersion = 3

// WSCounters are cumulative and saved with the ledger.
type WSCounters struct {
	Version           int               `json:"version"`
	Handshakes        uint64            `json:"handshakes"`
	HandshakeRejected map[string]uint64 `json:"handshakes_rejected_by_status"`
	HandshakeErrors   uint64            `json:"handshake_errors"`
	ReplayGaps        uint64            `json:"replay_gaps_409"`
	Disconnects       uint64            `json:"disconnects"`
	ServerDraining    uint64            `json:"server_draining"`
	Sent              uint64            `json:"commands_sent"`
	Resent            uint64            `json:"commands_resent"`
	Recorded          uint64            `json:"commands_recorded"`
	Completed         uint64            `json:"commands_completed"`
	Events            uint64            `json:"events_received"`
	DupEvents         uint64            `json:"duplicate_events"`
	DupAfterResend    uint64            `json:"duplicate_events_after_resend"`
	OutOfOrder        uint64            `json:"out_of_order_events"`
	Unknown           uint64            `json:"messages_for_unknown_commands"`
	Lost              uint64            `json:"lost_commands"`
	AfterExpiry       uint64            `json:"messages_after_expiry"`
	Unrecorded        uint64            `json:"unrecorded_commands"`
}

type wsLedger struct {
	mu   sync.Mutex
	c    WSCounters
	path string
	open atomic.Int64 // sessions connected right now
}

// openWSLedger loads the saved ledger. It returns the saved counters it
// discarded, if they were from another ledger version.
func openWSLedger(dir string) (*wsLedger, *WSCounters) {
	l := &wsLedger{path: filepath.Join(dir, "ws-ledger.json")}
	var discarded *WSCounters
	if b, err := os.ReadFile(l.path); err == nil {
		var saved WSCounters
		if json.Unmarshal(b, &saved) == nil {
			if saved.Version == wsLedgerVersion {
				l.c = saved
			} else {
				discarded = &saved
			}
		}
	}
	l.c.Version = wsLedgerVersion
	if l.c.HandshakeRejected == nil {
		l.c.HandshakeRejected = map[string]uint64{}
	}
	return l, discarded
}

func (l *wsLedger) add(f func(c *WSCounters)) {
	l.mu.Lock()
	f(&l.c)
	l.mu.Unlock()
}

func (l *wsLedger) view() WSCounters {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.c
	c.HandshakeRejected = map[string]uint64{}
	for k, v := range l.c.HandshakeRejected {
		c.HandshakeRejected[k] = v
	}
	return c
}

func (l *wsLedger) save() error {
	b, _ := json.Marshal(l.view())
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

// wsWindow counts one minute of WebSocket activity.
type wsWindow struct {
	sent, completed, dups, disconnects, rejected atomic.Int64
	cmd                                          hist // first send to complete
}

// ---- backend (the target's upstream) ----

var wsUpgrader = websocket.Upgrader{
	Subprotocols: []string{wsSubprotocol},
	CheckOrigin:  func(*http.Request) bool { return true },
}

type wsEnvelope struct {
	Type      string          `json:"type"`
	MessageID string          `json:"message_id,omitempty"`
	CausedBy  string          `json:"caused_by,omitempty"`
	StreamID  string          `json:"stream_id,omitempty"`
	State     string          `json:"state,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type wsEventPayload struct {
	I int `json:"i"`
	N int `json:"n"`
}

func (d *driver) handleWSUpstream(w http.ResponseWriter, r *http.Request) {
	e := d.wsUpstream
	e.hits.Add(1)
	if m := e.active(); m.ErrPct > 0 {
		e.errs.Add(1)
		w.Header().Set("Retry-After", strconv.Itoa(m.RetryAfter))
		w.WriteHeader(m.ErrStatus)
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(1 << 20)

	var wmu sync.Mutex
	write := func(v any) error {
		wmu.Lock()
		defer wmu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteJSON(v)
	}
	done := make(chan struct{})
	defer close(done)
	// Misbehaviour on established sockets: "down" closes everything at once,
	// "dropping" closes about one socket in 30 each second.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				m := e.active()
				if m.ErrPct == 100 || (m.Name == "dropping" && rand.IntN(30) == 0) {
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		var cmd wsEnvelope
		if err := conn.ReadJSON(&cmd); err != nil {
			return
		}
		if cmd.Type != "command" || cmd.MessageID == "" {
			continue
		}
		var p struct {
			Events int `json:"events"`
		}
		json.Unmarshal(cmd.Payload, &p)
		if p.Events <= 0 || p.Events > 100 {
			p.Events = 1
		}
		go func(mid string, n int) {
			if write(wsEnvelope{Type: "ack", MessageID: "ack-" + mid, CausedBy: mid}) != nil {
				return
			}
			for i := 0; i < n; i++ {
				if m := e.active(); m.SlowMax > 0 {
					time.Sleep(m.SlowMin + time.Duration(rand.Int64N(int64(m.SlowMax-m.SlowMin)+1)))
				}
				pl, _ := json.Marshal(wsEventPayload{I: i, N: n})
				if write(wsEnvelope{Type: "event", MessageID: fmt.Sprintf("ev-%s-%d", mid, i), CausedBy: mid, Payload: pl}) != nil {
					return
				}
			}
		}(cmd.MessageID, p.Events)
	}
}

// ---- client sessions ----

type wsCmd struct {
	firstSent time.Time
	lastSent  time.Time
	sends     int
	recorded  bool
	acked     bool
	got       []bool
	gotN      int
}

type wsSession struct {
	d       *driver
	idx     int
	id      string
	cursor  string
	next    int
	mu      sync.Mutex
	pending map[string]*wsCmd
	done    map[string]doneKind // recently closed, to tell duplicates and late messages from unknowns
	order   []string
}

type doneKind uint8

const (
	doneCompleted doneKind = iota + 1
	doneExpired
)

func (s *wsSession) markDone(mid string, kind doneKind) {
	s.done[mid] = kind
	s.order = append(s.order, mid)
	if len(s.order) > 1024 {
		delete(s.done, s.order[0])
		s.order = s.order[1:]
	}
}

// wsManager keeps the wanted number of sessions running. Sessions at an index
// at or above the wanted count stop sending, wait for their pending commands
// and exit.
func (d *driver) wsManager(stop <-chan struct{}) {
	running := map[int]bool{}
	var mu sync.Mutex
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		want := int(d.wsWanted.Load())
		if d.paused.Load() {
			want = 0
		}
		mu.Lock()
		for i := 0; i < want; i++ {
			if !running[i] {
				running[i] = true
				// A new ID each time: a burst session started at the same
				// index as an earlier one would otherwise connect without a
				// cursor and get the earlier session's whole transcript.
				s := &wsSession{d: d, idx: i, id: fmt.Sprintf("%s-ws-%d-%d", d.runID, i, time.Now().UnixMilli()),
					pending: map[string]*wsCmd{}, done: map[string]doneKind{}}
				d.wsSessions.Store(i, s)
				go func() {
					s.run(stop)
					d.wsSessions.Delete(s.idx)
					mu.Lock()
					delete(running, s.idx)
					mu.Unlock()
				}()
			}
		}
		mu.Unlock()
	}
}

func (s *wsSession) wanted() bool {
	return !s.d.paused.Load() && s.idx < int(s.d.wsWanted.Load())
}

func (s *wsSession) run(stop <-chan struct{}) {
	backoff := 250 * time.Millisecond
	for {
		select {
		case <-stop:
			return
		default:
		}
		s.mu.Lock()
		idle := len(s.pending) == 0
		s.mu.Unlock()
		if !s.wanted() && idle {
			return
		}
		ok := s.connectOnce(stop)
		if ok {
			backoff = 250 * time.Millisecond
		} else {
			backoff = min(backoff*2, 10*time.Second)
		}
		s.expire(time.Now())
		time.Sleep(backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1)))
	}
}

// connectOnce runs one connection until it drops. It reports whether the
// handshake succeeded.
func (s *wsSession) connectOnce(stop <-chan struct{}) bool {
	d := s.d
	q := url.Values{"session_id": {s.id}}
	s.mu.Lock()
	if s.cursor != "" {
		q.Set("after", s.cursor)
	}
	s.mu.Unlock()
	target := strings.Replace(d.cfg.Target, "http", "ws", 1) + "/websocket?" + q.Encode()
	hdr := http.Header{}
	hdr.Set("X-Aqueduct-Upstream-URL", strings.Replace(d.cfg.Self, "http", "ws", 1)+"/socket")
	dialer := websocket.Dialer{Subprotocols: []string{wsSubprotocol}, HandshakeTimeout: 20 * time.Second}
	conn, resp, err := dialer.Dial(target, hdr)
	if err != nil {
		if resp != nil {
			code := strconv.Itoa(resp.StatusCode)
			d.wsLedger.add(func(c *WSCounters) {
				c.HandshakeRejected[code]++
				if resp.StatusCode == 409 {
					c.ReplayGaps++
				}
			})
			if resp.StatusCode == 409 {
				// The transcript no longer covers our cursor. Start a fresh
				// session; pending commands are resent there with the same IDs.
				s.mu.Lock()
				s.id = fmt.Sprintf("%s-ws-%d-%d", d.runID, s.idx, time.Now().UnixMilli())
				s.cursor = ""
				s.mu.Unlock()
			}
		} else {
			d.wsLedger.add(func(c *WSCounters) { c.HandshakeErrors++ })
		}
		d.wsWin.rejected.Add(1)
		return false
	}
	d.wsLedger.add(func(c *WSCounters) { c.Handshakes++ })
	d.wsLedger.open.Add(1)
	defer d.wsLedger.open.Add(-1)
	conn.SetReadLimit(1 << 20)

	var wmu sync.Mutex
	send := func(mid string, resend bool) error {
		b, _ := json.Marshal(map[string]any{"type": "command", "message_id": mid,
			"payload": map[string]any{"events": d.cfg.WS.Events}})
		wmu.Lock()
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err := conn.WriteMessage(websocket.TextMessage, b)
		wmu.Unlock()
		if err == nil {
			if resend {
				d.wsLedger.add(func(c *WSCounters) { c.Resent++ })
			} else {
				d.wsLedger.add(func(c *WSCounters) { c.Sent++ })
				d.wsWin.sent.Add(1)
			}
		}
		return err
	}

	closed := make(chan struct{})
	replayed := make(chan struct{}, 1)
	upstreamBack := make(chan struct{}, 1)
	go s.writer(conn, send, closed, replayed, upstreamBack, stop)
	s.reader(conn, replayed, upstreamBack)
	close(closed)
	conn.Close()
	d.wsLedger.add(func(c *WSCounters) { c.Disconnects++ })
	d.wsWin.disconnects.Add(1)
	return true
}

func (s *wsSession) writer(conn *websocket.Conn, send func(string, bool) error, closed, replayed, upstreamBack <-chan struct{}, stop <-chan struct{}) {
	d := s.d
	// Wait for the replay to finish before resending, so commands the
	// backend already answered aren't sent twice.
	select {
	case <-replayed:
	case <-time.After(5 * time.Second):
	case <-closed:
		return
	}
	// resend sends every unanswered command again. Only called when a
	// command may have been lost: a new client or backend connection.
	resend := func() {
		now := time.Now()
		var mids []string
		s.mu.Lock()
		for mid, c := range s.pending {
			if !c.acked || c.gotN < len(c.got) {
				c.lastSent = now
				c.sends++
				mids = append(mids, mid)
			}
		}
		s.mu.Unlock()
		for _, mid := range mids {
			if send(mid, true) != nil {
				return
			}
		}
	}
	resend()

	every := d.cfg.WS.CmdEvery
	tick := time.NewTicker(every)
	defer tick.Stop()
	check := time.NewTicker(time.Second)
	defer check.Stop()
	// Spread sessions across the interval.
	time.Sleep(time.Duration(rand.Int64N(int64(every))))
	for {
		select {
		case <-closed:
			return
		case <-stop:
			conn.Close()
			return
		case <-upstreamBack:
			resend()
		case <-check.C:
			s.mu.Lock()
			idle := len(s.pending) == 0
			s.mu.Unlock()
			if !s.wanted() && idle {
				conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
				conn.Close()
				return
			}
		case <-tick.C:
			if !s.wanted() {
				continue
			}
			s.mu.Lock()
			if len(s.pending) >= 50 {
				s.mu.Unlock()
				continue
			}
			s.next++
			mid := fmt.Sprintf("c-%s-%d", s.id, s.next)
			now := time.Now()
			s.pending[mid] = &wsCmd{firstSent: now, lastSent: now, sends: 1, got: make([]bool, d.cfg.WS.Events)}
			s.mu.Unlock()
			if send(mid, false) != nil {
				return
			}
		}
	}
}

func (s *wsSession) reader(conn *websocket.Conn, replayed, upstreamBack chan<- struct{}) {
	d := s.d
	backendConnected := false
	signal := func(ch chan<- struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		var m wsEnvelope
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		now := time.Now()
		switch m.Type {
		case "status":
			switch m.State {
			case "replay_complete":
				signal(replayed)
			case "connected":
				// The first one belongs to this client connection; a later
				// one means the target reconnected the backend, and commands
				// in flight then may never have reached it.
				if backendConnected {
					signal(upstreamBack)
				} else {
					backendConnected = true
					signal(replayed)
				}
			case "server_draining":
				d.wsLedger.add(func(c *WSCounters) { c.ServerDraining++ })
			}
		case "command_recorded":
			s.mu.Lock()
			c := s.pending[m.MessageID]
			if c != nil && !c.recorded {
				c.recorded = true
				s.mu.Unlock()
				d.wsLedger.add(func(cc *WSCounters) { cc.Recorded++ })
			} else {
				s.mu.Unlock()
			}
		case "ack", "event":
			s.handleBackend(m, now)
		}
	}
}

func (s *wsSession) handleBackend(m wsEnvelope, now time.Time) {
	d := s.d
	s.mu.Lock()
	if m.StreamID != "" {
		s.cursor = m.StreamID
	}
	c := s.pending[m.CausedBy]
	if c == nil {
		kind := s.done[m.CausedBy]
		s.mu.Unlock()
		switch kind {
		case doneCompleted:
			d.wsLedger.add(func(cc *WSCounters) { cc.DupEvents++; cc.DupAfterResend++ })
			d.wsWin.dups.Add(1)
		case doneExpired:
			d.wsLedger.add(func(cc *WSCounters) { cc.AfterExpiry++ })
		default:
			d.wsLedger.add(func(cc *WSCounters) { cc.Unknown++ })
		}
		return
	}
	dup, outOfOrder := false, false
	if m.Type == "ack" {
		dup = c.acked
		c.acked = true
	} else {
		var p wsEventPayload
		json.Unmarshal(m.Payload, &p)
		if p.I < 0 || p.I >= len(c.got) {
			s.mu.Unlock()
			d.wsLedger.add(func(cc *WSCounters) { cc.Unknown++ })
			return
		}
		if c.got[p.I] {
			dup = true
		} else {
			for j := 0; j < p.I; j++ {
				if !c.got[j] {
					outOfOrder = true
					break
				}
			}
			c.got[p.I] = true
			c.gotN++
		}
	}
	complete := c.acked && c.gotN == len(c.got)
	resent := c.sends > 1
	if complete {
		delete(s.pending, m.CausedBy)
		s.markDone(m.CausedBy, doneCompleted)
	}
	s.mu.Unlock()

	d.wsLedger.add(func(cc *WSCounters) {
		if m.Type == "event" {
			cc.Events++
		}
		if dup {
			cc.DupEvents++
			if resent {
				cc.DupAfterResend++
			}
		}
		if outOfOrder {
			cc.OutOfOrder++
		}
		if complete {
			cc.Completed++
		}
	})
	if dup {
		d.wsWin.dups.Add(1)
	}
	if complete {
		d.wsWin.completed.Add(1)
		d.wsWin.cmd.add(now.Sub(c.firstSent))
	}
}

// expire closes out commands still incomplete 5 minutes after first send.
func (s *wsSession) expire(now time.Time) {
	var lost, unrecorded uint64
	s.mu.Lock()
	for mid, c := range s.pending {
		if now.Sub(c.firstSent) > 5*time.Minute {
			if c.recorded {
				lost++
			} else {
				unrecorded++
			}
			delete(s.pending, mid)
			s.markDone(mid, doneExpired)
		}
	}
	s.mu.Unlock()
	if lost+unrecorded > 0 {
		s.d.wsLedger.add(func(c *WSCounters) { c.Lost += lost; c.Unrecorded += unrecorded })
	}
}

// wsExpireLoop expires commands on sessions that stay connected (run only
// expires between connections).
func (d *driver) wsExpireLoop(stop <-chan struct{}) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			d.wsSessions.Range(func(_, v any) bool {
				v.(*wsSession).expire(now)
				return true
			})
		}
	}
}

// ---- peaks and ramps ----

// untilNext returns the time until the next hh:mm (minute of the hour).
func untilNext(minute int) time.Duration {
	now := time.Now().UTC()
	next := now.Truncate(time.Hour).Add(time.Duration(minute) * time.Minute)
	if !next.After(now) {
		next = next.Add(time.Hour)
	}
	return time.Until(next)
}

// wsBurstLoop raises the session count to ws-burst-sessions for burst-for at
// half past every hour, so WebSocket peaks fall between the HTTP ones.
func (d *driver) wsBurstLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-time.After(untilNext(30)):
		}
		if d.paused.Load() || !d.wsBusy.TryLock() {
			continue
		}
		d.wsBurst(d.cfg.BurstFor)
		d.wsBusy.Unlock()
	}
}

func (d *driver) wsBurst(dur time.Duration) {
	d.events.add("ws_burst_start", map[string]any{"sessions": d.cfg.WS.BurstSessions, "for_s": int(dur.Seconds())})
	d.wsPhase.Store("burst")
	d.wsWanted.Store(int64(d.cfg.WS.BurstSessions))
	time.Sleep(dur)
	d.wsWanted.Store(int64(d.cfg.WS.Sessions))
	d.wsPhase.Store("steady")
	d.events.add("ws_burst_end", nil)
}

// peakLoop runs both bursts at once every day at peak-hour UTC.
func (d *driver) peakLoop(stop <-chan struct{}) {
	if d.cfg.PeakHourUTC < 0 {
		return
	}
	for {
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), d.cfg.PeakHourUTC, 15, 0, 0, time.UTC)
		if !next.After(now) {
			next = next.Add(24 * time.Hour)
		}
		select {
		case <-stop:
			return
		case <-time.After(time.Until(next)):
		}
		if d.paused.Load() {
			continue
		}
		d.busy.Lock()
		d.wsBusy.Lock()
		d.events.add("combined_peak_start", map[string]any{"rate": d.cfg.BurstRate, "sessions": d.cfg.WS.BurstSessions})
		d.phase.Store("burst")
		d.rate.Store(int64(d.cfg.BurstRate))
		d.wsBurst(d.cfg.BurstFor)
		d.rate.Store(int64(d.cfg.Rate))
		d.phase.Store("steady")
		d.events.add("combined_peak_end", nil)
		d.wsBusy.Unlock()
		d.busy.Unlock()
	}
}

// wsRamp steps the session count up with every misbehaving endpoint held
// healthy and records the most sessions whose commands all completed on
// time. Called by rampLoop on alternate days, holding both busy locks.
func (d *driver) wsRamp() (int, []rampStep) {
	d.wsPhase.Store("ramp")
	var steps []rampStep
	ceiling := 0
	for _, n := range d.cfg.WS.RampSteps {
		d.wsWanted.Store(int64(n))
		time.Sleep(30 * time.Second) // let the sessions open
		before := d.wsLedger.view()
		time.Sleep(d.cfg.RampStep)
		after := d.wsLedger.view()
		secs := d.cfg.RampStep.Seconds()
		offered := float64(n) / d.cfg.WS.CmdEvery.Seconds()
		st := rampStep{Offered: n,
			Accepted:  float64(after.Sent-before.Sent) / secs,
			Completed: float64(after.Completed-before.Completed) / secs,
			Shed:      float64(sumMap(after.HandshakeRejected)-sumMap(before.HandshakeRejected)) / secs}
		st.OK = st.Accepted >= 0.95*offered && st.Completed >= 0.95*st.Accepted
		steps = append(steps, st)
		if !st.OK {
			break
		}
		ceiling = n
	}
	d.wsWanted.Store(int64(d.cfg.WS.Sessions))
	d.wsPhase.Store("steady")
	return ceiling, steps
}

func sumMap(m map[string]uint64) uint64 {
	var n uint64
	for _, v := range m {
		n += v
	}
	return n
}
