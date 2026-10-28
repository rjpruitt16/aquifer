// Copyright 2026 Rahmi Pruitt
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// The ledger accounts for every job the driver submits. Each job gets a
// sequence number, carried in its webhook URL (?s=N), so a webhook maps back
// to its job without keeping job IDs around after it finishes. One bit per
// job records "webhook received", which catches duplicates for the whole
// week in about 45MB.
//
// A job that is still unaccounted for 5 minutes after it was accepted gets
// checked with GET /jobs/{id}:
//   - completed or failed there, but no webhook 10 minutes later: the
//     webhook was given up on (expected during webhook-receiver outages);
//   - still queued or running: checked again later, "stuck" after 24h;
//   - 404 before ever being seen finished: lost. A loss whose 201 arrived
//     just before a kill -9 is counted apart, since the 100ms flush allows it.

type bitset struct{ W []uint64 }

func (b *bitset) set(i uint64) (was bool) {
	w := i / 64
	for uint64(len(b.W)) <= w {
		b.W = append(b.W, make([]uint64, 1024)...)
	}
	mask := uint64(1) << (i % 64)
	was = b.W[w]&mask != 0
	b.W[w] |= mask
	return was
}

func (b *bitset) has(i uint64) bool {
	w := i / 64
	return w < uint64(len(b.W)) && b.W[w]&(1<<(i%64)) != 0
}

type pending struct {
	JobID        string
	AcceptedAt   int64 // unix ms when the 201 arrived
	SeenTerminal int64 // unix ms when GET /jobs first showed completed/failed
	Checking     bool
	Node         string // cluster test: the node that kept the job
}

type lostJob struct {
	Seq        uint64 `json:"seq"`
	JobID      string `json:"job_id"`
	AcceptedAt string `json:"accepted_at"`
	Kind       string `json:"kind"`
	Node       string `json:"node,omitempty"`
	LostAt     string `json:"lost_at,omitempty"`
}

// Counters is the ledger's running totals, reported on /status.
type Counters struct {
	Submitted                uint64 `json:"submitted"`
	Accepted                 uint64 `json:"accepted"`
	Shed                     uint64 `json:"shed_429_503"`
	ClientErrors             uint64 `json:"client_errors"`
	Completed                uint64 `json:"completed_webhooks"`
	Failed                   uint64 `json:"failed_webhooks"`
	Duplicates               uint64 `json:"duplicate_webhooks"`
	DuplicatesNearRestart    uint64 `json:"duplicate_webhooks_within_10m_of_restart"`
	TerminalNoWebhook        uint64 `json:"finished_without_webhook"`
	LateWebhooks             uint64 `json:"late_webhooks_after_ledger_closed"`
	AcceptedAfterClientError uint64 `json:"webhooks_for_requests_that_errored_client_side"`
	UnknownWebhooks          uint64 `json:"webhooks_for_unknown_jobs"`
	GapWebhooks              uint64 `json:"webhooks_in_driver_restart_gaps"`
	Lost                     uint64 `json:"lost"`
	LostInFlushWindow        uint64 `json:"lost_within_flush_window_of_kill"`
	Stuck                    uint64 `json:"stuck_over_24h"`
	// LostInLine is gateway mode's lost: accepted into the gateway's line,
	// then never seen again, on the gateway, a backend, or as a webhook.
	LostInLine uint64 `json:"lost_in_gateway_line"`
	// Gateway mode: jobs that finished while their caller was still on the
	// gateway's line stream. The instance sends no webhook for those (the
	// result already reached the caller), so the stream is the record.
	CompletedOnStream uint64 `json:"completed_on_stream"`
	FailedOnStream    uint64 `json:"failed_on_stream"`
	// LostAfterInstanceRestart is gateway mode's flush-window loss: the
	// gateway handed the job over (the instance answered with its id), the
	// instance restarted, and it no longer has the job. The gateway's line
	// held it until then; the instance acknowledged it before it reached
	// disk. The hand-off can be minutes after the job entered the line, so
	// the 100ms rule can't be measured from acceptance.
	LostAfterInstanceRestart uint64 `json:"lost_after_instance_restart"`
}

type ledgerState struct {
	Next, Reserved uint64
	Done           bitset // webhook received
	Closed         bitset // accounted for without a webhook
	Uncertain      bitset // POST /jobs failed client-side; may still exist
	Outstanding    map[uint64]*pending
	Gaps           [][2]uint64
	Restarts       []int64 // unix ms of each kill -9 / machine restart
	// InstanceRestarts: gateway mode, unix ms of each kill or hold of a
	// backend instance (an Aquifer behind the gateway).
	InstanceRestarts []int64
	Counters         Counters
	Lost             []lostJob
	RunStarted       time.Time
}

type ledger struct {
	mu   sync.Mutex
	s    ledgerState
	path string
	seqF string
	// lostF gets one JSON line per lost job, so every one can be traced
	// afterwards; the state keeps only the last 200 of them.
	lostF string

	failedIsTerminal bool // storm -no-webhook, see resolve
}

const seqBlock = 100_000

func openLedger(dir string) *ledger {
	l := &ledger{path: filepath.Join(dir, "ledger.gob"), seqF: filepath.Join(dir, "seq.reserved"),
		lostF: filepath.Join(dir, "lost.jsonl")}
	l.s.Outstanding = map[uint64]*pending{}
	if f, err := os.Open(l.path); err == nil {
		if err := gob.NewDecoder(f).Decode(&l.s); err != nil {
			logf("ledger: could not load %s: %v (starting fresh)", l.path, err)
			l.s = ledgerState{Outstanding: map[uint64]*pending{}}
		}
		f.Close()
	}
	if l.s.Outstanding == nil {
		l.s.Outstanding = map[uint64]*pending{}
	}
	for _, p := range l.s.Outstanding {
		p.Checking = false // a check in progress when the driver stopped
	}
	if l.s.RunStarted.IsZero() {
		l.s.RunStarted = time.Now().UTC()
	}
	// Sequence numbers are reserved on disk in blocks ahead of use, so after
	// a driver crash nothing is reused; jobs submitted after the last saved
	// snapshot become a gap the ledger can't vouch for.
	if raw, err := os.ReadFile(l.seqF); err == nil {
		if r, err := strconv.ParseUint(string(raw), 10, 64); err == nil && r > l.s.Next {
			l.s.Gaps = append(l.s.Gaps, [2]uint64{l.s.Next, r})
			logf("ledger: driver restarted; seq %d..%d unaccounted (submitted after last save)", l.s.Next, r)
			l.s.Next = r
		}
	}
	l.s.Reserved = l.s.Next
	l.reserveLocked()
	return l
}

func (l *ledger) reserveLocked() {
	l.s.Reserved = l.s.Next + seqBlock
	tmp := l.seqF + ".tmp"
	os.WriteFile(tmp, []byte(strconv.FormatUint(l.s.Reserved, 10)), 0o644)
	if f, err := os.Open(tmp); err == nil {
		f.Sync()
		f.Close()
	}
	os.Rename(tmp, l.seqF)
}

func (l *ledger) nextSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq := l.s.Next
	l.s.Next++
	l.s.Counters.Submitted++
	if l.s.Next+seqBlock/2 >= l.s.Reserved {
		l.reserveLocked()
	}
	return seq
}

func (l *ledger) accepted(seq uint64, jobID string, at time.Time) {
	l.acceptedOn(seq, jobID, "", at)
}

// acceptedOn also records which cluster node kept the job ("" outside a
// cluster test).
func (l *ledger) acceptedOn(seq uint64, jobID, node string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.s.Counters.Accepted++
	if l.s.Done.has(seq) {
		return // the webhook beat the 201 back
	}
	l.s.Outstanding[seq] = &pending{JobID: jobID, AcceptedAt: at.UnixMilli(), Node: node}
}

func (l *ledger) shed() {
	l.mu.Lock()
	l.s.Counters.Shed++
	l.mu.Unlock()
}

func (l *ledger) clientError(seq uint64) {
	l.mu.Lock()
	l.s.Counters.ClientErrors++
	l.s.Uncertain.set(seq)
	l.mu.Unlock()
}

func (l *ledger) inGap(seq uint64) bool {
	for _, g := range l.s.Gaps {
		if seq >= g[0] && seq < g[1] {
			return true
		}
	}
	return false
}

func (l *ledger) nearRestart(t int64, window int64) bool {
	for i := len(l.s.Restarts) - 1; i >= 0; i-- {
		r := l.s.Restarts[i]
		if t >= r && t-r <= window {
			return true
		}
		if t-r > window {
			break
		}
	}
	return false
}

// webhook records a delivered webhook. It returns the job's end-to-end time
// (accept to webhook) when known, and whether this was a duplicate delivery.
func (l *ledger) webhook(seq uint64, status string, now time.Time) (e2e time.Duration, hasE2E, dup bool) {
	return l.finish(seq, status, now, false)
}

// finishedOnStream records a job whose result came back on the gateway's
// line stream instead of as a webhook.
func (l *ledger) finishedOnStream(seq uint64, status string, now time.Time) (dup bool) {
	_, _, dup = l.finish(seq, status, now, true)
	return dup
}

func (l *ledger) finish(seq uint64, status string, now time.Time, onStream bool) (e2e time.Duration, hasE2E, dup bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := &l.s.Counters
	if l.s.Done.set(seq) {
		c.Duplicates++
		if l.nearRestart(now.UnixMilli(), int64(10*time.Minute/time.Millisecond)) {
			c.DuplicatesNearRestart++
		}
		return 0, false, true
	}
	switch {
	case onStream && status == "failed":
		c.FailedOnStream++
	case onStream:
		c.CompletedOnStream++
	case status == "failed":
		c.Failed++
	default:
		c.Completed++
	}
	p, ok := l.s.Outstanding[seq]
	switch {
	case ok:
		delete(l.s.Outstanding, seq)
		return now.Sub(time.UnixMilli(p.AcceptedAt)), true, false
	case l.s.Closed.has(seq):
		c.LateWebhooks++
	case l.s.Uncertain.has(seq):
		c.AcceptedAfterClientError++
	case l.inGap(seq):
		c.GapWebhooks++
	case seq < l.s.Next:
		// Webhook raced ahead of the 201; accepted() will see Done.
	default:
		c.UnknownWebhooks++
	}
	return 0, false, false
}

func (l *ledger) recordRestart(at time.Time) {
	l.mu.Lock()
	l.s.Restarts = append(l.s.Restarts, at.UnixMilli())
	l.mu.Unlock()
}

func (l *ledger) recordInstanceRestart(at time.Time) {
	l.mu.Lock()
	l.s.InstanceRestarts = append(l.s.InstanceRestarts, at.UnixMilli())
	l.mu.Unlock()
}

// instanceRestartedSince reports whether an instance restarted after it
// could have acknowledged the job. acceptedAt is when the driver's request
// finished, which for a job relayed from the line can be just after the
// instance's ack and its kill (Canalis rerun 2026-10-28: jobs accepted up
// to 0.7s after a kill were gone), so it allows the request's length:
// at most the gateway's 5s line wait.
func (l *ledger) instanceRestartedSince(acceptedAt int64) bool {
	for _, r := range l.s.InstanceRestarts {
		if r >= acceptedAt-instanceAckSlack.Milliseconds() {
			return true
		}
	}
	return false
}

// stragglers returns up to max jobs accepted more than minAge ago with no
// webhook yet, oldest first, and marks them as being checked.
func (l *ledger) stragglers(minAge time.Duration, max int) map[uint64]pending {
	cutoff := time.Now().Add(-minAge).UnixMilli()
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[uint64]pending{}
	for seq, p := range l.s.Outstanding {
		if p.AcceptedAt < cutoff && !p.Checking {
			out[seq] = *p
			p.Checking = true
			if len(out) >= max {
				break
			}
		}
	}
	return out
}

const flushWindow = 300 * time.Millisecond

const instanceAckSlack = 5 * time.Second

// resolve applies the outcome of a GET /jobs/{id} check. status is the job's
// status there, "" for 404, or "error" if the check itself failed.
//
// With failedIsTerminal (storm -no-webhook) there is no webhook to wait
// for: a failed job is finished, and resolve returns "failed" when it
// closes one that way. A job Aquifer calls completed whose receiver never
// recorded it still counts as finished_without_webhook, which in that mode
// means "completed, but never delivered".
func (l *ledger) resolve(seq uint64, status string, now time.Time) (closed string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, ok := l.s.Outstanding[seq]
	if !ok {
		return "" // webhook arrived meanwhile
	}
	p.Checking = false
	c := &l.s.Counters
	closeAs := func(kind string) {
		delete(l.s.Outstanding, seq)
		l.s.Closed.set(seq)
		if kind != "" {
			j := lostJob{Seq: seq, JobID: p.JobID,
				AcceptedAt: time.UnixMilli(p.AcceptedAt).UTC().Format(time.RFC3339Nano), Kind: kind,
				Node: p.Node, LostAt: now.UTC().Format(time.RFC3339Nano)}
			l.s.Lost = append(l.s.Lost, j)
			l.recordLost(j)
			if len(l.s.Lost) > 200 {
				l.s.Lost = l.s.Lost[len(l.s.Lost)-200:]
			}
		}
	}
	switch status {
	case "error":
	case "failed":
		if l.failedIsTerminal {
			c.Failed++
			closeAs("")
			return "failed"
		}
		fallthrough
	case "completed":
		if p.SeenTerminal == 0 {
			p.SeenTerminal = now.UnixMilli()
		} else if now.Sub(time.UnixMilli(p.SeenTerminal)) > 10*time.Minute {
			c.TerminalNoWebhook++
			closeAs("")
		}
	case "unknown":
		// Gateway mode: a job accepted into the line whose id was never
		// seen and that the gateway no longer knows.
		if now.Sub(time.UnixMilli(p.AcceptedAt)) > lostInLineAfter {
			c.LostInLine++
			closeAs("lost_in_gateway_line")
		}
	case "":
		if p.SeenTerminal != 0 {
			c.TerminalNoWebhook++ // finished, then expired from the store
			closeAs("")
			return
		}
		if p.Node == lineNode && l.instanceRestartedSince(p.AcceptedAt) {
			c.LostAfterInstanceRestart++
			closeAs("lost_after_instance_restart")
			return
		}
		if l.acceptedJustBeforeRestart(p.AcceptedAt) {
			c.LostInFlushWindow++
			closeAs("lost_within_flush_window")
		} else {
			c.Lost++
			closeAs("lost")
		}
	default:
		if now.Sub(time.UnixMilli(p.AcceptedAt)) > 24*time.Hour {
			c.Stuck++
			closeAs("stuck_" + status)
		}
	}
	return ""
}

// recordLost appends j to lost.jsonl. Lost jobs are rare, so opening the
// file each time is fine.
func (l *ledger) recordLost(j lostJob) {
	line, _ := json.Marshal(j)
	f, err := os.OpenFile(l.lostF, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		logf("ledger: could not record lost job %d: %v", j.Seq, err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		logf("ledger: could not record lost job %d: %v", j.Seq, err)
	}
}

func (l *ledger) acceptedJustBeforeRestart(acceptedAt int64) bool {
	for _, r := range l.s.Restarts {
		if acceptedAt <= r+50 && r-acceptedAt <= flushWindow.Milliseconds() {
			return true
		}
	}
	return false
}

type ledgerView struct {
	Counters    Counters    `json:"counters"`
	Outstanding int         `json:"outstanding"`
	OldestAgeS  int64       `json:"oldest_outstanding_age_s"`
	Gaps        [][2]uint64 `json:"driver_restart_gaps,omitempty"`
	Restarts    int         `json:"target_restarts_caused"`
	RecentLost  []lostJob   `json:"recent_lost,omitempty"`
	RunStarted  time.Time   `json:"run_started"`
}

func (l *ledger) view() ledgerView {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := ledgerView{Counters: l.s.Counters, Outstanding: len(l.s.Outstanding), Gaps: l.s.Gaps,
		Restarts: len(l.s.Restarts), RunStarted: l.s.RunStarted}
	oldest := time.Now().UnixMilli()
	for _, p := range l.s.Outstanding {
		if p.AcceptedAt < oldest {
			oldest = p.AcceptedAt
		}
	}
	v.OldestAgeS = (time.Now().UnixMilli() - oldest) / 1000
	if n := len(l.s.Lost); n > 0 {
		v.RecentLost = append([]lostJob(nil), l.s.Lost[max(0, n-20):]...)
	}
	return v
}

// save writes the ledger to disk. Encoding holds the lock briefly enough
// (tens of ms for tens of MB) that intake barely notices.
func (l *ledger) save() error {
	tmp := l.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	l.mu.Lock()
	err = gob.NewEncoder(f).Encode(&l.s)
	l.mu.Unlock()
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err != nil {
		return fmt.Errorf("save ledger: %w", err)
	}
	return os.Rename(tmp, l.path)
}

// setJobID records the instance's id for a job accepted into a gateway's
// line, once the gateway names it.
func (l *ledger) setJobID(seq uint64, id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p, ok := l.s.Outstanding[seq]; ok && p.JobID == "" {
		p.JobID = id
	}
}
