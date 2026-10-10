package main

import (
	"encoding/gob"
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
}

type lostJob struct {
	Seq        uint64 `json:"seq"`
	JobID      string `json:"job_id"`
	AcceptedAt string `json:"accepted_at"`
	Kind       string `json:"kind"`
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
}

type ledgerState struct {
	Next, Reserved uint64
	Done           bitset // webhook received
	Closed         bitset // accounted for without a webhook
	Uncertain      bitset // POST /jobs failed client-side; may still exist
	Outstanding    map[uint64]*pending
	Gaps           [][2]uint64
	Restarts       []int64 // unix ms of each kill -9 / machine restart
	Counters       Counters
	Lost           []lostJob
	RunStarted     time.Time
}

type ledger struct {
	mu   sync.Mutex
	s    ledgerState
	path string
	seqF string
}

const seqBlock = 100_000

func openLedger(dir string) *ledger {
	l := &ledger{path: filepath.Join(dir, "ledger.gob"), seqF: filepath.Join(dir, "seq.reserved")}
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
	l.mu.Lock()
	defer l.mu.Unlock()
	l.s.Counters.Accepted++
	if l.s.Done.has(seq) {
		return // the webhook beat the 201 back
	}
	l.s.Outstanding[seq] = &pending{JobID: jobID, AcceptedAt: at.UnixMilli()}
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
	if status == "failed" {
		c.Failed++
	} else {
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

// resolve applies the outcome of a GET /jobs/{id} check. status is the job's
// status there, "" for 404, or "error" if the check itself failed.
func (l *ledger) resolve(seq uint64, status string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p, ok := l.s.Outstanding[seq]
	if !ok {
		return // webhook arrived meanwhile
	}
	p.Checking = false
	c := &l.s.Counters
	closeAs := func(kind string) {
		delete(l.s.Outstanding, seq)
		l.s.Closed.set(seq)
		if kind != "" {
			l.s.Lost = append(l.s.Lost, lostJob{Seq: seq, JobID: p.JobID,
				AcceptedAt: time.UnixMilli(p.AcceptedAt).UTC().Format(time.RFC3339Nano), Kind: kind})
			if len(l.s.Lost) > 200 {
				l.s.Lost = l.s.Lost[len(l.s.Lost)-200:]
			}
		}
	}
	switch status {
	case "error":
	case "completed", "failed":
		if p.SeenTerminal == 0 {
			p.SeenTerminal = now.UnixMilli()
		} else if now.Sub(time.UnixMilli(p.SeenTerminal)) > 10*time.Minute {
			c.TerminalNoWebhook++
			closeAs("")
		}
	case "":
		if p.SeenTerminal != 0 {
			c.TerminalNoWebhook++ // finished, then expired from the store
			closeAs("")
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
