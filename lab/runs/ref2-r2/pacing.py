"""Backend-side pacing for Aquifer (X-Aqueduct-* headers), used with `app.py --pacing`.

The saturation signal is the database pool: how long an order waits to check out a
connection. The loop is AIMD on a total budget (orders/s across every Aquifer queue):

- once a second, if the p95 checkout wait in the last second is over WAIT_LIMIT, or any
  order got a 503, cut the budget by CUT and remember where trouble started (ssthresh);
- otherwise grow it: STEP_FAST below ssthresh, STEP_SLOW above it, up to CEILING.

The budget is then split across the queues Aquifer is sending from, max-min fair: a
queue with little waiting (X-Aqueduct-Queue-Depth) only needs what it is using, so it is
offered that plus headroom; whatever is left goes evenly to the queues that are backed
up. Each response carries the rate for the queue that sent it (X-Aqueduct-Rps).

"Not backed up" is judged from what the queue actually used: a queue that sent less than
80% of the rate it was offered last tick has nothing more waiting. (X-Aqueduct-Queue-Depth
was the first choice, but on Aquifer 0.15.1 it carries the whole target's backlog, the same
number as X-Aqueduct-Upstream-Backlog, not the sending queue's own depth.)
Queues are told apart by the order's partner, which is the Aquifer user_id.
"""
import threading, time

START = 70.0       # step 2 knee
CEILING = 80.0     # 4 connections / 50 ms; the YAML ceiling is the hard cap anyway
FLOOR = 10.0
WAIT_LIMIT = 0.025 # seconds of pool checkout wait (p95) that counts as saturated
CUT = 0.7
STEP_FAST, STEP_SLOW = 4.0, 1.0


class Pacer:
    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.lock = threading.Lock()
        self.budget = START
        self.ssthresh = CEILING
        self.hold = 0
        self.tick_at = time.time() + 1
        self.waits = []
        self.errors = 0
        self.seen = {}       # partner -> (last_seen, depth)
        self.count = {}      # partner -> requests this tick
        self.rate = {}       # partner -> requests/s last tick
        self.alloc = {}
        self.cuts = self.raises = 0
        self.last = "start"

    def _tick(self, now):
        waits = sorted(self.waits)
        p95 = waits[int(0.95 * (len(waits) - 1))] if waits else 0.0
        if self.hold > 0:
            self.hold -= 1; self.last = "hold"
        elif p95 > WAIT_LIMIT or self.errors:
            self.ssthresh = max(FLOOR, self.budget * 0.95)
            self.budget = max(FLOOR, self.budget * CUT)
            self.hold = 1; self.cuts += 1
            self.last = f"cut p95_wait={p95:.3f} errors={self.errors}"
        elif waits:  # only grow while there is demand
            self.budget = min(CEILING, self.budget + (STEP_FAST if self.budget < self.ssthresh else STEP_SLOW))
            self.raises += 1; self.last = f"raise p95_wait={p95:.3f}"
        self.rate = dict(self.count)
        self.count = {}
        self.waits = []
        self.errors = 0
        self._allocate(now)

    def _allocate(self, now):
        active = {p: d for p, (t, d) in self.seen.items() if now - t < 2.0}
        if not active:
            self.alloc = {}; return
        need = {}
        for p in active:
            r = self.rate.get(p, 0.0)
            offered = self.alloc.get(p)
            if offered and r < 0.8 * offered:  # used less than offered: offer what it uses plus headroom
                need[p] = r * 1.25 + 2.0
        left = self.budget; todo = set(active); alloc = {}
        while todo:
            share = left / len(todo)
            small = [p for p in todo if p in need and need[p] <= share]
            if not small:
                for p in todo:
                    alloc[p] = share
                break
            for p in small:
                alloc[p] = need[p]; left -= need[p]; todo.discard(p)
        self.alloc = alloc

    def observe(self, order, aq, wait, code):
        now = time.time()
        partner = order.get("partner", "?")
        try:
            depth = float(aq.get("Queue-Depth") or 0)
        except ValueError:
            depth = 0.0
        with self.lock:
            self.waits.append(wait or 0.0)
            if code >= 500:
                self.errors += 1
            self.seen[partner] = (now, depth)
            self.count[partner] = self.count.get(partner, 0) + 1
            if now >= self.tick_at:
                self.tick_at = now + 1
                self._tick(now)
            n = max(1, sum(1 for t, _ in self.seen.values() if now - t < 2.0))
            rps = self.alloc.get(partner, self.budget / n)
        return {"X-Aqueduct-Rps": f"{max(0.5, rps):.2f}"}

    def snapshot(self):
        with self.lock:
            return {"budget": round(self.budget, 1), "ssthresh": round(self.ssthresh, 1), "cuts": self.cuts,
                    "raises": self.raises, "last": self.last, "alloc": {k: round(v, 1) for k, v in self.alloc.items()}}
