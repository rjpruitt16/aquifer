"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (aquifer-pilot branch), all off by default so the service
behaves exactly as before unless asked:
  --log FILE   append one JSON line per POST /orders (timing, outcome, and any
               X-Aqueduct-* headers Aquifer sent) plus a 1 s metrics sample.
  --pace       answer with X-Aqueduct-* pacing headers so Aquifer releases
               orders at the rate the database pool can take (see Pacer).
"""
import argparse, json, threading, time
from collections import deque
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

LOG = None            # open file when --log is given
log_lock = threading.Lock()
PACER = None          # Pacer when --pace is given

AQ_HEADERS = ("Flow-Rate", "Queue-Depth", "Active-Queues", "Upstream-Backlog", "Total-Jobs", "Node-Id")


def log_line(obj):
    if LOG is None:
        return
    with log_lock:
        LOG.write(json.dumps(obj) + "\n")


class Pacer:
    """AIMD on the database pool, plus a max-min fair split across partners.

    Saturation signal: time spent waiting for a pool connection (p95 over the
    last 0.5 s) and any checkout timeouts. The DB plan's ceiling is the pool:
    POOL_SIZE / WRITE_SECONDS writes per second at most.

    budget   total orders/s the backend wants across every Aquifer queue
    per-queue X-Aqueduct-Rps: partners whose queue is not backlogged get what
             they are actually using plus headroom; backlogged partners split
             what is left evenly (water-filling).
    """

    TICK = 0.5
    WAIT_P95_LIMIT = 0.030   # cut when p95 checkout wait passes this (--pace-wait-ms)

    def __init__(self, start, floor, ceiling, split):
        self.budget = start
        self.floor = floor
        self.ceiling = ceiling
        self.ssthresh = ceiling
        self.split = split          # "waterfill" or "even"
        self.lock = threading.Lock()
        self.waits = []             # checkout waits this tick
        self.timeouts = 0
        self.done = 0
        self.last_cut = 0.0
        self.cuts = 0
        self.partners = {}          # partner -> {"done": deque[ts], "depth": int, "seen": ts}
        self.active_queues = 0
        self.upstream_backlog = 0
        self.advertised = {}        # partner -> last X-Aqueduct-Rps sent
        threading.Thread(target=self._loop, daemon=True).start()

    def observe(self, partner, wait, timed_out, depth, active, backlog=None):
        now = time.time()
        with self.lock:
            p = self.partners.setdefault(partner, {"done": deque(), "depth": 0, "seen": now})
            p["seen"] = now
            if depth is not None:
                p["depth"] = depth
            if active is not None:
                self.active_queues = active
            if backlog is not None:
                self.upstream_backlog = backlog
            if timed_out:
                self.timeouts += 1
            else:
                self.waits.append(wait)
                self.done += 1
                p["done"].append(now)

    def _loop(self):
        while True:
            time.sleep(self.TICK)
            now = time.time()
            with self.lock:
                waits = sorted(self.waits); self.waits = []
                timeouts = self.timeouts; self.timeouts = 0
                done = self.done; self.done = 0
                p95 = waits[int(0.95 * (len(waits) - 1))] if waits else 0.0
                before = self.budget
                action = "hold"
                if timeouts or p95 > self.WAIT_P95_LIMIT:
                    if now - self.last_cut > 1.0:       # one cut per round trip
                        self.ssthresh = max(self.floor, self.budget * 0.9)
                        self.budget = max(self.floor, self.budget * 0.7)
                        self.last_cut = now
                        self.cuts += 1
                        action = "cut"
                elif done >= 0.8 * self.budget * self.TICK or self.upstream_backlog > self.budget:
                    # grow when the budget is being used, or when Aquifer says
                    # more than a second of work is waiting for us
                    step = 4.0 if self.budget < self.ssthresh else 1.0
                    self.budget = min(self.ceiling, self.budget + step)
                    action = "raise"
                for k in [k for k, v in self.partners.items() if now - v["seen"] > 3]:
                    del self.partners[k]
                for v in self.partners.values():
                    while v["done"] and now - v["done"][0] > 2.0:
                        v["done"].popleft()
                shares = {k: round(self._rate_locked(k), 2) for k in self.partners}
            in_use = db.pool_stats()
            log_line({"type": "tick", "t": now, "budget": round(self.budget, 2), "before": round(before, 2),
                      "action": action, "p95_wait_ms": round(p95 * 1000, 2), "timeouts": timeouts,
                      "done": done, "aq_active_queues": self.active_queues,
                      "shares": shares, **in_use})

    def rate_for(self, partner):
        """Per-queue X-Aqueduct-Rps for this partner's Aquifer queue."""
        with self.lock:
            return self._rate_locked(partner)

    def _rate_locked(self, partner):
        parts = self.partners
        budget = self.budget
        if not parts:
            return budget
        if self.split == "even":
            # The skill's default: budget / X-Aqueduct-Active-Queues.
            return max(0.5, budget / max(1, self.active_queues or len(parts)))
        # Water-filling. A queue using under 80% of the rate we last gave it is
        # demand-limited: reserve what it uses plus 25% (+1) headroom. The rest
        # of the budget is split evenly among queues that are using their rate
        # (they're the backlogged ones). X-Aqueduct-Queue-Depth can't tell us
        # this: in v0.15.1 it is the node-wide queued count, not per queue.
        reserve, backlogged = {}, []
        for k, v in parts.items():
            used = len(v["done"]) / 2.0
            last = self.advertised.get(k)
            if last is not None and used < 0.8 * last:
                reserve[k] = used * 1.25 + 1.0
            else:
                backlogged.append(k)
        total_reserve = sum(reserve.values())
        if total_reserve > budget:
            # never hand out more than the budget in total
            reserve = {k: r * budget / total_reserve for k, r in reserve.items()}
            total_reserve = budget
        left = max(0.0, budget - total_reserve)
        share = left / len(backlogged) if backlogged else left
        if partner in reserve:
            return max(0.5, reserve[partner])
        return max(0.5, share)


def _int(h):
    try:
        return int(float(h))
    except (TypeError, ValueError):
        return None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, payload, extra=None):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"ok": True})
        if self.path == "/stats":
            with stats_lock:
                s = dict(stats)
            s.update(db.pool_stats())
            if PACER:
                s["budget"] = PACER.budget
                s["cuts"] = PACER.cuts
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t0 = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        partner = str(order.get("partner", "unknown"))
        aq = {h: self.headers.get("X-Aqueduct-" + h) for h in AQ_HEADERS if self.headers.get("X-Aqueduct-" + h) is not None}
        timed_out = False
        order_id = None
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            timed_out = True
        t1 = time.time()
        wait = max(0.0, (t1 - t0) - db.WRITE_SECONDS) if not timed_out else t1 - t0
        log_line({"type": "req", "t0": t0, "t1": t1, "partner": partner, "order": order.get("order"),
                  "ok": not timed_out, "wait_ms": round(wait * 1000, 2), "aq": aq})
        extra = {}
        if PACER:
            PACER.observe(partner, wait, timed_out, _int(aq.get("Queue-Depth")), _int(aq.get("Active-Queues")),
                          _int(aq.get("Upstream-Backlog")))
            rate = PACER.rate_for(partner)
            PACER.advertised[partner] = rate
            extra = {"X-Aqueduct-Rps": f"{rate:.2f}",
                     "X-Aqueduct-Max-Concurrent": str(db.POOL_SIZE)}
        if timed_out:
            with stats_lock:
                stats["unavailable"] += 1
            if PACER:
                # Busy, not broken: ask Aquifer to slow down and come back.
                extra["Retry-After"] = "1"
                return self._send(429, {"error": "database busy, slow down"}, extra)
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        self._send(201, {"order_id": order_id}, extra)


def _metrics_sampler():
    while True:
        time.sleep(1.0)
        with stats_lock:
            s = dict(stats)
        s.update(db.pool_stats())
        log_line({"type": "sample", "t": time.time(), **s})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log", default=None, help="pilot: per-request JSONL log")
    ap.add_argument("--pace", action="store_true", help="pilot: send X-Aqueduct-* pacing headers")
    ap.add_argument("--pace-start", type=float, default=40.0)
    ap.add_argument("--pace-ceiling", type=float, default=80.0)
    ap.add_argument("--pace-split", choices=["waterfill", "even"], default="waterfill")
    ap.add_argument("--pace-wait-ms", type=float, default=30.0)
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
        threading.Thread(target=_metrics_sampler, daemon=True).start()
    if a.pace:
        Pacer.WAIT_P95_LIMIT = a.pace_wait_ms / 1000
        PACER = Pacer(a.pace_start, 5.0, a.pace_ceiling, a.pace_split)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
