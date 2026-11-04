"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (all off unless flagged, except idempotency on an order "key"):
  --log FILE   one JSON line per POST /orders (partner, key, status, pool wait,
               and any X-Aqueduct-* headers Aquifer sent)
  --pace       answer with X-Aqueduct-Rps pacing headers computed from the
               database pool's checkout wait (AIMD), split max-min fairly
               across the partners' queues
"""
import argparse, collections, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "duplicate": 0}

# Orders are idempotent on order["key"] when the sender provides one, so an
# at-least-once delivery (Aquifer, or a partner retry after a lost 201) never
# writes the same order twice.
_seen_lock = threading.Lock()
_seen = {}

LOG = None
_log_lock = threading.Lock()
PACER = None

AQ_IN = ("Flow-Rate", "Queue-Depth", "Active-Queues", "Upstream-Backlog", "Total-Jobs", "Node-Id")


class Pacer:
    """AIMD on the pool's checkout wait, the database's own saturation signal.

    budget: total orders/s the database should receive from Aquifer.
    Each queue is advertised a max-min fair share of it: queues that aren't
    backlogged get what they're using plus headroom, backlogged queues split
    the rest.
    """
    def __init__(self, start, floor=10.0, cap=120.0):
        self.budget = start; self.floor = floor; self.cap = cap
        self.ssthresh = cap; self.hold_until = 0.0
        self.lock = threading.Lock()
        self.waits = collections.deque()      # (t, wait_s, timed_out)
        self.done = collections.deque()       # (t, partner)
        self.depth = {}                       # partner -> (t, queue depth)
        self.trace = []
        threading.Thread(target=self._loop, daemon=True).start()

    def observe(self, partner, wait, timed_out, depth):
        now = time.monotonic()
        with self.lock:
            self.waits.append((now, wait, timed_out))
            if not timed_out:
                self.done.append((now, partner))
            if depth is not None:
                self.depth[partner] = (now, depth)

    def _loop(self):
        while True:
            time.sleep(0.5)
            now = time.monotonic()
            with self.lock:
                while self.waits and self.waits[0][0] < now - 1.0:
                    self.waits.popleft()
                while self.done and self.done[0][0] < now - 2.0:
                    self.done.popleft()
                ws = sorted(w for _, w, _ in self.waits)
                tos = sum(1 for _, _, t in self.waits if t)
                p95 = ws[int(0.95 * (len(ws) - 1))] if ws else 0.0
                before = self.budget
                if tos or p95 > 0.15:
                    if now >= self.hold_until:
                        self.ssthresh = self.budget
                        self.budget = max(self.floor, self.budget * 0.7)
                        self.hold_until = now + 1.0
                elif p95 < 0.06 and ws:
                    step = 4.0 if self.budget < self.ssthresh else 1.0
                    self.budget = min(self.cap, self.budget + step)
                self.trace.append({"t": time.time(), "p95_wait": round(p95, 4), "timeouts": tos,
                                   "budget_before": round(before, 1), "budget": round(self.budget, 1)})

    def advertise(self, partner):
        # X-Aqueduct-Queue-Depth turned out to be the node-wide queued count,
        # not this queue's (pass B), so demand is judged from what each queue
        # actually used: a queue sending well under what it was last offered
        # is demand-limited ("light") and gets its use plus headroom; queues
        # using their whole offer split the rest of the budget.
        now = time.monotonic()
        with self.lock:
            rate = collections.Counter(p for _, p in self.done)
            active = [p for p, (t, _) in self.depth.items() if now - t < 3.0]
            if partner not in active:
                active.append(partner)
            last = getattr(self, "last_adv", {})
            self.last_adv = last
            use = {p: rate[p] / 2.0 for p in active}
            light = {p for p in active if p in last and use[p] < 0.8 * last[p]}
            light_alloc = {p: use[p] * 1.5 + 2.0 for p in light}
            heavy = [p for p in active if p not in light]
            rest = max(self.floor / max(1, len(active)), self.budget - sum(light_alloc.values()))
            adv = light_alloc[partner] if partner in light else rest / max(1, len(heavy))
            adv = max(1.0, adv)
            last[partner] = adv
            return adv


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
                s["pace_budget"] = PACER.budget
            return self._send(200, s)
        if self.path == "/pacer-trace" and PACER:
            return self._send(200, PACER.trace)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t_in = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        partner = order.get("partner", "?"); key = order.get("key")
        aq = {h: self.headers.get("X-Aqueduct-" + h) for h in AQ_IN if self.headers.get("X-Aqueduct-" + h) is not None}
        status, payload, wait, dup = 201, None, 0.0, False
        with _seen_lock:
            prior = _seen.get(key) if key else None
        if prior is not None:
            status, payload, dup = 201, {"order_id": prior, "duplicate": True}, True
            with stats_lock:
                stats["duplicate"] += 1
        else:
            try:
                order_id = db.save_order(order)
                wait = db.last_wait()
                if key:
                    with _seen_lock:
                        _seen[key] = order_id
                payload = {"order_id": order_id}
                with stats_lock:
                    stats["ok"] += 1
            except db.PoolTimeout:
                wait = db.last_wait()
                status, payload = 503, {"error": "database busy, try again"}
                with stats_lock:
                    stats["unavailable"] += 1
        extra, adv = {}, None
        if PACER:
            depth = aq.get("Queue-Depth")
            PACER.observe(partner, wait, status == 503, int(depth) if depth else 0)
            adv = PACER.advertise(partner)
            extra["X-Aqueduct-Rps"] = f"{adv:.2f}"
        if LOG:
            rec = {"t": t_in, "t_done": time.time(), "partner": partner, "key": key, "status": status,
                   "dup": dup, "wait": round(wait, 4), "aq": aq}
            if adv is not None:
                rec["adv_rps"] = round(adv, 2)
            line = json.dumps(rec) + "\n"
            with _log_lock:
                LOG.write(line)
        self._send(status, payload, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log")
    ap.add_argument("--pace", action="store_true")
    ap.add_argument("--pace-start", type=float, default=75.0)
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    if a.pace:
        PACER = Pacer(a.pace_start)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
