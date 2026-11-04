"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot-only options (all off by default, so the default behaviour is unchanged):
  --log PATH   append one JSON line per /orders request (outcome, timing, and
               any X-Aqueduct-* headers Aquifer sent) for the pilot's evidence.
  --pace       speak the Aqueduct pacing protocol: answer "pool busy" with 429
               instead of 503, and advertise X-Aqueduct-Rps per queue from an
               AIMD loop on connection-checkout wait time.
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

LOG = None
log_lock = threading.Lock()
PACE = None


class Pacer:
    """AIMD on the database pool's checkout wait.

    The pool can do POOL_SIZE / WRITE_SECONDS writes a second (80 here). The
    total budget grows by STEP rps per healthy window and is cut x0.6 when the
    p95 checkout wait over the last window exceeds WAIT_SLO, at most once per
    window. Each queue is advertised budget / active queues (from
    X-Aqueduct-Active-Queues), so the total stays near the budget however many
    tenants are sending.
    """
    WINDOW = 1.0
    WAIT_SLO = 0.10
    STEP = 4.0
    FLOOR = 10.0
    CEIL = 80.0

    def __init__(self, start):
        self.lock = threading.Lock()
        self.budget = start
        self.ssthresh = self.CEIL
        self.waits = []
        self.window_start = time.time()
        self.cuts = 0
        self.raises = 0

    def observe(self, wait):
        with self.lock:
            self.waits.append(wait)
            now = time.time()
            if now - self.window_start < self.WINDOW:
                return
            w = sorted(self.waits)
            p95 = w[min(len(w) - 1, int(0.95 * len(w)))] if w else 0.0
            if p95 > self.WAIT_SLO:
                self.ssthresh = self.budget
                self.budget = max(self.FLOOR, self.budget * 0.6)
                self.cuts += 1
            else:
                step = self.STEP if self.budget < self.ssthresh else self.STEP / 4
                self.budget = min(self.CEIL, self.budget + step)
                self.raises += 1
            self.waits = []
            self.window_start = now

    def per_queue(self, active):
        with self.lock:
            return max(0.5, self.budget / max(1, active))


def log(rec):
    if LOG is None:
        return
    line = json.dumps(rec) + "\n"
    with log_lock:
        LOG.write(line)


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
            if PACE is not None:
                s.update({"pace_budget_rps": round(PACE.budget, 1), "pace_cuts": PACE.cuts, "pace_raises": PACE.raises})
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t_in = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        aq = {k: v for k, v in self.headers.items() if k.lower().startswith("x-aqueduct-")}
        try:
            order_id, wait = db.save_order(order, return_wait=True)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            if PACE is not None:
                PACE.observe(db.CHECKOUT_TIMEOUT)
            log({"t": t_in, "t_done": time.time(), "partner": order.get("partner"), "order": order.get("order"),
                 "status": 429 if PACE else 503, "aq": aq})
            if PACE is not None:
                active = int(aq.get("X-Aqueduct-Active-Queues", "1") or 1)
                return self._send(429, {"error": "database busy, slow down"},
                                  {"Retry-After": "1", "X-Aqueduct-Rps": f"{PACE.per_queue(active):.2f}"})
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        extra = None
        if PACE is not None:
            PACE.observe(wait)
            active = int(aq.get("X-Aqueduct-Active-Queues", "1") or 1)
            extra = {"X-Aqueduct-Rps": f"{PACE.per_queue(active):.2f}"}
        log({"t": t_in, "t_done": time.time(), "partner": order.get("partner"), "order": order.get("order"),
             "status": 201, "wait": round(wait, 4), "aq": aq})
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log")
    ap.add_argument("--pace", action="store_true")
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    if a.pace:
        PACE = Pacer(start=40.0)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
