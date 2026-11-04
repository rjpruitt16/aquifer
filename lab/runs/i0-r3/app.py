"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (all off by default, so plain `python3 app.py` behaves as before):
  --order-log PATH   append one CSV line per POST /orders (time, partner, order,
                     status, save seconds, X-Aqueduct-* request headers)
  --pacing           answer with X-Aqueduct-* pacing headers driven by an AIMD
                     loop on database connection-checkout wait, and answer a pool
                     timeout with 429 + Retry-After instead of 503
  --pacing-share     divide (advertise budget / active queues, the default) or
                     full (advertise the whole budget to every queue)
  --slow-window S:E:W  test only: from S to E seconds after the first order, each
                     insert holds its connection W seconds instead of 0.05 (a DB slowdown)
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

ORDER_LOG = None
order_log_lock = threading.Lock()
PACER = None


class Pacer:
    """AIMD on the database's own saturation signal.

    Signal: time a request waited for a pool connection (save time minus the
    insert itself), p95 over each 1 s window, plus any checkout timeout.
    Healthy: grow fast below the last trouble point (ssthresh), slowly above.
    Saturated: cut x0.7, hold two windows before cutting again.
    """

    def __init__(self, start, floor, ceiling, max_backlog, share):
        self.rate = start
        self.floor = floor
        self.ceiling = ceiling
        self.ssthresh = ceiling
        self.max_backlog = max_backlog
        self.share = share
        self.lock = threading.Lock()
        self.waits = []
        self.timeouts = 0
        self.hold = 0
        self.active_queues = {}  # node id -> last X-Aqueduct-Active-Queues
        self.trace = []
        threading.Thread(target=self._loop, daemon=True).start()

    def observe(self, wait_s, timed_out, node, active):
        with self.lock:
            if timed_out:
                self.timeouts += 1
            else:
                self.waits.append(wait_s)
            if node and active is not None:
                self.active_queues[node] = active

    def _loop(self):
        while True:
            time.sleep(1.0)
            with self.lock:
                w = sorted(self.waits); to = self.timeouts
                self.waits = []; self.timeouts = 0
                p95 = w[int(0.95 * (len(w) - 1))] if w else 0.0
                saturated = to > 0 or p95 > 0.10
                action = "hold"
                if saturated and self.hold == 0:
                    self.ssthresh = self.rate
                    self.rate = max(self.floor, self.rate * 0.7)
                    self.hold = 2
                    action = "cut"
                elif self.hold > 0:
                    self.hold -= 1
                elif not saturated and len(w) > 0:
                    step = 8.0 if self.rate < self.ssthresh * 0.9 else 2.0
                    self.rate = min(self.ceiling, self.rate + step)
                    action = "raise"
                self.trace.append({"t": round(time.time(), 3), "rate": round(self.rate, 2), "p95_wait": round(p95, 4),
                                   "n": len(w), "timeouts": to, "action": action,
                                   "active_queues": sum(self.active_queues.values())})

    def headers(self):
        with self.lock:
            senders = max(1, sum(self.active_queues.values())) if self.share == "divide" else 1
            per_queue = max(0.5, self.rate / senders)
            return {"X-Aqueduct-Rps": f"{per_queue:.2f}",
                    "X-Aqueduct-Max-Backlog": str(self.max_backlog)}


SLOW = None  # (start_s, end_s, write_s)
_first_order = []


def slow_tick():
    if not _first_order:
        _first_order.append(time.time())
    el = time.time() - _first_order[0]
    db.WRITE_SECONDS = SLOW[2] if SLOW[0] <= el < SLOW[1] else 0.05


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
            return self._send(200, s)
        if self.path == "/pacer" and PACER:
            with PACER.lock:
                return self._send(200, {"rate": PACER.rate, "ssthresh": PACER.ssthresh, "trace": PACER.trace})
        self._send(404, {"error": "not found"})

    def _log(self, order, status, save_s):
        if not ORDER_LOG:
            return
        h = self.headers
        line = ",".join(str(x) for x in (
            f"{time.time():.4f}", order.get("partner", ""), order.get("order", ""), status, f"{save_s:.4f}",
            h.get("X-Aqueduct-Node-Id", ""), h.get("X-Aqueduct-Flow-Rate", ""), h.get("X-Aqueduct-Queue-Depth", ""),
            h.get("X-Aqueduct-Active-Queues", ""), h.get("X-Aqueduct-Upstream-Backlog", ""),
            h.get("X-Aqueduct-Total-Jobs", ""), order.get("run", "")))
        with order_log_lock:
            ORDER_LOG.write(line + "\n")

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        if SLOW:
            slow_tick()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        t0 = time.time()
        node = self.headers.get("X-Aqueduct-Node-Id")
        try:
            active = int(self.headers.get("X-Aqueduct-Active-Queues", ""))
        except ValueError:
            active = None
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            dt = time.time() - t0
            with stats_lock:
                stats["unavailable"] += 1
            self._log(order, 503 if not PACER else 429, dt)
            if PACER:
                PACER.observe(dt, True, node, active)
                h = PACER.headers(); h["Retry-After"] = "1"
                return self._send(429, {"error": "database busy, slow down"}, h)
            return self._send(503, {"error": "database busy, try again"})
        dt = time.time() - t0
        with stats_lock:
            stats["ok"] += 1
        self._log(order, 201, dt)
        extra = None
        if PACER:
            PACER.observe(max(0.0, dt - db.WRITE_SECONDS), False, node, active)  # wait = save time minus the insert
            extra = PACER.headers()
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--order-log")
    ap.add_argument("--pacing", action="store_true")
    ap.add_argument("--pacing-start", type=float, default=40.0)
    ap.add_argument("--pacing-ceiling", type=float, default=80.0)
    ap.add_argument("--pacing-max-backlog", type=int, default=20000)
    ap.add_argument("--pacing-share", choices=["divide", "full"], default="divide")
    ap.add_argument("--slow-window")
    a = ap.parse_args()
    if a.slow_window:
        SLOW = tuple(float(x) for x in a.slow_window.split(":"))
    if a.order_log:
        ORDER_LOG = open(a.order_log, "a", buffering=1)
    if a.pacing:
        PACER = Pacer(a.pacing_start, 2.0, a.pacing_ceiling, a.pacing_max_backlog, a.pacing_share)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
