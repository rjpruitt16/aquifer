"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

# Pilot instrumentation: one JSON line per request when --log is given.
_log_lock = threading.Lock()
_log_file = None


def log_event(rec):
    if _log_file is None:
        return
    line = json.dumps(rec)
    with _log_lock:
        _log_file.write(line + "\n")

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}


class Pacer:
    """Pilot: tells Aquifer how fast to send, via X-Aqueduct-Rps (AIMD).

    Saturation signal: time spent waiting for a database connection. Healthy
    writes wait ~0 ms (benchmark knee ~70 orders/s); past it the wait climbs
    toward the 2 s checkout timeout and then 503s. Each second: if p95 wait >
    100 ms or any 503, cut the advertised rate x0.7 (at most once a second);
    otherwise add 10 below the last trouble point, 2 above it. Capped at the
    benchmarked knee. The rate is per Aquifer queue; Aquifer's configured
    budget (also the knee) caps the total across queues on the node.
    """

    def __init__(self, ceiling, enabled):
        self.enabled = enabled
        self.ceiling = ceiling
        self.rate = ceiling
        self.ssthresh = ceiling
        self.lock = threading.Lock()
        self.waits = []
        self.failed = 0
        self.win_start = time.time()
        self.last_cut = 0.0

    def observe(self, wait, failed):
        with self.lock:
            self.waits.append(wait)
            self.failed += failed
            now = time.time()
            if now - self.win_start < 1.0:
                return
            w = sorted(self.waits)
            p95 = w[int(0.95 * (len(w) - 1))]
            if self.failed or p95 > 0.1:
                if now - self.last_cut >= 1.0:
                    self.ssthresh = self.rate
                    self.rate = max(5.0, self.rate * 0.7)
                    self.last_cut = now
                    log_event({"t": now, "pacer": "cut", "rate": self.rate, "p95_wait": p95, "failed": self.failed})
            else:
                step = 10.0 if self.rate < self.ssthresh else 2.0
                new = min(self.ceiling, self.rate + step)
                if new != self.rate:
                    self.rate = new
                    log_event({"t": now, "pacer": "raise", "rate": self.rate, "p95_wait": p95})
            self.waits = []
            self.failed = 0
            self.win_start = now

    def headers(self):
        if not self.enabled:
            return {}
        with self.lock:
            return {"X-Aqueduct-Rps": f"{self.rate:.1f}", "X-Aqueduct-Max-Concurrent": str(db.POOL_SIZE)}


pacer = Pacer(70.0, False)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if self.path == "/orders":
            for k, v in pacer.headers().items():
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
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        t0 = time.time()
        aq = {k: v for k, v in self.headers.items() if k.lower().startswith("x-aqueduct-")}
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            pacer.observe(time.time() - t0, 1)
            log_event({"t": t0, "dur": time.time() - t0, "status": 503, "partner": order.get("partner"), "order": order.get("order"), "aq": aq})
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        pacer.observe(max(0.0, time.time() - t0 - db.WRITE_SECONDS), 0)
        log_event({"t": t0, "dur": time.time() - t0, "status": 201, "partner": order.get("partner"), "order": order.get("order"), "aq": aq})
        self._send(201, {"order_id": order_id})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log", default=None, help="pilot: write one JSON line per order request here")
    ap.add_argument("--pacing", action="store_true", help="pilot: send X-Aqueduct-Rps pacing headers")
    ap.add_argument("--pacing-ceiling", type=float, default=70.0, help="benchmarked knee, orders/s")
    a = ap.parse_args()
    pacer = Pacer(a.pacing_ceiling, a.pacing)
    if a.log:
        _log_file = open(a.log, "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
