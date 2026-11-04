"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (all optional; with no flags the service behaves as before):
  --log FILE   append one JSON line per POST /orders (time, partner, order,
               status, X-Aqueduct-* headers seen) for the pilot's evidence.
  --pacing     answer "database busy" with 429 + Retry-After instead of 503,
               and advertise X-Aqueduct-Rps from a small AIMD loop on pool
               wait time (see Pacer below).
  Idempotency-Key header: a repeated key returns the first order_id instead
               of writing the order twice (Aquifer delivers at-least-once).
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "duplicate": 0}

LOG = None
log_lock = threading.Lock()
PACING = False

seen_lock = threading.Lock()
seen = {}  # Idempotency-Key -> order_id (in Postgres: a unique constraint)


class Pacer:
    """AIMD on the backend's own saturation signal: time spent waiting for a
    pool connection. Budget is the total for all Aquifer queues; each response
    advertises budget / active queues."""

    def __init__(self, start=60.0, floor=10.0, ceiling=80.0, step=4.0):
        self.rate = start; self.floor = floor; self.ceiling = ceiling; self.step = step
        self.ssthresh = ceiling; self.lock = threading.Lock()
        self.waits = []; self.last_cut = 0.0; self.last_tick = time.time()

    def observe(self, wait_s, failed):
        with self.lock:
            self.waits.append(1.0 if failed else wait_s)
            now = time.time()
            if now - self.last_tick < 0.5:
                return
            w = sorted(self.waits); self.waits = []; self.last_tick = now
            p95 = w[int(0.95 * (len(w) - 1))] if w else 0.0
            if p95 > 0.25 and now - self.last_cut > 1.0:
                self.ssthresh = self.rate
                self.rate = max(self.floor, self.rate * 0.7); self.last_cut = now
            elif p95 < 0.1:
                inc = self.step if self.rate < self.ssthresh else self.step / 4
                self.rate = min(self.ceiling, self.rate + inc)

    def per_queue(self, active):
        with self.lock:
            return max(1.0, self.rate / max(1, active))


pacer = Pacer()


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
            if PACING:
                s["advertised_total_rps"] = round(pacer.rate, 1)
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def _log(self, order, code, t0, wait_s):
        if LOG is None:
            return
        aq = {k: v for k, v in self.headers.items() if k.lower().startswith("x-aqueduct-")}
        rec = {"t": round(t0, 4), "dur": round(time.time() - t0, 4), "wait": round(wait_s, 4),
               "partner": order.get("partner"), "order": order.get("order"), "code": code, "aq": aq}
        with log_lock:
            LOG.write(json.dumps(rec) + "\n")

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t0 = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        key = self.headers.get("Idempotency-Key")
        if key:
            with seen_lock:
                prev = seen.get(key)
            if prev is not None:
                with stats_lock:
                    stats["duplicate"] += 1
                self._log(order, 200, t0, 0.0)
                return self._send(200, {"order_id": prev, "duplicate": True})
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            wait = time.time() - t0
            self._log(order, 429 if PACING else 503, t0, wait)
            if PACING:
                pacer.observe(wait, True)
                active = int(self.headers.get("X-Aqueduct-Active-Queues", "1") or 1)
                return self._send(429, {"error": "database busy, slow down"},
                                  {"Retry-After": "1", "X-Aqueduct-Rps": f"{pacer.per_queue(active):.1f}"})
            return self._send(503, {"error": "database busy, try again"})
        wait = max(0.0, time.time() - t0 - db.WRITE_SECONDS)
        if key:
            with seen_lock:
                seen.setdefault(key, order_id)
        with stats_lock:
            stats["ok"] += 1
        self._log(order, 201, t0, wait)
        extra = None
        if PACING:
            pacer.observe(wait, False)
            active = int(self.headers.get("X-Aqueduct-Active-Queues", "1") or 1)
            extra = {"X-Aqueduct-Rps": f"{pacer.per_queue(active):.1f}"}
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log")
    ap.add_argument("--pacing", action="store_true")
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    PACING = a.pacing
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
