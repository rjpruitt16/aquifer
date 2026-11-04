"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (all off unless asked for):
  --log PATH     append one JSON line per order attempt (time, partner, status,
                 pool wait, any X-Aqueduct-* headers Aquifer sent)
  --pacing       answer with X-Aqueduct-Rps (AIMD on database pool wait), so
                 Aquifer paces dispatch to what the pool can take
  Idempotency-Key request header: a repeat of a key already written returns
                 the original order_id instead of inserting a second order.
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "duplicate": 0}
ARGS = None
LOG = None
log_lock = threading.Lock()
seen_keys = {}            # idempotency key -> order_id
keys_lock = threading.Lock()


class Pacer:
    """AIMD on the time requests wait for a database connection.

    Budget is the total orders/s the pool should take; each Aquifer queue is
    told budget / X-Aqueduct-Active-Queues."""
    def __init__(self, start, floor, ceiling, step, cut, wait_slo):
        self.rate = start; self.floor = floor; self.ceiling = ceiling
        self.step = step; self.cut = cut; self.wait_slo = wait_slo
        self.ssthresh = ceiling
        self.waits = []; self.last_tick = time.time(); self.last_cut = 0
        self.lock = threading.Lock()

    def observe(self, wait, failed):
        with self.lock:
            self.waits.append(10.0 if failed else wait)
            now = time.time()
            if now - self.last_tick < 0.5:
                return
            w = sorted(self.waits); self.waits = []; self.last_tick = now
            p95 = w[min(len(w) - 1, int(0.95 * len(w)))]
            if p95 > self.wait_slo:
                if now - self.last_cut > 1.0:   # one cut per round trip
                    self.ssthresh = self.rate
                    self.rate = max(self.floor, self.rate * self.cut)
                    self.last_cut = now
            else:
                inc = self.step if self.rate < self.ssthresh else self.step / 5
                self.rate = min(self.ceiling, self.rate + inc)

    def per_queue(self, active):
        with self.lock:
            return self.rate / max(1, active)


PACER = None


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
                s["advertised_total_rps"] = round(PACER.rate, 1)
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def _log(self, rec):
        if LOG:
            line = json.dumps(rec) + "\n"
            with log_lock:
                LOG.write(line)

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t_in = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        aq = {k: v for k, v in self.headers.items() if k.lower().startswith("x-aqueduct-")}
        key = self.headers.get("Idempotency-Key")
        rec = {"t_in": t_in, "partner": order.get("partner"), "order": order.get("order"), "key": key, "aq": aq}
        extra = {}
        if PACER:
            active = int(aq.get("X-Aqueduct-Active-Queues") or 1)
            extra["X-Aqueduct-Rps"] = f"{PACER.per_queue(active):.2f}"
        if key:
            with keys_lock:
                prev = seen_keys.get(key)
            if prev is not None:
                with stats_lock:
                    stats["duplicate"] += 1
                rec.update(status=201, dup=True, t_done=time.time())
                self._log(rec)
                return self._send(201, {"order_id": prev, "duplicate": True}, extra)
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            wait = time.time() - t_in
            if PACER:
                PACER.observe(wait, True)
            with stats_lock:
                stats["unavailable"] += 1
            rec.update(status=503, t_done=time.time(), pool_wait=wait)
            self._log(rec)
            return self._send(503, {"error": "database busy, try again"}, extra)
        t_done = time.time()
        wait = max(0.0, t_done - t_in - db.WRITE_SECONDS)
        if PACER:
            PACER.observe(wait, False)
        if key:
            with keys_lock:
                seen_keys.setdefault(key, order_id)
        with stats_lock:
            stats["ok"] += 1
        rec.update(status=201, t_done=t_done, pool_wait=wait, order_id=order_id)
        self._log(rec)
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log", default=None)
    ap.add_argument("--pacing", action="store_true")
    ap.add_argument("--pacing-start", type=float, default=40.0)
    ap.add_argument("--pacing-ceiling", type=float, default=80.0)
    ap.add_argument("--wait-slo", type=float, default=0.05, help="p95 pool wait (s) above which to cut")
    a = ap.parse_args()
    ARGS = a
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    if a.pacing:
        PACER = Pacer(start=a.pacing_start, floor=5.0, ceiling=a.pacing_ceiling, step=5.0, cut=0.6, wait_slo=a.wait_slo)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
