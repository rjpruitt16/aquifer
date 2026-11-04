"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot additions (all off unless the flag is given):
  --log FILE   write one JSON line per POST /orders (status, timing, X-Aqueduct-* headers)
  --pacing     answer with X-Aqueduct-Rps / X-Aqueduct-Max-Concurrent from an AIMD loop
               on database pool checkout wait (see pacing.py)
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "requests": 0}

LOG = None          # open file when --log is given
log_lock = threading.Lock()
PACER = None        # pacing.Pacer when --pacing is given

AQ_HEADERS = ("Flow-Rate", "Queue-Depth", "Active-Queues", "Upstream-Backlog", "Total-Jobs", "Node-Id")


def aq_header(headers, name):
    v = headers.get("X-Aqueduct-" + name)
    return v if v is not None else headers.get("X-Aquifer-" + name)


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
                s["pacing"] = PACER.snapshot()
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        t0 = time.time()
        with stats_lock:
            stats["requests"] += 1
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        aq = {k: aq_header(self.headers, k) for k in AQ_HEADERS}
        wait = None
        try:
            order_id, wait = db.save_order_timed(order)
            code, payload = 201, {"order_id": order_id}
        except db.PoolTimeout:
            wait = db.CHECKOUT_TIMEOUT
            code, payload = 503, {"error": "database busy, try again"}
        with stats_lock:
            stats["ok" if code == 201 else "unavailable"] += 1
        extra = PACER.observe(order, aq, wait, code) if PACER else None
        self._send(code, payload, extra)
        if LOG:
            rec = {"t": round(time.time(), 4), "t0": round(t0, 4), "run": order.get("run"),
                   "partner": order.get("partner"), "order": order.get("order"),
                   "status": code, "wait": round(wait, 4), "aq": {k: v for k, v in aq.items() if v is not None}}
            if extra:
                rec["sent"] = extra
            line = json.dumps(rec) + "\n"
            with log_lock:
                LOG.write(line)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log")
    ap.add_argument("--pacing", action="store_true")
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    if a.pacing:
        import pacing
        PACER = pacing.Pacer(db.POOL_SIZE)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
