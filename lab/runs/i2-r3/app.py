"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

# Pilot instrumentation, all off unless a flag turns it on (see pilot/README.md).
ORDER_LOG = None        # --log-orders PATH: one JSON line per order attempt
PACER = None            # --pacing: send X-Aqueduct-* pacing headers
_log_lock = threading.Lock()


def _log(rec):
    if ORDER_LOG is None:
        return
    with _log_lock:
        ORDER_LOG.write(json.dumps(rec) + "\n")


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
            if PACER is not None:
                s["pacing"] = PACER.snapshot()
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
            extra = PACER.on_result(t0, time.time(), False, aq) if PACER else None
            _log({"t": t0, "dur": time.time() - t0, "status": 503, "partner": order.get("partner"),
                  "order": order.get("order"), "aq": aq, "sent": extra})
            return self._send(503, {"error": "database busy, try again"}, extra)
        with stats_lock:
            stats["ok"] += 1
        extra = PACER.on_result(t0, time.time(), True, aq) if PACER else None
        _log({"t": t0, "dur": time.time() - t0, "status": 201, "partner": order.get("partner"),
              "order": order.get("order"), "aq": aq, "sent": extra})
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log-orders", default=None, help="pilot: append one JSON line per order attempt")
    ap.add_argument("--pacing", action="store_true", help="pilot: send X-Aqueduct-* pacing headers (off by default)")
    ap.add_argument("--pacing-budget", type=float, default=72.0, help="starting total orders/s to advertise")
    a = ap.parse_args()
    if a.log_orders:
        ORDER_LOG = open(a.log_orders, "a", buffering=1)  # line-buffered so a kill loses nothing
    if a.pacing:
        import pacing
        PACER = pacing.Pacer(start_budget=a.pacing_budget)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
