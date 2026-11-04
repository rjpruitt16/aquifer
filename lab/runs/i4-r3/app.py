"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db
import pacing

if pacing.ENABLED:
    db.on_checkout = pacing.record_wait

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, payload, extra=None):
        body = json.dumps(payload).encode()
        self.send_response(code)
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"ok": True})
        if self.path == "/stats":
            with stats_lock:
                s = dict(stats)
            s.update(db.pool_stats())
            if pacing.ENABLED:
                s["pacing"] = dict(pacing.state)
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        via_aquifer = pacing.ENABLED and self.headers.get("X-Aqueduct-Node-Id") is not None
        extra = {}
        if via_aquifer:
            extra["X-Aqueduct-Rps"] = str(pacing.advertise(str(order.get("partner")), dict(self.headers.items())))
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            if via_aquifer:  # "busy, slow down" for Aquifer; direct callers keep today's 503
                extra["Retry-After"] = "1"
                return self._send(429, {"error": "database busy, slow down"}, extra)
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        self._send(201, {"order_id": order_id}, extra)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    a = ap.parse_args()
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
