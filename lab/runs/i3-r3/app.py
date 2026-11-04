"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

# Pilot only: optional per-request log (--log PATH). Off by default.
req_log = None
log_lock = threading.Lock()


def log_request(order, code, headers):
    if req_log is None:
        return
    aq = {k: v for k, v in headers.items() if k.lower().startswith("x-aqueduct-")}
    line = json.dumps({"t": time.time(), "partner": order.get("partner"), "order": order.get("order"),
                       "run": order.get("run"), "status": code, "aq": aq})
    with log_lock:
        req_log.write(line + "\n")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
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
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            log_request(order, 503, self.headers)
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        log_request(order, 201, self.headers)
        self._send(201, {"order_id": order_id})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log", default=None, help="pilot: append one JSON line per order request")
    a = ap.parse_args()
    if a.log:
        req_log = open(a.log, "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
