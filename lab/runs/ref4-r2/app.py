"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

# Pilot instrumentation: one JSON line per POST /orders, with any X-Aqueduct-* /
# X-Aquifer-* request headers, so both pilot passes can be measured the same way.
log_lock = threading.Lock()
log_file = None


def log_event(ev):
    if log_file is None:
        return
    line = json.dumps(ev) + "\n"
    with log_lock:
        log_file.write(line)

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}


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
        t_in = time.time()
        aq = {k.lower(): v for k, v in self.headers.items()
              if k.lower().startswith(("x-aqueduct-", "x-aquifer-"))}
        ev = {"t_in": t_in, "partner": order.get("partner"), "order": order.get("order"),
              "sent_at": order.get("sent_at"), "hdr": aq}
        try:
            order_id, dup, wait = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            ev.update(status=503, t_out=time.time())
            log_event(ev)
            return self._send(503, {"error": "database busy, try again"})
        with stats_lock:
            stats["ok"] += 1
        ev.update(status=201, t_out=time.time(), dup=dup, pool_wait=round(wait, 4))
        log_event(ev)
        self._send(201, {"order_id": order_id})


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log", help="pilot: append one JSON line per order request here")
    a = ap.parse_args()
    if a.log:
        log_file = open(a.log, "a", buffering=1)
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
