"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot-only options (all off by default, so plain `python3 app.py` behaves as before):
  --log PATH   append one JSON line per POST /orders: time, partner, order, status,
               DB checkout wait, and any X-Aqueduct-* headers Aquifer sent.
  --pace       return X-Aqueduct-Rps pacing headers from an AIMD loop on the
               database pool's checkout wait (see pacing.py).
Orders carrying an "idem" field are de-duplicated: a repeat of an already-written
order returns the original order_id without writing again (Aquifer delivers at
least once).
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "duplicates": 0}
seen = {}            # idem key -> order_id
seen_lock = threading.Lock()
LOG = None
log_lock = threading.Lock()
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
                s["pace"] = PACER.snapshot()
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def _log(self, order, status, wait):
        if LOG is None:
            return
        rec = {"t": time.time(), "partner": order.get("partner"), "order": order.get("order"),
               "t0": order.get("t0"), "status": status, "wait": round(wait, 4)}
        for k, v in self.headers.items():
            if k.lower().startswith("x-aqueduct-"):
                rec[k.lower()[11:]] = v
        with log_lock:
            LOG.write(json.dumps(rec) + "\n")

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        idem = order.get("idem")
        if idem is not None:
            with seen_lock:
                prior = seen.get(idem)
            if prior is not None:
                with stats_lock:
                    stats["duplicates"] += 1
                self._log(order, 200, 0.0)
                return self._send(201, {"order_id": prior, "duplicate": True}, self._pace_headers())
        t = time.time()
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            wait = time.time() - t
            if PACER:
                PACER.observe(wait, failed=True)
            with stats_lock:
                stats["unavailable"] += 1
            self._log(order, 503, wait)
            return self._send(503, {"error": "database busy, try again"}, self._pace_headers())
        wait = time.time() - t - db.WRITE_SECONDS
        if PACER:
            PACER.observe(wait, failed=False)
        if idem is not None:
            with seen_lock:
                seen[idem] = order_id
        with stats_lock:
            stats["ok"] += 1
        self._log(order, 201, wait)
        self._send(201, {"order_id": order_id}, self._pace_headers())

    def _pace_headers(self):
        if not PACER:
            return None
        queues = self.headers.get("X-Aqueduct-Active-Queues") or "1"
        try:
            queues = max(1, int(queues))
        except ValueError:
            queues = 1
        return {"X-Aqueduct-Rps": f"{PACER.rate() / queues:.2f}"}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--log")
    ap.add_argument("--pace", action="store_true")
    a = ap.parse_args()
    if a.log:
        LOG = open(a.log, "a", buffering=1)
    if a.pace:
        import pacing
        PACER = pacing.Pacer()
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
