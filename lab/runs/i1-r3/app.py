"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot options (all off by default; without them the service behaves as before):
  --aquifer URL    POST /orders hands the order to Aquifer (one queue per partner)
                   and answers 202; Aquifer delivers it to POST /internal/orders.
  --pacing         /internal/orders answers with X-Aqueduct-Rps from an AIMD loop
                   on database connection-wait time.
  --log PATH       JSONL log of completed writes, X-Aqueduct-* request headers and
                   pool samples (every 100 ms), for the pilot report.
"""
import argparse, json, threading, time, urllib.request, urllib.error
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0, "enqueued": 0, "enqueue_refused": 0}

CFG = {"aquifer": None, "self": None, "pacing": False}

# --- pilot log -------------------------------------------------------------
_log_lock = threading.Lock()
_log_f = None


def log(rec):
    if _log_f is None:
        return
    line = json.dumps(rec) + "\n"
    with _log_lock:
        _log_f.write(line)


def sampler():
    while True:
        s = db.pool_stats()
        log({"k": "pool", "t": time.time(), "in_use": s["pool_in_use"], "waiting": s["pool_waiting"]})
        time.sleep(0.1)


# --- backend pacing (AIMD on connection-wait time) -------------------------
class Pacer:
    """Advertises a node-wide budget split across Aquifer's active queues.

    Saturation signal: p95 time waiting for a DB connection over the last 0.5 s.
    Healthy baseline is ~0; we cut when p95 wait exceeds 25 ms.
    """
    def __init__(self, start=60.0, floor=10.0, ceiling=80.0):
        self.rate = start; self.floor = floor; self.ceiling = ceiling
        self.ssthresh = ceiling; self.last_cut = 0.0; self.waits = []
        self.lock = threading.Lock(); self.window_start = time.time()

    def observe(self, wait_s):
        with self.lock:
            self.waits.append(wait_s)
            now = time.time()
            if now - self.window_start < 0.5:
                return
            w = sorted(self.waits); p95 = w[int(0.95 * (len(w) - 1))] if w else 0
            self.waits = []; self.window_start = now
            if p95 > 0.025:
                if now - self.last_cut > 1.0:   # wait a round trip before cutting again
                    self.ssthresh = self.rate
                    self.rate = max(self.floor, self.rate * 0.7); self.last_cut = now
            else:
                step = 5.0 if self.rate < self.ssthresh else 1.0
                self.rate = min(self.ceiling, self.rate + step)

    def per_queue(self, active_queues):
        with self.lock:
            return self.rate / max(1, active_queues)


pacer = Pacer()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, code, payload, headers=None):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
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

    def _read_order(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_POST(self):
        if self.path == "/orders":
            order = self._read_order()
            if CFG["aquifer"]:
                return self._enqueue(order)
            return self._write(order, internal=False)
        if self.path == "/internal/orders":
            return self._write(self._read_order(), internal=True)
        return self._send(404, {"error": "not found"})

    def _enqueue(self, order):
        partner = str(order.get("partner", "unknown"))
        job = {
            "user_id": partner,
            "idempotent_key": f"{partner}-{order.get('order')}",
            "url": CFG["self"] + "/internal/orders",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(order),
            "max_retries": -1,
        }
        req = urllib.request.Request(CFG["aquifer"] + "/jobs", data=json.dumps(job).encode(),
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                out = json.loads(r.read() or b"{}")
            with stats_lock:
                stats["enqueued"] += 1
            return self._send(202, {"job_id": out.get("job_id"), "status": "accepted"})
        except urllib.error.HTTPError as e:
            body = e.read()
            with stats_lock:
                stats["enqueue_refused"] += 1
            try:
                info = json.loads(body)
            except Exception:
                info = {"raw": body.decode(errors="replace")[:200]}
            log({"k": "refused", "t": time.time(), "status": e.code, "partner": partner, "body": info})
            hdr = {"Retry-After": e.headers.get("Retry-After", "5")}
            return self._send(429 if e.code == 429 else 503, {"error": "busy, try again", "detail": info}, hdr)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            log({"k": "refused", "t": time.time(), "status": 0, "partner": partner, "body": str(e)})
            return self._send(503, {"error": "queue unavailable"})

    def _write(self, order, internal):
        if internal and _log_f is not None:
            h = {k: v for k, v in self.headers.items() if k.lower().startswith("x-aqueduct-")}
            log({"k": "hdr", "t": time.time(), "partner": order.get("partner"), "h": h})
        t0 = time.time()
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            if internal:
                # Busy, not broken: 429 tells Aquifer to slow this queue down.
                return self._send(429, {"error": "database busy"}, {"Retry-After": "1"})
            return self._send(503, {"error": "database busy, try again"})
        t1 = time.time()
        wait = max(0.0, (t1 - t0) - db.WRITE_SECONDS)
        with stats_lock:
            stats["ok"] += 1
        log({"k": "done", "t": t1, "partner": order.get("partner"), "order": order.get("order"),
             "wait": round(wait, 4), "via": "aquifer" if internal else "direct"})
        hdr = {}
        if internal and CFG["pacing"]:
            pacer.observe(wait)
            aq = int(self.headers.get("X-Aqueduct-Active-Queues", "1") or 1)
            hdr["X-Aqueduct-Rps"] = f"{pacer.per_queue(aq):.2f}"
        self._send(201, {"order_id": order_id}, hdr)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--aquifer", default=None, help="Aquifer base URL; enables queued intake")
    ap.add_argument("--pacing", action="store_true", help="send X-Aqueduct-Rps from /internal/orders")
    ap.add_argument("--log", default=None, help="JSONL pilot log path")
    a = ap.parse_args()
    CFG.update(aquifer=a.aquifer, self=f"http://127.0.0.1:{a.port}", pacing=a.pacing)
    if a.log:
        _log_f = open(a.log, "a", buffering=1)
        threading.Thread(target=sampler, daemon=True).start()
    ThreadingHTTPServer.daemon_threads = True
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
