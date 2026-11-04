"""orders-api: accepts orders from partner stores and writes them to the database.

Run:  python3 app.py --port 9000

Pilot options (all off by default, so plain `python3 app.py` behaves as before):
  --pilot-log FILE   write one CSV line per POST /orders (arrival, partner, order,
                     status, pool wait, and the X-Aqueduct-* headers Aquifer sent)
                     plus a pool sample every 0.5 s to FILE.pool
  --pacing           answer with Aqueduct pacing headers (X-Aqueduct-Rps) driven by
                     an AIMD loop on database pool wait, and answer a pool timeout
                     with 429 + Retry-After instead of 503
"""
import argparse, json, threading, time
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

import db

stats_lock = threading.Lock()
stats = {"ok": 0, "unavailable": 0}

PILOT_LOG = None       # open file, or None
PILOT_LOG_LOCK = threading.Lock()
PACER = None           # Pacer, or None

AQ_HEADERS = ("X-Aqueduct-Flow-Rate", "X-Aqueduct-Active-Queues",
              "X-Aqueduct-Upstream-Backlog", "X-Aqueduct-Queue-Depth",
              "X-Aqueduct-Node-Id")


class Pacer:
    """Backend-side AIMD on the database pool.

    Total budget starts at the benchmarked knee. Every 0.5 s: if any checkout
    waited longer than WAIT_CUT_S (the pool is queueing), cut the total by 0.7
    and remember the rate as ssthresh; otherwise add a step (bigger below
    ssthresh). Each response advertises the queue its share: queues that use
    less than an even split keep what they use, and the rest is split among
    the queues that want more (water-filling over the last 2 s of arrivals).
    """
    WAIT_CUT_S = 0.25
    FLOOR, CEIL = 10.0, 90.0

    def __init__(self, start=72.0):
        self.total = start
        self.ssthresh = start
        self.lock = threading.Lock()
        self.max_wait = 0.0
        self.arrivals = {}    # partner -> list of arrival times (last 2 s)
        self.shares = {}
        self.last_cut = 0.0
        threading.Thread(target=self._loop, daemon=True).start()

    def observe(self, partner, wait_s):
        now = time.time()
        with self.lock:
            self.max_wait = max(self.max_wait, wait_s)
            self.arrivals.setdefault(partner, []).append(now)

    def share(self, partner):
        with self.lock:
            return self.shares.get(partner, self.total)

    def _loop(self):
        while True:
            time.sleep(0.5)
            now = time.time()
            with self.lock:
                w, self.max_wait = self.max_wait, 0.0
                if w > self.WAIT_CUT_S:
                    if now - self.last_cut > 1.0:   # one cut per round trip
                        self.ssthresh = self.total
                        self.total = max(self.FLOOR, self.total * 0.7)
                        self.last_cut = now
                else:
                    step = 4.0 if self.total < self.ssthresh else 1.0
                    self.total = min(self.CEIL, self.total + step)
                use = {}
                for p, ts in list(self.arrivals.items()):
                    ts[:] = [t for t in ts if now - t < 2.0]
                    if ts:
                        use[p] = len(ts) / 2.0
                    else:
                        del self.arrivals[p]
                self.shares = water_fill(self.total, use)


def water_fill(total, use):
    """Split total so queues using less than an even share keep their use
    (plus 25% headroom), and the remainder goes evenly to the rest."""
    shares, left, rest = {}, total, dict(use)
    while rest:
        even = left / len(rest)
        small = {p: u for p, u in rest.items() if u * 1.25 < even}
        if not small:
            for p in rest:
                shares[p] = even
            break
        for p, u in small.items():
            shares[p] = u * 1.25
            left -= u * 1.25
            del rest[p]
    return shares


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
                s["pacer_total_rps"] = round(PACER.total, 1)
            return self._send(200, s)
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            return self._send(404, {"error": "not found"})
        arrived = time.time()
        length = int(self.headers.get("Content-Length", 0))
        order = json.loads(self.rfile.read(length) or b"{}")
        partner = str(order.get("partner", ""))
        status = 201
        extra = {}
        try:
            order_id = db.save_order(order)
        except db.PoolTimeout:
            with stats_lock:
                stats["unavailable"] += 1
            if PACER:
                status = 429
                extra = {"Retry-After": "1",
                         "X-Aqueduct-Rps": f"{PACER.share(partner):.2f}"}
                PACER.observe(partner, db.CHECKOUT_TIMEOUT)
                self._log(arrived, partner, order, status, db.CHECKOUT_TIMEOUT)
                return self._send(429, {"error": "database busy, slow down"}, extra)
            status = 503
            self._log(arrived, partner, order, status, db.CHECKOUT_TIMEOUT)
            return self._send(503, {"error": "database busy, try again"})
        wait = max(0.0, time.time() - arrived - db.WRITE_SECONDS)
        with stats_lock:
            stats["ok"] += 1
        if PACER:
            PACER.observe(partner, wait)
            extra = {"X-Aqueduct-Rps": f"{PACER.share(partner):.2f}"}
        self._log(arrived, partner, order, status, wait)
        self._send(201, {"order_id": order_id}, extra)

    def _log(self, arrived, partner, order, status, wait):
        if not PILOT_LOG:
            return
        h = [self.headers.get(k, "") for k in AQ_HEADERS]
        line = ",".join([f"{arrived:.4f}", partner, str(order.get("order", "")),
                         str(order.get("run", "")), str(status), f"{wait:.4f}"] + h)
        with PILOT_LOG_LOCK:
            PILOT_LOG.write(line + "\n")


def pool_sampler(path):
    with open(path, "w") as f:
        f.write("t,in_use,waiting,pacer_total\n")
        while True:
            s = db.pool_stats()
            pt = f"{PACER.total:.1f}" if PACER else ""
            f.write(f"{time.time():.3f},{s['pool_in_use']},{s['pool_waiting']},{pt}\n")
            f.flush()
            if PILOT_LOG:
                with PILOT_LOG_LOCK:
                    PILOT_LOG.flush()
            time.sleep(0.5)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--pilot-log", default=None)
    ap.add_argument("--pacing", action="store_true")
    a = ap.parse_args()
    if a.pacing:
        PACER = Pacer()
    if a.pilot_log:
        PILOT_LOG = open(a.pilot_log, "w", buffering=1 << 16)
        PILOT_LOG.write("arrived,partner,order,run,status,pool_wait_s,"
                        + ",".join(k.lower() for k in AQ_HEADERS) + "\n")
        threading.Thread(target=pool_sampler, args=(a.pilot_log + ".pool",), daemon=True).start()
    import signal, sys
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    ThreadingHTTPServer.daemon_threads = True
    try:
        ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
    finally:
        if PILOT_LOG:
            PILOT_LOG.flush()
