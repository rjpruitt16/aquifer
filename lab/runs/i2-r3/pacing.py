"""Pilot: orders-api tells Aquifer how fast to send (X-Aqueduct-* response headers).

AIMD on the database pool, the real bottleneck:
- every second, look at how long order requests took (pool wait + insert);
- a pool timeout, or p95 above 2x the healthy insert time, cuts the total budget x0.7
  (at most once every 2 s) and remembers it as the trouble point (ssthresh);
- otherwise grow +5/s below the trouble point, +1/s above it.
The total is split across the Aquifer queues sending right now
(X-Aqueduct-Active-Queues, summed per X-Aqueduct-Node-Id), so N queues don't each get
the whole budget. Off unless app.py runs with --pacing.
"""
import threading, time

import db


class Pacer:
    def __init__(self, start_budget=72.0, floor=5.0, cap=100.0):
        self.budget = start_budget; self.floor = floor; self.cap = cap
        self.ssthresh = cap; self.lock = threading.Lock()
        self.window = []; self.timeouts = 0; self.tick = time.time(); self.last_cut = 0.0
        self.healthy_s = None; self.nodes = {}; self.cuts = 0; self.raises = 0

    def _maybe_tick(self, now):
        if now - self.tick < 1.0:
            return
        self.tick = now
        durs = sorted(self.window); self.window = []
        timeouts, self.timeouts = self.timeouts, 0
        if not durs and not timeouts:
            return
        if durs:
            p50 = durs[len(durs) // 2]
            self.healthy_s = p50 if self.healthy_s is None else min(self.healthy_s * 1.01, max(p50, db.WRITE_SECONDS))
        p95 = durs[int(0.95 * (len(durs) - 1))] if durs else 99
        trouble = timeouts > 0 or p95 > 2 * (self.healthy_s or db.WRITE_SECONDS)
        if trouble:
            if now - self.last_cut >= 2.0:
                self.ssthresh = max(self.floor, self.budget * 0.9)
                self.budget = max(self.floor, self.budget * 0.7)
                self.last_cut = now; self.cuts += 1
        else:
            self.budget = min(self.cap, self.budget + (5 if self.budget < self.ssthresh else 1)); self.raises += 1

    def on_result(self, t_start, t_end, ok, aq):
        now = t_end
        with self.lock:
            if ok:
                self.window.append(t_end - t_start)
            else:
                self.timeouts += 1
            node = aq.get("X-Aqueduct-Node-Id")
            if node:
                self.nodes[node] = (int(aq.get("X-Aqueduct-Active-Queues", "1") or 1), now)
            self._maybe_tick(now)
            senders = sum(n for n, seen in self.nodes.values() if now - seen < 3) or 1
            per_queue = max(0.5, self.budget / senders)
            conc = max(1, db.POOL_SIZE // senders)
        h = {"X-Aqueduct-Rps": f"{per_queue:.2f}", "X-Aqueduct-Max-Concurrent": str(conc)}
        if not ok:
            h["Retry-After"] = "1"
        return h

    def snapshot(self):
        with self.lock:
            return {"budget": round(self.budget, 1), "ssthresh": round(self.ssthresh, 1), "cuts": self.cuts,
                    "raises": self.raises, "healthy_ms": round(1000 * (self.healthy_s or 0), 1)}
