"""AIMD pacing for Aquifer, driven by the database pool's checkout wait.

The budget is a total for the whole database; app.py divides it by
X-Aqueduct-Active-Queues before advertising it as X-Aqueduct-Rps.

Every second: if p95 checkout wait > SAT_WAIT or any checkout timed out, cut the
budget by CUT (at most once per second) and remember the rate as ssthresh.
Otherwise add STEP below ssthresh and STEP_SLOW above it.
"""
import threading, time

START, FLOOR, CEIL = 40.0, 5.0, 90.0
STEP, STEP_SLOW, CUT = 5.0, 1.0, 0.6
SAT_WAIT = 0.100   # seconds waiting for a pool connection


class Pacer:
    def __init__(self):
        self.lock = threading.Lock()
        self.budget = START
        self.ssthresh = CEIL
        self.waits = []
        self.failed = 0
        self.tick_at = time.time() + 1
        self.cuts = self.raises = 0

    def observe(self, wait, failed):
        with self.lock:
            self.waits.append(wait)
            self.failed += failed
            now = time.time()
            if now >= self.tick_at:
                self._tick()
                self.tick_at = now + 1

    def _tick(self):
        w = sorted(self.waits)
        p95 = w[int(0.95 * (len(w) - 1))] if w else 0.0
        if self.failed or p95 > SAT_WAIT:
            self.ssthresh = max(FLOOR, self.budget * 0.9)
            self.budget = max(FLOOR, self.budget * CUT)
            self.cuts += 1
        elif w:
            self.budget = min(CEIL, self.budget + (STEP if self.budget < self.ssthresh else STEP_SLOW))
            self.raises += 1
        self.waits, self.failed = [], 0

    def rate(self):
        with self.lock:
            return self.budget

    def snapshot(self):
        with self.lock:
            return {"budget": round(self.budget, 1), "ssthresh": round(self.ssthresh, 1),
                    "cuts": self.cuts, "raises": self.raises}
