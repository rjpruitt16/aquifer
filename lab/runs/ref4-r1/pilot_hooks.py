"""Pilot-only hooks for the Aquifer evaluation. Both are off unless app.py is
started with --pilot-log and/or --pacing, so normal runs behave exactly as before.

- record(): one JSON line per order attempt (partner, order, status, checkout
  wait, and every X-Aqueduct-* header Aquifer sent), plus a pool sample every
  250 ms, so saturation can be measured.
- pacing_headers(): X-Aqueduct-Rps / X-Aqueduct-Max-Concurrent from an AIMD
  loop on the database pool's checkout wait (the saturation signal for a
  fixed connection pool). The total is split max-min fairly across the
  tenants Aquifer is currently sending: tenants using less than an even
  share keep what they use plus headroom, the rest is shared by the others.
"""
import json, threading, time

import db

_log = None
_log_lock = threading.Lock()
_pacing = False

CAPACITY_RPS = db.POOL_SIZE / db.WRITE_SECONDS     # 80 orders/s in theory
TICK = 0.5                                          # control loop period (s)
HEALTHY_WAIT = db.WRITE_SECONDS                     # p95 checkout wait we accept
FLOOR, CEIL = 10.0, CAPACITY_RPS

_ctl_lock = threading.Lock()
_total = 60.0            # advertised total across all queues
_ssthresh = CAPACITY_RPS
_last_cut = 0.0
_waits = []              # checkout waits this tick
_timeouts = 0
_served = {}             # tenant -> [timestamps of 201s] (last 2 s)
_active_queues = 1
_per_tenant = {}         # tenant -> advertised rps
_cur_tenant = threading.local()


def configure(log_path, pacing):
    global _log, _pacing
    _pacing = pacing
    if log_path:
        _log = open(log_path, "a", buffering=1 << 16)
        threading.Thread(target=_sampler, daemon=True).start()
    if pacing:
        threading.Thread(target=_controller, daemon=True).start()


def _write(rec):
    if _log is None:
        return
    line = json.dumps(rec, separators=(",", ":")) + "\n"
    with _log_lock:
        _log.write(line)


def _sampler():
    while True:
        s = db.pool_stats()
        _write({"type": "pool", "t": time.time(), "in_use": s["pool_in_use"], "waiting": s["pool_waiting"],
                "advertised_total": round(_total, 1) if _pacing else None})
        with _log_lock:
            _log.flush()
        time.sleep(0.25)


def record(headers, order, status, t0):
    global _timeouts, _active_queues
    now = time.time()
    wait = (now - t0) - (db.WRITE_SECONDS if status == 201 else 0)
    tenant = order.get("partner", "?")
    _cur_tenant.name = tenant
    aq = {k: v for k, v in headers.items() if k.lower().startswith(("x-aqueduct-", "x-aquifer-"))}
    if _pacing:
        with _ctl_lock:
            _waits.append(max(wait, 0.0))
            if status == 503:
                _timeouts += 1
            else:
                _served.setdefault(tenant, []).append(now)
            try:
                _active_queues = max(1, int(headers.get("X-Aqueduct-Active-Queues", _active_queues)))
            except ValueError:
                pass
    _write({"type": "req", "t": now, "t0": t0, "run": order.get("run"), "partner": tenant,
            "order": order.get("order"), "status": status, "wait": round(max(wait, 0.0), 4), "hdr": aq})


def pacing_headers():
    if not _pacing:
        return []
    tenant = getattr(_cur_tenant, "name", None)
    with _ctl_lock:
        rate = _per_tenant.get(tenant) or _total / max(1, _active_queues)
    return [("X-Aqueduct-Rps", f"{rate:.1f}"), ("X-Aqueduct-Max-Concurrent", str(db.POOL_SIZE))]


def _controller():
    global _total, _ssthresh, _last_cut, _waits, _timeouts, _per_tenant
    while True:
        time.sleep(TICK)
        now = time.time()
        with _ctl_lock:
            w = sorted(_waits); _waits = []
            t = _timeouts; _timeouts = 0
            p95 = w[int(0.95 * (len(w) - 1))] if w else 0.0
            if (t > 0 or p95 > 2 * HEALTHY_WAIT) and now - _last_cut > 1.0:
                _ssthresh = max(FLOOR, _total)
                _total = max(FLOOR, _total * 0.7)
                _last_cut = now
            elif w and p95 <= HEALTHY_WAIT:
                _total = min(CEIL, _total + (4.0 if _total < _ssthresh else 1.0))
            # max-min fair split across the tenants seen in the last 2 s
            rates = {}
            for k, ts in list(_served.items()):
                ts[:] = [x for x in ts if now - x < 2.0]
                if ts:
                    rates[k] = len(ts) / 2.0
                else:
                    del _served[k]
            n = max(len(rates), _active_queues, 1)
            share = _total / n
            alloc, rest, left = {}, [], _total
            for k, r in sorted(rates.items(), key=lambda kv: kv[1]):
                if r < 0.8 * share:
                    alloc[k] = min(share, r * 1.5 + 2.0)
                    left -= alloc[k]
                else:
                    rest.append(k)
            for k in rest:
                alloc[k] = max(1.0, left / len(rest))
            _per_tenant = alloc
