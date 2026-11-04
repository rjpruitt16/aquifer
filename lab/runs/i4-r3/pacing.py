"""Backend-side pacing for Aquifer (X-Aqueduct-* headers).

Off unless PACING=on. The saturation signal is the database pool: how long a
write waits for a free connection. An AIMD loop keeps a total budget for all
Aquifer queues together; each queue is advertised what the others aren't using
(water-filling), never less than an even share, so small shops always have room
and the big partner gets the rest.
"""
import json, os, threading, time

ENABLED = os.environ.get("PACING", "off") == "on"
TICK = 0.5                 # seconds per control step
CEILING = 65.0             # same as the YAML rps ceiling (knee ~72-80 from pilot/bench.py)
FLOOR = 10.0
WAIT_SLO = float(os.environ.get("PACING_WAIT_SLO_MS", "10")) / 1000  # p95 connection wait above this = saturated
STEP_FAST, STEP_SLOW, CUT = 5.0, 1.0, 0.7

_lock = threading.Lock()
_waits = []
_used = {}                 # queue -> [timestamps of recent requests]
state = {"budget": 60.0, "ssthresh": CEILING, "cuts": 0, "raises": 0, "hold": 0, "last_p95_ms": 0.0}
_log = open(os.environ.get("PACING_LOG", os.devnull), "a", buffering=1)


def record_wait(sec):
    with _lock:
        _waits.append(sec)


def _loop():
    while True:
        time.sleep(TICK)
        with _lock:
            w = sorted(_waits); _waits.clear()
            p95 = w[int(0.95 * (len(w) - 1))] if w else 0.0
            state["last_p95_ms"] = round(p95 * 1000, 1)
            if state["hold"] > 0:
                state["hold"] -= 1
            elif p95 > WAIT_SLO:
                state["ssthresh"] = state["budget"]
                state["budget"] = max(FLOOR, state["budget"] * CUT)
                state["cuts"] += 1; state["hold"] = 2   # wait about a round trip before cutting again
            elif w:
                step = STEP_FAST if state["budget"] < state["ssthresh"] else STEP_SLOW
                state["budget"] = min(CEILING, state["budget"] + step); state["raises"] += 1


def advertise(queue, headers):
    """Rate for this queue's next response, and log what Aquifer told us."""
    now = time.time()
    with _lock:
        ts = _used.setdefault(queue, []); ts.append(now)
        cutoff = now - 2.0
        for q in list(_used):
            _used[q] = [t for t in _used[q] if t > cutoff]
            if not _used[q]:
                del _used[q]
        active = max(1, int(headers.get("X-Aqueduct-Active-Queues") or len(_used) or 1), len(_used))
        others = sum(len(v) for q, v in _used.items() if q != queue) / 2.0
        rate = max(state["budget"] / active, state["budget"] - others)
    _log.write(json.dumps({"t": now, "queue": queue, "rate": round(rate, 1), "budget": round(state["budget"], 1),
                           **{k: v for k, v in headers.items() if k.lower().startswith("x-aqueduct-")}}) + "\n")
    return round(rate, 1)


if ENABLED:
    threading.Thread(target=_loop, daemon=True).start()
