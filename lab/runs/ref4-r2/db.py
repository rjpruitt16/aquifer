"""Database access. The managed Postgres plan we're on allows a small number of
connections, so we keep a fixed pool. (Simulated here: each write holds a
connection for about as long as our real insert does.)"""
import itertools, threading, time

POOL_SIZE = 4          # connections our database plan allows
CHECKOUT_TIMEOUT = 2.0 # seconds to wait for a free connection
WRITE_SECONDS = 0.05   # time one order insert holds a connection

_pool = threading.BoundedSemaphore(POOL_SIZE)
_lock = threading.Lock()
_in_use = 0
_waiting = 0
_ids = itertools.count(1)
_seen = {}        # idempotency: (partner, order) -> order_id, like a unique index
_dupes = 0


class PoolTimeout(Exception):
    pass


def save_order(order):
    """Returns (order_id, duplicate, pool_wait_seconds)."""
    global _in_use, _waiting, _dupes
    t0 = time.monotonic()
    with _lock:
        _waiting += 1
    got = _pool.acquire(timeout=CHECKOUT_TIMEOUT)
    with _lock:
        _waiting -= 1
        if got:
            _in_use += 1
    wait = time.monotonic() - t0
    if not got:
        raise PoolTimeout()
    try:
        time.sleep(WRITE_SECONDS)  # INSERT ... ON CONFLICT DO NOTHING still costs the round trip
        key = (order.get("partner"), order.get("order"))
        with _lock:
            if key in _seen:
                _dupes += 1
                return _seen[key], True, wait
            oid = next(_ids)
            _seen[key] = oid
        return oid, False, wait
    finally:
        with _lock:
            _in_use -= 1
        _pool.release()


def pool_stats():
    with _lock:
        return {"pool_size": POOL_SIZE, "pool_in_use": _in_use, "pool_waiting": _waiting,
                "duplicates": _dupes}
