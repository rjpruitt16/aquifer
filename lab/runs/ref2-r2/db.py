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


class PoolTimeout(Exception):
    pass


def save_order(order):
    global _in_use, _waiting
    with _lock:
        _waiting += 1
    got = _pool.acquire(timeout=CHECKOUT_TIMEOUT)
    with _lock:
        _waiting -= 1
        if got:
            _in_use += 1
    if not got:
        raise PoolTimeout()
    try:
        time.sleep(WRITE_SECONDS)
        return next(_ids)
    finally:
        with _lock:
            _in_use -= 1
        _pool.release()


def save_order_timed(order):
    """save_order, also returning how long the connection checkout waited (pilot logging)."""
    t = time.time()
    global _in_use, _waiting
    with _lock:
        _waiting += 1
    got = _pool.acquire(timeout=CHECKOUT_TIMEOUT)
    waited = time.time() - t
    with _lock:
        _waiting -= 1
        if got:
            _in_use += 1
    if not got:
        raise PoolTimeout()
    try:
        time.sleep(WRITE_SECONDS)
        return next(_ids), waited
    finally:
        with _lock:
            _in_use -= 1
        _pool.release()


def pool_stats():
    with _lock:
        return {"pool_size": POOL_SIZE, "pool_in_use": _in_use, "pool_waiting": _waiting}
