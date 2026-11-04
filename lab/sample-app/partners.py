"""Replays a burst like the one our partner stores send during a sale.

Each partner store sends orders as fast as its schedule says, and retries a
failed order right away (up to 5 attempts), the way their integration does.

  python3 partners.py --target http://127.0.0.1:9000 --seconds 30
"""
import argparse, json, random, threading, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

PARTNERS = {"bigbox": 150, "corner-shop": 10, "kiosk": 5, "deli": 5}  # orders/s during the sale


def send(target, partner, n):
    data = json.dumps({"partner": partner, "order": n, "items": random.randint(1, 5)}).encode()
    start = time.time(); attempts = 0
    while attempts < 5:
        attempts += 1
        req = urllib.request.Request(f"{target}/orders", data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                if r.status < 300:
                    return partner, True, attempts, time.time() - start
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.1)
    return partner, False, attempts, time.time() - start


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="http://127.0.0.1:9000")
    ap.add_argument("--seconds", type=int, default=30)
    a = ap.parse_args()
    results = []; futures = []
    with ThreadPoolExecutor(max_workers=400) as pool:
        t0 = time.time()
        for partner, rate in PARTNERS.items():
            def schedule(partner=partner, rate=rate):
                n = 0
                while time.time() - t0 < a.seconds:
                    futures.append(pool.submit(send, a.target, partner, n)); n += 1
                    time.sleep(1 / rate)
            threading.Thread(target=schedule, daemon=True).start()
        time.sleep(a.seconds + 0.5)
    results = [f.result() for f in futures]
    by = {}
    for partner, ok, attempts, secs in results:
        b = by.setdefault(partner, {"sent": 0, "ok": 0, "failed": 0, "attempts": 0, "lat": []})
        b["sent"] += 1; b["ok" if ok else "failed"] += 1; b["attempts"] += attempts; b["lat"].append(secs)
    for partner, b in by.items():
        lat = sorted(b.pop("lat")); p = lambda q: round(lat[min(len(lat) - 1, int(q * len(lat)))], 2)
        b["p50_s"], b["p99_s"] = p(0.5), p(0.99)
        print(partner, json.dumps(b))


if __name__ == "__main__":
    main()
