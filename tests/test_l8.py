#!/usr/bin/env python3
"""
L8 protocol integration tests for Aquifer.

Requires the 'cryptography' package: pip install cryptography

Three cases:
  1. Handshake    — Aquifer completes the challenge with the L8 receiver
  2. Signed delivery — webhook arrives with valid X-L8-Signature
  3. Signature valid — verify sig against Aquifer's public key from /.well-known/l8
"""
import base64
import hashlib
import json
import os
import sys
import time
import uuid

import requests

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from cryptography.exceptions import InvalidSignature
except ImportError:
    print("ERROR: 'cryptography' package required. Run: pip install cryptography")
    sys.exit(1)

AQUIFER_URL  = os.getenv("AQUIFER_URL",  "http://localhost:8080")
TARGET_URL   = os.getenv("TARGET_URL",   "http://localhost:9000")
RECEIVER_URL = os.getenv("RECEIVER_URL", "http://localhost:9001")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

failures = []


def fail(test, reason):
    print(f"  {FAIL} {test}: {reason}")
    failures.append((test, reason))


def ok(test):
    print(f"  {PASS} {test}")


def reset():
    requests.post(f"{RECEIVER_URL}/reset", timeout=5)


def submit_job():
    payload = {
        "user_id":        "l8-test-user",
        "idempotent_key": str(uuid.uuid4()),
        "url":            TARGET_URL,
        "method":         "GET",
        "headers":        {},
        "webhook_url":    f"{RECEIVER_URL}/webhook",
    }
    r = requests.post(f"{AQUIFER_URL}/jobs", json=payload, timeout=5)
    r.raise_for_status()
    return r.json()["job_id"]


def wait_for_delivery(job_id, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{RECEIVER_URL}/deliveries/{job_id}", timeout=5)
        if r.status_code == 200:
            entry = r.json()
            if entry.get("payload"):
                return entry
        time.sleep(0.5)
    return None


def aquifer_public_key() -> str:
    r = requests.get(f"{AQUIFER_URL}/.well-known/l8", timeout=5)
    r.raise_for_status()
    return r.json()["public_key"]


def verify_l8_signature(pub_b64: str, body: bytes, headers: dict) -> bool:
    delivery_id = headers.get("X-L8-Delivery-Id", "")
    timestamp   = headers.get("X-L8-Timestamp", "")
    signature   = headers.get("X-L8-Signature", "")
    if not (delivery_id and timestamp and signature):
        return False
    body_hash = base64.b64encode(hashlib.sha256(body).digest()).decode()
    msg = f"{delivery_id}.{timestamp}.{body_hash}".encode()
    try:
        pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(pub_b64))
        pub.verify(base64.b64decode(signature), msg)
        return True
    except (InvalidSignature, Exception):
        return False


# ---- Test 1: /.well-known/l8 endpoint ----------------------------------------

def test_well_known_l8():
    print("\nTest 1: Aquifer exposes /.well-known/l8")
    try:
        r = requests.get(f"{AQUIFER_URL}/.well-known/l8", timeout=5)
    except Exception as e:
        fail("/.well-known/l8 reachable", str(e))
        return

    if r.status_code != 200:
        fail("/.well-known/l8 returns 200", f"got {r.status_code}")
        return
    ok("/.well-known/l8 returns 200")

    meta = r.json()
    for field in ("protocol_version", "public_key", "challenge_endpoint", "capabilities"):
        if field not in meta:
            fail(f"metadata contains {field}", "missing")
        else:
            ok(f"metadata contains {field}")

    if meta.get("protocol_version") == "0.1":
        ok("protocol_version is 0.1")
    else:
        fail("protocol_version is 0.1", f"got {meta.get('protocol_version')}")


# ---- Test 2: handshake + signed delivery -------------------------------------

def test_signed_delivery():
    print("\nTest 2: Handshake completes and webhook arrives signed")
    reset()

    # Delete cached trust so handshake runs fresh
    import os as _os, glob
    for f in glob.glob("l8-trust/localhost*.json"):
        _os.remove(f)

    job_id = submit_job()
    entry  = wait_for_delivery(job_id, timeout=25)

    if entry is None:
        fail("webhook delivered to L8 receiver", "no delivery within 25s")
        return
    ok("webhook delivered to L8 receiver")

    l8_headers = entry.get("l8_headers", {})
    if not l8_headers:
        fail("webhook carries X-L8-* headers", "no L8 headers present — handshake may not have run")
        return
    ok("webhook carries X-L8-* headers")

    for h in ("X-L8-Delivery-Id", "X-L8-Timestamp", "X-L8-Key-Id", "X-L8-Signature"):
        if l8_headers.get(h):
            ok(f"header present: {h}")
        else:
            fail(f"header present: {h}", "missing")


# ---- Test 3: signature cryptographically valid --------------------------------

def test_signature_valid():
    print("\nTest 3: Signature is cryptographically valid")
    reset()

    job_id = submit_job()
    entry  = wait_for_delivery(job_id, timeout=25)

    if entry is None:
        fail("webhook delivered", "no delivery within 25s")
        return

    l8_headers = entry.get("l8_headers", {})
    if not l8_headers.get("X-L8-Signature"):
        fail("has signature to verify", "no X-L8-Signature header (trust not yet established — run test 2 first)")
        return

    aquifer_pub = aquifer_public_key()
    # Use the raw bytes Aquifer actually sent — re-serializing would differ in key order
    raw_body = base64.b64decode(entry.get("raw_body", ""))

    if verify_l8_signature(aquifer_pub, raw_body, l8_headers):
        ok("signature verifies against Aquifer's public key")
    else:
        fail("signature verifies against Aquifer's public key",
             "verification failed — signature mismatch or wrong public key")


# ---- Test 4: /health advertises L8 -------------------------------------------

def test_health_advertises_l8():
    print("\nTest 4: /health advertises L8 protocol version")
    r = requests.get(f"{AQUIFER_URL}/health", timeout=5)
    data = r.json()

    if data.get("l8_protocol") == "0.1":
        ok("health reports l8_protocol: 0.1")
    else:
        fail("health reports l8_protocol", f"got {data.get('l8_protocol')}")

    if data.get("l8_public_key"):
        ok("health includes l8_public_key")
    else:
        fail("health includes l8_public_key", "missing")


# ---- Main --------------------------------------------------------------------

if __name__ == "__main__":
    print(f"L8 protocol tests")
    print(f"  Aquifer:  {AQUIFER_URL}")
    print(f"  Target:   {TARGET_URL}")
    print(f"  Receiver: {RECEIVER_URL}")

    test_well_known_l8()
    test_signed_delivery()
    test_signature_valid()
    test_health_advertises_l8()

    print()
    if failures:
        print(f"{FAIL} {len(failures)} test(s) failed:")
        for name, reason in failures:
            print(f"  - {name}: {reason}")
        sys.exit(1)
    else:
        print(f"{PASS} All L8 tests passed!")
        sys.exit(0)
