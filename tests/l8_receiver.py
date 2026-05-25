#!/usr/bin/env python3
"""
Standalone L8-compliant webhook receiver for protocol tests.

Runs on :9001. Implements:
  GET  /.well-known/l8   — publishes receiver identity and public key
  POST /l8/challenge     — completes the ownership handshake
  POST /webhook          — receives signed webhook deliveries, stores headers
  GET  /deliveries/:id   — returns stored delivery + L8 headers for test assertions
  POST /reset            — clears state between tests
"""
import base64
import json
import sys
import time
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
        Ed25519PublicKey,
    )
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    from cryptography.exceptions import InvalidSignature
except ImportError:
    print("ERROR: 'cryptography' package required. Run: pip install cryptography")
    sys.exit(1)

PORT = int(__import__("os").getenv("L8_RECEIVER_PORT", "9001"))

# ---- identity ----
_priv = Ed25519PrivateKey.generate()
_pub  = _priv.public_key()
PUBLIC_KEY_B64 = base64.b64encode(
    _pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
).decode()

def _sign(msg: bytes) -> str:
    return base64.b64encode(_priv.sign(msg)).decode()

def _verify(pub_b64: str, msg: bytes, sig_b64: str) -> bool:
    try:
        pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(pub_b64))
        pub.verify(base64.b64decode(sig_b64), msg)
        return True
    except (InvalidSignature, Exception):
        return False

# ---- state ----
deliveries   = {}   # job_id -> {payload, l8_headers}
used_nonces  = set()
lock         = threading.Lock()


class Handler(BaseHTTPRequestHandler):

    # ---- GET ----

    def do_GET(self):
        if self.path == "/.well-known/l8":
            self._json(200, {
                "protocol_version":     "0.1",
                "service_name":         "l8-test-receiver",
                "public_key":           PUBLIC_KEY_B64,
                "challenge_endpoint":   "/l8/challenge",
                "supported_algorithms": ["ed25519"],
                "capabilities":         ["signed_payloads"],
            })
            return

        if self.path.startswith("/deliveries/"):
            job_id = self.path[len("/deliveries/"):]
            with lock:
                entry = deliveries.get(job_id)
            if entry:
                self._json(200, entry)
            else:
                self._json(404, {"delivery": None})
            return

        self._json(404, {"error": "not found"})

    # ---- POST ----

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length) if length else b""

        if self.path == "/l8/challenge":
            self._handle_challenge(body)
            return

        if self.path == "/webhook":
            self._handle_webhook(body)
            return

        if self.path == "/reset":
            with lock:
                deliveries.clear()
                used_nonces.clear()
            self._json(200, {"status": "reset"})
            return

        self._json(404, {"error": "not found"})

    # ---- handlers ----

    def _handle_challenge(self, body: bytes):
        try:
            req          = json.loads(body)
            challenge_id = req["challenge_id"]
            nonce        = req["nonce"]
            timestamp    = int(req["timestamp"])
            sender_pub   = req["sender_public_key"]
            signature    = req["signature"]
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            self._json(400, {"error": f"invalid request: {e}"})
            return

        if abs(time.time() - timestamp) > 300:
            self._json(400, {"error": "timestamp expired"})
            return

        with lock:
            if nonce in used_nonces:
                self._json(400, {"error": "nonce already used"})
                return
            used_nonces.add(nonce)

        msg = f"{challenge_id}:{nonce}".encode()
        if not _verify(sender_pub, msg, signature):
            self._json(400, {"error": "signature verification failed"})
            return

        print(f"[{datetime.now().strftime('%H:%M:%S')}] L8 handshake accepted from {sender_pub[:16]}...", flush=True)
        self._json(200, {
            "challenge_id":        challenge_id,
            "nonce":               nonce,
            "receiver_signature":  _sign(msg),
            "receiver_public_key": PUBLIC_KEY_B64,
        })

    def _handle_webhook(self, body: bytes):
        payload    = json.loads(body) if body else {}
        job_id     = payload.get("job_id")
        l8_headers = {
            k: self.headers.get(k)
            for k in ("X-L8-Delivery-Id", "X-L8-Timestamp", "X-L8-Key-Id", "X-L8-Signature")
            if self.headers.get(k)
        }
        signed = "signed" if l8_headers else "unsigned"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] webhook  "
              f"job={str(job_id or '?')[:8]}  status={payload.get('status','?')}  [{signed}]",
              flush=True)
        with lock:
            if job_id:
                deliveries[job_id] = {
                    "payload":    payload,
                    "raw_body":   base64.b64encode(body).decode(),  # exact bytes Aquifer signed
                    "l8_headers": l8_headers,
                }
        self._json(200, {"ok": True})

    # ---- helpers ----

    def _json(self, code: int, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"L8 receiver on :{PORT}  pub={PUBLIC_KEY_B64[:16]}...")
    server.serve_forever()
