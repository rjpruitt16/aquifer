#!/usr/bin/env python3
"""
Upstream target + webhook receiver for Aquifer tests.
Returns X-Aquifer-* headers to drive faster pacing.
Tracks webhook deliveries by job_id for stream fallback tests.
Implements the L8 receiver protocol for signed-delivery tests.
"""
import base64
import json
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

RPS = 20
MAX_CONCURRENT = 5

job_count = 0
webhook_count = 0
webhooks_by_job = {}  # job_id -> {payload, l8_headers}
lock = threading.Lock()

# ---- L8 receiver identity (generated once at startup) ----
try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    _l8_priv = Ed25519PrivateKey.generate()
    _l8_pub  = _l8_priv.public_key()
    L8_PUBLIC_KEY_B64 = base64.b64encode(
        _l8_pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
    ).decode()
    L8_ENABLED = True

    def l8_sign(message: bytes) -> str:
        return base64.b64encode(_l8_priv.sign(message)).decode()

    def l8_verify(pub_b64: str, message: bytes, sig_b64: str) -> bool:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        try:
            pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(pub_b64))
            pub.verify(base64.b64decode(sig_b64), message)
            return True
        except (InvalidSignature, Exception):
            return False

    print(f"[L8] receiver key ready  pub={L8_PUBLIC_KEY_B64[:16]}...", flush=True)

except ImportError:
    L8_ENABLED = False
    L8_PUBLIC_KEY_B64 = ""
    print("[L8] cryptography library not found — L8 endpoints disabled", flush=True)


used_nonces = set()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global job_count, webhook_count

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""

        if self.path == "/webhook":
            with lock:
                webhook_count += 1
                payload = json.loads(body) if body else {}
                job_id = payload.get("job_id")
                l8_headers = {
                    k: self.headers.get(k)
                    for k in ("X-L8-Delivery-Id", "X-L8-Timestamp", "X-L8-Key-Id", "X-L8-Signature")
                    if self.headers.get(k)
                }
                if job_id:
                    webhooks_by_job[job_id] = {"payload": payload, "l8_headers": l8_headers}
                signed = "signed" if l8_headers else "unsigned"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] webhook #{webhook_count}  "
                      f"job={str(job_id or '?')[:8]}  status={payload.get('status','?')}  [{signed}]",
                      flush=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
            return

        if self.path == "/l8/challenge" and L8_ENABLED:
            try:
                req = json.loads(body)
                challenge_id = req["challenge_id"]
                nonce        = req["nonce"]
                timestamp    = req["timestamp"]
                sender_pub   = req["sender_public_key"]
                signature    = req["signature"]

                import time
                if abs(time.time() - timestamp) > 300:
                    self._json_error(400, "timestamp expired")
                    return
                if nonce in used_nonces:
                    self._json_error(400, "nonce already used")
                    return
                used_nonces.add(nonce)

                msg = f"{challenge_id}:{nonce}".encode()
                if not l8_verify(sender_pub, msg, signature):
                    self._json_error(400, "signature verification failed")
                    return

                resp = {
                    "challenge_id":        challenge_id,
                    "nonce":               nonce,
                    "receiver_signature":  l8_sign(msg),
                    "receiver_public_key": L8_PUBLIC_KEY_B64,
                }
                print(f"[{datetime.now().strftime('%H:%M:%S')}] L8 challenge from {sender_pub[:16]}... accepted",
                      flush=True)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(resp).encode())
            except (KeyError, json.JSONDecodeError) as e:
                self._json_error(400, str(e))
            return

        if self.path == "/reset":
            with lock:
                job_count = 0
                webhook_count = 0
                webhooks_by_job.clear()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"reset"}')
            return

        # Everything else is the upstream target
        with lock:
            job_count += 1
            n = job_count
        print(f"[{datetime.now().strftime('%H:%M:%S')}] request #{n:4d}  {self.command} {self.path}", flush=True)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Aquifer-Rps", str(RPS))
        self.send_header("X-Aquifer-Max-Concurrent", str(MAX_CONCURRENT))
        self.end_headers()
        self.wfile.write(json.dumps({"request": n}).encode())

    def do_GET(self):
        if self.path == "/.well-known/l8" and L8_ENABLED:
            meta = {
                "protocol_version":   "0.1",
                "service_name":       "aquifer-test-target",
                "public_key":         L8_PUBLIC_KEY_B64,
                "challenge_endpoint": "/l8/challenge",
                "supported_algorithms": ["ed25519"],
                "capabilities":       ["signed_payloads"],
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(meta).encode())
            return

        if self.path.startswith("/webhooks/"):
            job_id = self.path[len("/webhooks/"):]
            with lock:
                entry = webhooks_by_job.get(job_id)
            if entry:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                # "webhook" key stays backward-compatible with test_stream.py
                self.wfile.write(json.dumps({
                    "webhook":    entry["payload"],
                    "l8_headers": entry["l8_headers"],
                }).encode())
            else:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"webhook":null,"l8_headers":{}}')
            return

        if self.path == "/webhooks":
            with lock:
                count = webhook_count
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"count": count}).encode())
            return

        # GET requests treated as upstream target
        self.do_POST()

    def _json_error(self, code, msg):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"error": msg}).encode())

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9000), Handler)
    print(f"Target server on :9000  (RPS hint={RPS}, max_concurrent={MAX_CONCURRENT})")
    server.serve_forever()
