#!/usr/bin/env python3
"""
Upstream target + webhook receiver for Aquifer load testing.
Returns X-Aquifer-* headers to drive faster pacing.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime

RPS = 20
MAX_CONCURRENT = 5

job_count = 0
webhook_count = 0
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global job_count, webhook_count

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""

        if self.path == "/webhook":
            with lock:
                webhook_count += 1
                payload = json.loads(body) if body else {}
                print(f"[{datetime.now().strftime('%H:%M:%S')}] webhook #{webhook_count}  "
                      f"job={payload.get('job_id','?')[:8]}  status={payload.get('status','?')}", flush=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
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
        self.do_POST()

    def log_message(self, *_):
        pass  # suppress default access log


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 9000), Handler)
    print(f"Target server on :9000  (RPS hint={RPS}, max_concurrent={MAX_CONCURRENT})")
    server.serve_forever()
