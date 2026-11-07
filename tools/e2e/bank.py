"""An outside world for tools/kafka-journal-tm-loss.sh: a bank that remembers every request it saw.

POST /pay answers at once. POST /hold records the request and never answers, so a test can lose the
TaskManager during the call. GET /seen lists what arrived, as [path, Idempotency-Key] pairs.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SEEN = []
FOREVER = threading.Event()


class Bank(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _answer(self, body):
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._answer(SEEN)

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        SEEN.append([self.path, self.headers.get("Idempotency-Key")])
        if self.path == "/hold":
            FOREVER.wait(600)
            return
        self._answer({"paid": True})


ThreadingHTTPServer(("0.0.0.0", 8080), Bank).serve_forever()
