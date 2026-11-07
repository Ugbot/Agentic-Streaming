"""The local HTTP server the fixtures' `{{http}}` tools talk to, for the Python bindings.

    /ok            200 {"ok": true}
    /status/<code> that status, with a JSON error body
    /hang          answers after 5 s, past any fixture's `timeout_ms`
    /echo          200 {"key": <the Idempotency-Key header>}

The JVM bindings serve the same four routes. Nothing here is a runtime: it stands in for the
outside world.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - the stdlib names it
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        path = self.path.split("?")[0]
        if path == "/hang":
            time.sleep(5)
            self._answer(200, {"ok": True})
        elif path == "/echo":
            self._answer(200, {"key": self.headers.get("Idempotency-Key")})
        elif path.startswith("/status/"):
            self._answer(int(path.rsplit("/", 1)[1]), {"error": "stub"})
        else:
            self._answer(200, {"ok": True})

    do_GET = do_POST  # noqa: N815

    def _answer(self, code: int, body: Any) -> None:
        raw = json.dumps(body).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except OSError:
            pass  # the caller gave up, which is what /hang is for

    def log_message(self, *args: Any) -> None:
        pass


class HttpStub:
    def __enter__(self) -> "HttpStub":
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.daemon_threads = True
        self.base = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, args=(0.01,), daemon=True).start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
