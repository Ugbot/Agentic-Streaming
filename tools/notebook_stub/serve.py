"""Local page server for the notebooks under notebooks/.

Serves the static HTML pages in the ``pages/`` directory next to this file over plain HTTP so
the scrape and RAG notebooks (02, 03) and the quickstart web fetch (01) run without reaching
the public internet. It uses only the standard library, binds to the loopback interface, and
makes no outbound calls.

Run from the repository root:

    python tools/notebook_stub/serve.py            # http://127.0.0.1:8077
    python tools/notebook_stub/serve.py --port 9000

Endpoints:

    /robots.txt     allow-all robots file, so WebFetchTool's robots check passes
    /flink.html     short overview of Apache Flink
    /agents.html    short overview of this repository
    /               index linking the pages above

Stop with Ctrl-C. Notebooks read the base URL from NOTEBOOK_STUB_URL and default to
http://localhost:8077.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import pathlib
import sys

PAGES = pathlib.Path(__file__).resolve().parent / "pages"
DEFAULT_PORT = 8077


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler rooted at ``pages/`` that logs one line per request."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (stdlib signature)
        sys.stderr.write("stub %s %s\n" % (self.address_string(), format % args))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args(argv)

    if not PAGES.is_dir():
        print(f"pages directory missing: {PAGES}", file=sys.stderr)
        return 2

    handler = functools.partial(QuietHandler, directory=str(PAGES))
    with http.server.ThreadingHTTPServer((args.host, args.port), handler) as server:
        print(f"notebook stub serving {PAGES} at http://{args.host}:{args.port} (Ctrl-C to stop)")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
