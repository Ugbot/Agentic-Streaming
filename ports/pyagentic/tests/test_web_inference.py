"""Phase E — web toolkit (robots-aware fetch + link extraction against a local HTTP
server fixture, no network) and the DL inference SPI (lexicon + nearest-centroid
classifiers, classifier guardrail). All offline/deterministic."""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyagentic.inference import (  # noqa: E402
    ClassifierGuardrail,
    ClassifierScorer,
    EmbeddingClassifier,
    LexiconClassifier,
)
from pyagentic.tools import ToolRegistry  # noqa: E402
from pyagentic.web import register_web_tools, web_fetch  # noqa: E402

_PAGE = (
    "<html><head><title>Acme Bank</title><style>.x{color:red}</style></head>"
    "<body><h1>Welcome</h1><p>Open a savings account today.</p>"
    "<script>var x=1;</script>"
    "<a href='/cards'>Cards</a> <a href='https://example.org/help'>Help</a>"
    "</body></html>"
)


class _Handler(BaseHTTPRequestHandler):
    robots = "User-agent: *\nAllow: /\n"

    def log_message(self, *args):  # silence
        pass

    def do_GET(self):
        if self.path == "/robots.txt":
            body = self.robots.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
        elif self.path == "/blocked":
            body = b"secret"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
        else:
            body = _PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()


def test_web_fetch_extracts_text_and_links(server):
    result = web_fetch(f"{server}/page")
    assert result["title"] == "Acme Bank"
    assert "Open a savings account today." in result["text"]
    # script/style content is stripped
    assert "var x" not in result["text"]
    assert "color:red" not in result["text"]
    # relative link resolved to absolute; external link kept; deduped
    assert f"{server}/cards" in result["links"]
    assert "https://example.org/help" in result["links"]


def test_web_tools_register_and_run(server):
    reg = ToolRegistry()
    ids = register_web_tools(reg)
    assert "web_fetch" in ids and "web_links" in ids
    text = reg.execute("web_fetch", {"url": f"{server}/page"})
    assert "Acme Bank" in text and "savings account" in text
    links = reg.execute("web_links", {"url": f"{server}/page"})
    assert "https://example.org/help" in links


def test_robots_disallow_blocks_fetch(server):
    _Handler.robots = "User-agent: *\nDisallow: /blocked\n"
    try:
        with pytest.raises(PermissionError):
            web_fetch(f"{server}/blocked")
        # an allowed path still works
        assert web_fetch(f"{server}/page")["title"] == "Acme Bank"
    finally:
        _Handler.robots = "User-agent: *\nAllow: /\n"


def test_lexicon_classifier_labels_by_keywords():
    clf = LexiconClassifier(
        {
            "billing": ["invoice", "charge", "refund", "payment"],
            "tech": ["error", "crash", "bug", "login"],
        },
        default_label="other",
    )
    assert clf.classify("I want a refund for this charge").label == "billing"
    assert clf.classify("the app keeps showing a login error").label == "tech"
    assert clf.classify("hello there").label == "other"
    # per-label scores form a distribution
    c = clf.classify("refund my payment please")
    assert abs(sum(c.scores.values()) - 1.0) < 1e-9


def test_embedding_classifier_nearest_centroid():
    clf = EmbeddingClassifier().fit(
        {
            "billing": ["refund my invoice", "dispute a charge", "payment failed"],
            "tech": ["app crashed on login", "error message bug", "cannot sign in"],
        }
    )
    assert clf.classify("please refund this invoice charge").label == "billing"
    assert clf.classify("login error keeps crashing").label == "tech"
    c = clf.classify("refund invoice")
    assert 0.0 <= c.score <= 1.0
    assert abs(sum(c.scores.values()) - 1.0) < 1e-6


def test_classifier_guardrail_blocks_label():
    clf = LexiconClassifier(
        {"toxic": ["idiot", "stupid", "hate"], "ok": ["please", "thanks", "help"]},
        default_label="ok",
    )
    guard = ClassifierGuardrail(clf, blocked_labels=["toxic"], threshold=0.3)
    assert guard.check_input("you stupid idiot") is not None
    assert guard.check_input("please help, thanks") is None
    # scorer adapter exposes the per-label probability
    scorer = ClassifierScorer(clf, "toxic")
    assert scorer.score("idiot") > scorer.score("thanks please help")
