"""Web toolkit (Tier 4) — a robots-aware fetch + text/link extraction tool, built on the
Python standard library (``urllib`` + ``html.parser`` + ``urllib.robotparser``) so it has
**no extra dependencies**. Ships as an opt-in tool, default off: call
``register_web_tools(registry)`` to expose ``web_fetch`` / ``web_links`` to an agent.
Mirrors the Flink ``web/`` toolkit (Jsoup + crawler-commons robots) at a portable subset.
"""

from __future__ import annotations

import urllib.error
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional
from urllib.parse import urldefrag, urljoin, urlparse

_DEFAULT_UA = "jagentic-webfetch/0.1 (+https://github.com/jagentic)"
_SKIP_TAGS = {"script", "style", "noscript", "template"}


class _Extractor(HTMLParser):
    """Collects visible text and absolute links from an HTML document."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self._base = base_url
        self._skip_depth = 0
        self._chunks: List[str] = []
        self._title: List[str] = []
        self._in_title = False
        self.links: List[str] = []
        self._seen_links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True
        if tag == "a":
            for k, v in attrs:
                if k == "href" and v:
                    absolute = urldefrag(urljoin(self._base, v.strip()))[0]
                    scheme = urlparse(absolute).scheme
                    if scheme in ("http", "https") and absolute not in self._seen_links:
                        self._seen_links.add(absolute)
                        self.links.append(absolute)

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = data.strip()
        if not text:
            return
        if self._in_title:
            self._title.append(text)
        else:
            self._chunks.append(text)

    @property
    def title(self) -> str:
        return " ".join(self._title).strip()

    @property
    def text(self) -> str:
        # collapse runs of whitespace, keep paragraph-ish separation
        return "\n".join(self._chunks)


def _robots_allows(url: str, user_agent: str, timeout: float) -> bool:
    """Check the site's robots.txt; fail-open if it can't be fetched (the common
    convention), fail-closed only on an explicit Disallow match."""
    parts = urlparse(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    try:
        req = urllib.request.Request(robots_url, headers={"User-Agent": user_agent})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", "replace")
        rp.parse(content.splitlines())
    except Exception:
        return True  # no robots.txt / unreachable => allowed
    return rp.can_fetch(user_agent, url)


def web_fetch(
    url: str,
    *,
    user_agent: str = _DEFAULT_UA,
    timeout: float = 15.0,
    max_bytes: int = 2_000_000,
    respect_robots: bool = True,
) -> Dict[str, Any]:
    """Fetch ``url`` and return ``{url, status, title, text, links}``. Honours robots.txt
    unless ``respect_robots=False``. Raises ``PermissionError`` if robots disallows."""
    if urlparse(url).scheme not in ("http", "https"):
        raise ValueError(f"unsupported URL scheme: {url!r}")
    if respect_robots and not _robots_allows(url, user_agent, timeout):
        raise PermissionError(f"robots.txt disallows fetching {url}")
    req = urllib.request.Request(url, headers={"User-Agent": user_agent, "Accept": "text/html,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        raw = resp.read(max_bytes)
        charset = resp.headers.get_content_charset() or "utf-8"
        final_url = resp.geturl()
    html = raw.decode(charset, "replace")
    extractor = _Extractor(final_url)
    extractor.feed(html)
    return {
        "url": final_url,
        "status": status,
        "title": extractor.title,
        "text": extractor.text,
        "links": extractor.links,
    }


def register_web_tools(registry, *, respect_robots: bool = True, prefix: str = "") -> List[str]:
    """Register ``web_fetch`` (text+title) and ``web_links`` (link list) into a
    ToolRegistry. Opt-in. Returns the registered tool ids."""

    def _fetch(params: Dict[str, Any]) -> Any:
        url = params.get("url") or params.get("query") or ""
        result = web_fetch(str(url), respect_robots=respect_robots)
        # tools return a single string the brain can read back
        title = result["title"]
        head = f"{title}\n\n" if title else ""
        return head + result["text"]

    def _links(params: Dict[str, Any]) -> Any:
        url = params.get("url") or ""
        result = web_fetch(str(url), respect_robots=respect_robots)
        return "\n".join(result["links"])

    fetch_id = f"{prefix}web_fetch"
    links_id = f"{prefix}web_links"
    registry.register(fetch_id, "Fetch a web page (robots-aware) and return its title + text.", _fetch)
    registry.register(links_id, "Fetch a web page and return its outbound links, one per line.", _links)
    return [fetch_id, links_id]
