"""Web toolkit wrappers — Jsoup + crawler-commons + Tika behind a Python facade."""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from ._jvm import jclass


def options(
    *,
    user_agent: Optional[str] = None,
    fetch_timeout: Optional[timedelta] = None,
    max_page_bytes: int = 0,
    max_depth: int = 4,
    respect_robots: bool = True,
    follow_redirects: bool = True,
):
    """Build a :class:`WebToolkitOptions` with Python-friendly kwargs."""
    Opt = jclass("org.agentic.flink.web.WebToolkitOptions")
    Duration = jclass("java.time.Duration")
    o = Opt.defaults()
    if user_agent is not None:
        o = o.withUserAgent(user_agent)
    if fetch_timeout is not None:
        o = o.withFetchTimeout(Duration.ofMillis(int(fetch_timeout.total_seconds() * 1000)))
    if max_page_bytes > 0:
        o = o.withMaxPageBytes(max_page_bytes)
    o = o.withMaxDepth(max_depth)
    o = o.withRespectRobots(respect_robots)
    o = o.withFollowRedirects(follow_redirects)
    return o


def fetch_tool(opts=None):
    """Build a :class:`WebFetchTool` (Java ``ToolExecutor``)."""
    WFT = jclass("org.agentic.flink.web.WebFetchTool")
    return WFT(opts if opts is not None else options())


def extract_links_tool(opts=None):
    """Build an :class:`ExtractLinksTool`."""
    ELT = jclass("org.agentic.flink.web.ExtractLinksTool")
    return ELT(opts if opts is not None else options())


def url_request(url: str, source: str = "python", depth: int = 0):
    """Build a :class:`UrlRequest` for the crawler frontier."""
    UR = jclass("org.agentic.flink.web.UrlRequest")
    return UR(url, source, int(depth))


def crawler_core(*frontier_channels, opts=None):
    """Return a :class:`CrawlerCore.Builder` configured with the supplied
    frontier channels. Call ``.open(env)`` on the result to materialize a
    ``DataStream<CrawledPage>``."""
    Core = jclass("org.agentic.flink.web.CrawlerCore")
    b = Core.builder()
    if frontier_channels:
        b = b.frontier(*frontier_channels)
    if opts is not None:
        b = b.options(opts)
    return b


__all__ = [
    "options",
    "fetch_tool",
    "extract_links_tool",
    "url_request",
    "crawler_core",
]
