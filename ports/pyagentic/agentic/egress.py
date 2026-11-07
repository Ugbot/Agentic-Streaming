"""The operator's egress policy, applied to every HTTP tool and model call before anything is sent.

The Python peer of `org.jagentic.core.net.Egress` and `OutboundUrlPolicy`. Private addresses and the
metadata address are refused unless the operator says otherwise, so a document that names an internal
URL does not reach it. The settings are the operator's, not the document's:
`AGENTIC_EGRESS_ALLOW_HOSTS` (exact names or `*.suffix`, comma separated; empty means any public host)
and `AGENTIC_EGRESS_ALLOW_PRIVATE`.

A check resolves the host and the client resolves it again to connect, so a name that changes between
the two gets through. Redirects are not followed.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, List, Mapping, Optional, Sequence

Resolver = Callable[[str], Sequence[str]]


class EgressBlocked(Exception):
    """A URL the policy refuses. Nothing was sent, so it is a definite failure."""


def _resolve(host: str) -> List[str]:
    try:
        return sorted({info[4][0] for info in socket.getaddrinfo(host, None)})
    except OSError as exc:
        raise EgressBlocked(f"host {host} does not resolve") from exc


def forbidden_address(raw: str) -> bool:
    """Loopback, link-local, private, multicast, unspecified, and IPv4 inside IPv6."""
    address = ipaddress.ip_address(raw.split("%")[0])
    if isinstance(address, ipaddress.IPv6Address):
        embedded = address.ipv4_mapped or address.sixtofour
        if embedded is None and address in ipaddress.ip_network("64:ff9b::/96"):
            embedded = ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
        if embedded is not None:
            return True
    return not address.is_global or address.is_multicast


def _host_allowed(host: str, allowed: Sequence[str]) -> bool:
    for entry in allowed:
        if entry.startswith("*.") and host.endswith(entry[1:]) and host != entry[2:]:
            return True
        if host == entry:
            return True
    return False


def check(url: str, env: Optional[Mapping[str, str]] = None, resolve: Resolver = _resolve) -> None:
    """Raises `EgressBlocked` for a URL the operator's policy refuses."""
    settings = os.environ if env is None else env
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise EgressBlocked(f"scheme {parts.scheme!r} is not allowed")
    if parts.username is not None or parts.password is not None:
        raise EgressBlocked("a url with credentials in it is not allowed")
    host = (parts.hostname or "").lower()
    if not host:
        raise EgressBlocked("a url needs a host")
    allowed = [h.strip().lower() for h in settings.get("AGENTIC_EGRESS_ALLOW_HOSTS", "").split(",") if h.strip()]
    if allowed and not _host_allowed(host, allowed):
        raise EgressBlocked(f"host {host} is not in the allowlist")
    if settings.get("AGENTIC_EGRESS_ALLOW_PRIVATE", "").lower() == "true":
        return
    for address in resolve(host):
        if forbidden_address(address):
            raise EgressBlocked(f"host {host} resolves to a non-public address {address}")


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirects)


def urlopen(request: urllib.request.Request, timeout: float) -> Any:
    """`urlopen` that follows no redirect: a 3xx comes back as an `HTTPError`, a definite failure."""
    return _OPENER.open(request, timeout=timeout)
