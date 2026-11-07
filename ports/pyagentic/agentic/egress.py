"""The operator's egress policy, applied to every HTTP tool and model call before anything is sent.

The Python peer of `org.jagentic.core.net.Egress` and `OutboundUrlPolicy`. Private addresses and the
metadata address are refused unless the operator says otherwise, so a document that names an internal
URL does not reach it. The settings are the operator's, not the document's:
`AGENTIC_EGRESS_ALLOW_HOSTS` (exact names or `*.suffix`, comma separated; empty means any public host)
and `AGENTIC_EGRESS_ALLOW_PRIVATE`.

A check resolves the host once and checks every address it gets. `check` returns them as a `Pin`, and
`urlopen` connects to one of those addresses and no other, with the host name kept for `Host`, SNI and
the certificate, so a name whose answer changes after the check (DNS rebinding) cannot move the call.
Redirects are not followed, and an environment proxy is not used, because a proxy would resolve the
name again itself.
"""

from __future__ import annotations

import http.client
import ipaddress
import os
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, List, Mapping, NamedTuple, Optional, Sequence

Resolver = Callable[[str], Sequence[str]]


class EgressBlocked(Exception):
    """A URL the policy refuses. Nothing was sent, so it is a definite failure."""


class Pin(NamedTuple):
    """The addresses a host was checked at: a connection goes to one of these and nowhere else."""

    host: str
    addresses: List[str]


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


def check(url: str, env: Optional[Mapping[str, str]] = None, resolve: Resolver = _resolve) -> Pin:
    """Raises `EgressBlocked` for a URL the operator's policy refuses, and returns where it may go."""
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
    addresses = list(resolve(host))
    if not addresses:
        raise EgressBlocked(f"host {host} does not resolve")
    if settings.get("AGENTIC_EGRESS_ALLOW_PRIVATE", "").lower() == "true":
        return Pin(host, addresses)
    for address in addresses:
        if forbidden_address(address):
            raise EgressBlocked(f"host {host} resolves to a non-public address {address}")
    return Pin(host, addresses)


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _pinned_connection(base: type, pin: Pin) -> Callable[..., http.client.HTTPConnection]:
    """An `HTTPConnection` or `HTTPSConnection` that opens its socket to a pinned address. Everything
    else is the class's own: `Host` and, for TLS, SNI and the certificate check use the host name."""

    def create_connection(address: Any, timeout: Any = socket._GLOBAL_DEFAULT_TIMEOUT,
                          source_address: Any = None, **kwargs: Any) -> socket.socket:
        host, port = address[0], address[1]
        if host.lower().rstrip(".") != pin.host:
            raise EgressBlocked(f"connection to {host} is not the checked host {pin.host}")
        last: Optional[OSError] = None
        for ip in pin.addresses:
            try:
                return socket.create_connection((ip.split("%")[0], port), timeout, source_address)
            except OSError as exc:
                last = exc
        raise last if last is not None else OSError(f"no address for {host}")

    class Pinned(base):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._create_connection = create_connection

    return Pinned


class _PinnedHTTP(urllib.request.HTTPHandler):
    def __init__(self, pin: Pin) -> None:
        super().__init__()
        self.pin = pin

    def http_open(self, req: urllib.request.Request) -> Any:
        return self.do_open(_pinned_connection(http.client.HTTPConnection, self.pin), req)


class _PinnedHTTPS(urllib.request.HTTPSHandler):
    def __init__(self, pin: Pin, context: Optional[ssl.SSLContext]) -> None:
        super().__init__(context=context)
        self.pin = pin

    def https_open(self, req: urllib.request.Request) -> Any:
        return self.do_open(_pinned_connection(http.client.HTTPSConnection, self.pin), req,
                            context=self._context)


def urlopen(request: urllib.request.Request, timeout: float, pin: Optional[Pin] = None,
            context: Optional[ssl.SSLContext] = None) -> Any:
    """Send `request` to an address the policy checked, following no redirect: a 3xx comes back as an
    `HTTPError`, a definite failure. Without a `pin` the URL is checked here, which is the same check."""
    pin = check(request.full_url) if pin is None else pin
    host = (urllib.parse.urlsplit(request.full_url).hostname or "").lower().rstrip(".")
    if host != pin.host.lower().rstrip("."):
        raise EgressBlocked(f"{host} is not the checked host {pin.host}")
    pin = Pin(host, list(pin.addresses))
    opener = urllib.request.OpenerDirector()
    for handler in (urllib.request.ProxyHandler({}), urllib.request.UnknownHandler(), _PinnedHTTP(pin),
                    _PinnedHTTPS(pin, context), urllib.request.HTTPDefaultErrorHandler(), _NoRedirects(),
                    urllib.request.HTTPErrorProcessor()):
        opener.add_handler(handler)
    return opener.open(request, timeout=timeout)
