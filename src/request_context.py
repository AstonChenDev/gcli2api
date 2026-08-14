"""HTTP request context used by request-level statistics."""

import ipaddress
import os
from contextvars import ContextVar
from functools import lru_cache
from typing import Optional

from starlette.types import ASGIApp, Receive, Scope, Send

_client_ip: ContextVar[Optional[str]] = ContextVar("client_ip", default=None)


def get_client_ip() -> Optional[str]:
    """Return the client IP associated with the current HTTP request."""
    return _client_ip.get()


def _normalize_ip(value: str) -> Optional[str]:
    """Validate and canonicalize an IPv4/IPv6 string."""
    value = value.strip().strip('"')
    if not value:
        return None

    # RFC 7239-style IPv6 values may be wrapped in brackets.
    if value.startswith("[") and "]" in value:
        value = value[1 : value.index("]")]

    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        # Some proxies append a port to IPv4 addresses.
        host, separator, port = value.rpartition(":")
        if separator and port.isdigit():
            try:
                return str(ipaddress.ip_address(host))
            except ValueError:
                pass
    return None


@lru_cache(maxsize=16)
def _parse_trusted_proxy_networks(raw_value: str):
    networks = []
    for item in raw_value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            continue
    return tuple(networks)


def _trust_proxy_headers(peer_ip: Optional[str]) -> bool:
    """Only accept caller-IP headers from explicitly trusted proxies."""
    trust_all = os.getenv("TRUST_PROXY_HEADERS", "").strip().lower()
    if trust_all in {"1", "true", "yes", "on"}:
        return True

    if not peer_ip:
        return False
    try:
        peer = ipaddress.ip_address(peer_ip)
    except ValueError:
        return False

    raw_networks = os.getenv("TRUSTED_PROXY_IPS", "")
    return any(peer in network for network in _parse_trusted_proxy_networks(raw_networks))


def _is_trusted_proxy_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    networks = _parse_trusted_proxy_networks(os.getenv("TRUSTED_PROXY_IPS", ""))
    return any(address in network for network in networks)


def resolve_client_ip(scope: Scope) -> str:
    """Resolve the direct peer IP, or a trusted proxy's original client IP."""
    client = scope.get("client")
    peer_ip = _normalize_ip(str(client[0])) if client else None

    if _trust_proxy_headers(peer_ip):
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        if headers.get("cf-connecting-ip"):
            resolved = _normalize_ip(headers["cf-connecting-ip"])
            if resolved:
                return resolved

        if headers.get("x-forwarded-for"):
            forwarded_ips = [
                resolved
                for item in headers["x-forwarded-for"].split(",")
                if (resolved := _normalize_ip(item))
            ]
            trust_all = os.getenv("TRUST_PROXY_HEADERS", "").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if trust_all and forwarded_ips:
                return forwarded_ips[0]

            # Walk from the trusted proxy towards the caller. This prevents a
            # client-supplied leftmost X-Forwarded-For value from winning.
            for resolved in reversed(forwarded_ips):
                if not _is_trusted_proxy_ip(resolved):
                    return resolved
            if forwarded_ips:
                return forwarded_ips[0]

        if headers.get("x-real-ip"):
            resolved = _normalize_ip(headers["x-real-ip"])
            if resolved:
                return resolved

    return peer_ip or "unknown"


class ClientIPContextMiddleware:
    """Keep the caller IP available until the full response stream completes."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        token = _client_ip.set(resolve_client_ip(scope))
        try:
            await self.app(scope, receive, send)
        finally:
            _client_ip.reset(token)
