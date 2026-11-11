"""
通用的HTTP客户端模块
为所有需要使用httpx的模块提供统一的客户端配置和方法
保持通用性，不与特定业务逻辑耦合

按事件循环、代理和超时配置复用连接；配置变化后旧连接待在途请求结束再关闭。
"""

import asyncio
import json
from weakref import WeakKeyDictionary
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional

import httpx
from fastapi import Response
from anyio import CancelScope, fail_after

from config import get_proxy_config, get_stream_handshake_timeout_enabled, get_stream_handshake_timeout_seconds
from log import log


class _StreamingClient(httpx.AsyncClient):
    def __init__(self, *, handshake_timeout, **kwargs):
        super().__init__(**kwargs)
        self.handshake_timeout = handshake_timeout

    async def send(self, request, **kwargs):
        # connect= alone does not bound an HTTP proxy's CONNECT response read.
        # Keep one deadline through TCP, proxy negotiation and TLS, then lift it
        # before sending the origin request. Model generation has no deadline.
        original_trace = request.extensions.get("trace")
        pending_stream = None
        with fail_after(self.handshake_timeout) as handshake:
            async def trace(event, info):
                nonlocal pending_stream
                if event.endswith((".connect_tcp.complete", ".connect_unix_socket.complete", ".start_tls.complete")):
                    pending_stream = info.get("return_value")
                if event in ("http11.send_request_headers.started", "http2.send_request_headers.started"):
                    if info["request"].method != b"CONNECT":
                        handshake.deadline = float("inf")
                        pending_stream = None  # The HTTP connection now owns it.
                if original_trace:
                    await original_trace(event, info)

            request.extensions["trace"] = trace
            try:
                return await super().send(request, **kwargs)
            except BaseException:
                # httpcore may not yet own a socket if SOCKS negotiation is
                # cancelled. Closing the client pool alone misses that socket.
                if pending_stream is not None:
                    with CancelScope(shield=True):
                        try:
                            await pending_stream.aclose()
                        except Exception as exc:
                            log.warning(f"Handshake socket cleanup failed: {type(exc).__name__}")
                if handshake.cancel_called:
                    raise httpx.ConnectTimeout(
                        f"Upstream connection/proxy/TLS handshake exceeded {self.handshake_timeout:g}s",
                        request=request,
                    ) from None
                raise
            finally:
                if original_trace is None:
                    request.extensions.pop("trace", None)
                else:
                    request.extensions["trace"] = original_trace


@dataclass
class _ClientLease:
    client: httpx.AsyncClient
    users: int = 0
    retired: bool = False


# Each application loop owns its clients; never share transports across loops.
_client_pools = WeakKeyDictionary()


async def _safe_aclose(client):
    with CancelScope(shield=True):
        try:
            await client.aclose()
        except Exception as exc:
            log.warning(f"HTTP client cleanup failed: {type(exc).__name__}")


async def get_client_kwargs(timeout=30.0, **kwargs):
    options = {"timeout": timeout, **kwargs}
    proxy = await get_proxy_config()
    if proxy and "proxy" not in options:
        options["proxy"] = proxy
    return options


@asynccontextmanager
async def _lease_client(*, timeout, streaming=False, handshake_timeout=None, **kwargs):
    options = await get_client_kwargs(timeout=timeout, **kwargs)
    factory = _StreamingClient if streaming else httpx.AsyncClient
    if streaming:
        options["handshake_timeout"] = handshake_timeout
    # Explicit transports, hooks, certificates etc. belong only to this caller.
    if kwargs:
        client = factory(**options)
        try:
            yield client
        finally:
            await _safe_aclose(client)
        return

    pool = _client_pools.setdefault(asyncio.get_running_loop(), {})
    proxy = options.get("proxy", "")
    timeout_key = tuple(httpx.Timeout(timeout).as_dict().items())
    key = (proxy, streaming, handshake_timeout, timeout_key)
    # Construction may reject an invalid proxy. Keep the old pool managed until
    # its replacement exists, so failure cannot orphan idle sockets.
    entry = pool.get(key)
    if entry is None or entry.client.is_closed:
        entry = pool[key] = _ClientLease(factory(**options))
    # No awaits between lookup, retirement and lease acquisition. A proxy or
    # handshake setting change must not close another request's active stream.
    stale = []
    for old_key, old in list(pool.items()):
        if old_key[0] != proxy or (streaming and old_key[1] and old_key[2] != handshake_timeout):
            del pool[old_key]
            old.retired = True
            if not old.users:
                stale.append(old.client)
    entry.users += 1
    try:
        for client in stale:
            await _safe_aclose(client)
        yield entry.client
    finally:
        entry.users -= 1
        if entry.retired and not entry.users:
            await _safe_aclose(entry.client)


async def close_shared_clients():
    """Retire this loop's pool; active streams close upon their final release."""
    pool = _client_pools.pop(asyncio.get_running_loop(), {})
    for entry in pool.values():
        entry.retired = True
        if not entry.users:
            await _safe_aclose(entry.client)


@asynccontextmanager
async def get_client(timeout=30.0, **kwargs):
    async with _lease_client(timeout=timeout, **kwargs) as client:
        yield client


@asynccontextmanager
async def get_streaming_client(timeout=None, **kwargs):
    handshake_timeout = (
        await get_stream_handshake_timeout_seconds()
        if await get_stream_handshake_timeout_enabled() else None
    )
    if timeout is None:
        timeout = httpx.Timeout(None, connect=handshake_timeout)
    async with _lease_client(timeout=timeout, streaming=True,
                             handshake_timeout=handshake_timeout, **kwargs) as client:
        yield client


# 通用的异步方法
async def get_async(
    url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 30.0, **kwargs
) -> httpx.Response:
    """通用异步GET请求"""
    async with get_client(timeout=timeout) as client:
        return await client.get(url, headers=headers, **kwargs)


async def post_async(
    url: str,
    data: Any = None,
    json: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 900.0,
    **kwargs,
) -> httpx.Response:
    """通用异步POST请求"""
    async with get_client(timeout=timeout) as client:
        return await client.post(url, data=data, json=json, headers=headers, **kwargs)


# 调试用：设为 True 时所有流式请求都返回 429
_MOCK_STREAM_429 = False

async def stream_post_async(
    url: str,
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
    **kwargs,
):
    """流式异步POST请求"""
    if _MOCK_STREAM_429:
        log.warning(f"[MOCK] stream_post_async: 返回模拟429错误")
        yield Response(
            content=json.dumps({"error": {"code": 429, "message": "mock rate limit", "status": "RESOURCE_EXHAUSTED"}}),
            status_code=429,
        )
        return

    async with get_streaming_client(**kwargs) as client:
        async with client.stream("POST", url, json=body, headers=headers) as r:
            # 错误直接返回
            if r.status_code != 200:
                yield Response(await r.aread(), r.status_code, dict(r.headers))
                return

            # 如果native=True，直接返回bytes流
            if native:
                async for chunk in r.aiter_bytes():
                    yield chunk
            else:
                # 通过aiter_lines转化成str流返回
                async for line in r.aiter_lines():
                    yield line
