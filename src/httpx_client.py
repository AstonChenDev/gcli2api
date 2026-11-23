"""
通用的HTTP客户端模块
为所有需要使用httpx的模块提供统一的客户端配置和方法
保持通用性，不与特定业务逻辑耦合

- 共享 AsyncClient（按代理配置指纹缓存），复用 TCP/TLS 连接，
  避免每请求新建客户端造成的握手开销
- 每个出口独立复用连接；只回收空闲池，代理热更新不打断已有流
- 超时等请求级参数在每次请求时传入，不受客户端实例缓存影响
"""

import asyncio
from codecs import BOM_UTF8
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import httpx
import json
from fastapi import Response
from anyio import CancelScope

from config import get_proxy_config
from log import log
from src.credential_proxy import resolve_proxy

# ====================== 共享客户端 ======================

# Each proxy owns a pool. Keep at most 32 idle pools; active streams are never evicted.
_shared_clients: List[Tuple[str, httpx.AsyncClient]] = []
_shared_clients_lock = asyncio.Lock()
_client_users = {}
_MAX_IDLE_POOLS = 32


def decoded_response_headers(headers) -> Dict[str, str]:
    """httpx has decoded the body; let Starlette size the downstream bytes."""
    excluded = {
        "content-encoding", "content-length", "transfer-encoding", "connection",
        "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade",
    }
    excluded.update(value.strip().lower() for value in headers.get("connection", "").split(","))
    return {key: value for key, value in headers.items() if key.lower() not in excluded}


def retry_after_headers(headers) -> Dict[str, str]:
    """Keep only the end-to-end retry hint when rebuilding an embedded error."""
    if "retry-after" in headers:
        value = decoded_response_headers(headers).get("retry-after")
        if value:
            return {"retry-after": value}
    return {}


def strip_json_bom(response: httpx.Response) -> httpx.Response:
    if response.content.startswith(BOM_UTF8):
        return httpx.Response(response.status_code, content=response.content[len(BOM_UTF8):],
                              headers=decoded_response_headers(response.headers))
    return response


async def _build_client(proxy_url: Optional[str]) -> httpx.AsyncClient:
    kwargs: Dict[str, Any] = {"timeout": None}
    if proxy_url:
        kwargs.update(proxy=proxy_url, trust_env=False)
    return httpx.AsyncClient(**kwargs)


async def _safe_aclose(client):
    try:
        await client.aclose()
    except Exception as exc:
        log.warning(f"关闭旧 httpx 客户端失败: {type(exc).__name__}")


async def _get_shared_client(credential_email=None, acquire=False, credential_revision=None):
    proxy_url = await resolve_proxy(credential_email, credential_revision) if credential_email else None
    if proxy_url is None:
        proxy_url = await get_proxy_config()
    fingerprint = proxy_url or ""
    async with _shared_clients_lock:
        client = next((c for fp, c in _shared_clients if fp == fingerprint), None)
        if client is None:
            client = await _build_client(proxy_url)
            _shared_clients.append((fingerprint, client))
        if acquire:
            _client_users[client] = _client_users.get(client, 0) + 1
        return client


@asynccontextmanager
async def _lease_client(credential_email=None, credential_revision=None):
    client = await _get_shared_client(credential_email=credential_email, acquire=True, credential_revision=credential_revision)
    try:
        yield client
    except httpx.RequestError as exc:
        if credential_email:
            # Transport exceptions can include a proxy's username or password.
            raise type(exc)(f"凭证出口连接失败 ({type(exc).__name__})") from None
        raise
    finally:
        with CancelScope(shield=True):
            stale = []
            async with _shared_clients_lock:
                _client_users[client] = max(0, _client_users.get(client, 1) - 1)
                idle = [(fp, c) for fp, c in _shared_clients if not _client_users.get(c)]
                for entry in idle[:-_MAX_IDLE_POOLS]:
                    _shared_clients.remove(entry)
                    _client_users.pop(entry[1], None)
                    stale.append(entry[1])
            for old in stale:
                await _safe_aclose(old)


async def close_shared_clients() -> None:
    """关闭所有共享客户端（应用关闭时调用）"""
    async with _shared_clients_lock:
        clients = [c for _, c in _shared_clients]
        _shared_clients.clear()
        _client_users.clear()
    for client in clients:
        await _safe_aclose(client)


# ====================== 客户端获取（兼容旧接口） ======================

@asynccontextmanager
async def get_client(
    timeout: float = 30.0, **kwargs
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """获取配置好的异步HTTP客户端（共享复用）"""
    async with _lease_client(kwargs.pop("credential_email", None), kwargs.pop("credential_revision", None)) as client:
        yield client


@asynccontextmanager
async def get_streaming_client(
    timeout: float = None, **kwargs
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """获取用于流式请求的HTTP客户端（共享复用）"""
    async with _lease_client(kwargs.pop("credential_email", None), kwargs.pop("credential_revision", None)) as client:
        yield client


# ====================== 异步请求方法 ======================

async def get_async(
    url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 30.0, **kwargs
) -> httpx.Response:
    """通用异步GET请求"""
    async with _lease_client(kwargs.pop("credential_email", None), kwargs.pop("credential_revision", None)) as client:
        return await client.get(url, headers=headers, timeout=timeout, **kwargs)


async def post_async(
    url: str,
    data: Any = None,
    json: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 900.0,
    **kwargs,
) -> httpx.Response:
    """通用异步POST请求"""
    async with _lease_client(kwargs.pop("credential_email", None), kwargs.pop("credential_revision", None)) as client:
        return await client.post(
            url, data=data, json=json, headers=headers, timeout=timeout, **kwargs
        )


# 调试用：设为 True 时所有流式请求都返回 429
_MOCK_STREAM_429 = False


async def _sse_lines(response: httpx.Response):
    """Split only on CR, LF or CRLF. Other Unicode separators are JSON text."""
    # SSE is UTF-8. Its optional initial BOM can span multiple network reads.
    response.encoding = "utf-8-sig"
    pending = []
    after_cr = False
    async for text in response.aiter_text():
        if after_cr:
            text = text.removeprefix("\n")
        after_cr = text.endswith("\r")
        lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        for line in lines[:-1]:
            if pending:
                pending.append(line)
                line = "".join(pending)
                pending.clear()
            yield line
        if lines[-1]:
            pending.append(lines[-1])
    if pending:
        yield "".join(pending)


async def stream_post_async(
    url: str,
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
    retry_headers: Optional[Dict[str, str]] = None,
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

    async with _lease_client(kwargs.pop("credential_email", None), kwargs.pop("credential_revision", None)) as client:
        async with client.stream(
            "POST", url, json=body, headers=headers, **kwargs
        ) as r:
            # 错误直接返回
            if r.status_code != 200:
                yield Response(await r.aread(), r.status_code, decoded_response_headers(r.headers))
                return
            if retry_headers is not None:
                retry_headers.update(retry_after_headers(r.headers))

            # 如果native=True，直接返回bytes流
            if native:
                async for chunk in r.aiter_bytes():
                    yield chunk
            else:
                is_sse = r.headers.get("content-type", "").partition(";")[0].strip().lower() == "text/event-stream"
                async for line in _sse_lines(r):
                    # Only data fields contribute to an SSE payload. Keep comments
                    # for heartbeats and blank lines for multi-line record boundaries.
                    if is_sse and line and line != "data" and not line.startswith(("data:", ":")):
                        continue
                    yield line
