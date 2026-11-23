"""Persistent proxy management and per-credential egress selection."""
import asyncio
import socket
import ssl
import time
import uuid
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select, text

from src.credential_cooldown import CredentialReplaced
from src.storage.database import AntigravityCredential, ProxyEndpoint, get_database


_PROBE_URL = "https://connectivitycheck.gstatic.com/generate_204"
_PROBE_TIMEOUT = 12


def _probe_failure(exc):
    if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
        return "测试超时，请检查代理是否在线，或稍后重试。"
    if isinstance(exc, httpx.ProxyError):
        reason = str(exc).lower()
        if reason.startswith("407") or "invalid username/password" in reason:
            return "代理认证失败，请检查用户名和密码。"
        if reason.startswith("requested ") and "from proxy server" in reason:
            return "SOCKS5 认证方式不匹配，请检查代理的认证设置。"
        return "代理未能建立到 Google 的隧道，请检查代理协议和访问权限。"
    cause = exc
    while cause is not None:
        if isinstance(cause, ssl.SSLError):
            return "TLS 握手失败，请检查证书和代理协议。"
        if isinstance(cause, socket.gaierror):
            return "无法解析主机名，请检查代理地址和 DNS。"
        if isinstance(cause, ConnectionRefusedError):
            return "连接被拒绝，请检查代理地址、端口及服务状态。"
        cause = cause.__cause__ or cause.__context__
    return "连接失败，请检查代理地址、协议及网络连通性。"


async def check_proxy(db, proxy_id):
    async with db.session() as session:
        url = await session.scalar(select(ProxyEndpoint.url).where(ProxyEndpoint.id == proxy_id))
    if url is None:
        raise LookupError("代理不存在")

    start = time.monotonic()
    ok = False
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT):
            async with httpx.AsyncClient(proxy=url, trust_env=False, timeout=_PROBE_TIMEOUT) as client:
                async with client.stream("GET", _PROBE_URL) as response:
                    status = response.status_code
        ok = status == 204
        if ok:
            message = "已通过代理连接 Google"
        elif status == 407:
            message = "代理认证失败，请检查用户名和密码。"
        else:
            message = f"连通性检测返回 HTTP {status}，预期为 204，请检查网络拦截或稍后重试。"
    except Exception as exc:
        # Network libraries can include proxy credentials in exception text.
        message = _probe_failure(exc)
    return {"ok": ok, "message": message, "elapsed_ms": round((time.monotonic() - start) * 1000)}


def validate_proxy_url(value):
    value = value.strip()
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in {"http", "https", "socks5", "socks5h"}
                or not parsed.hostname or not parsed.port
                or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
                or any(c.isspace() or ord(c) < 32 for c in value)):
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError("请输入有效代理地址：协议://[用户名:密码@]主机:端口") from None
    return value


def public_proxy(row, bound_count=0):
    parsed = urlsplit(row.url)
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    return {"id": row.id, "name": row.name, "address": f"{parsed.scheme}://{host}:{parsed.port}",
            "has_auth": parsed.username is not None, "bound_count": bound_count}


async def list_proxies(db):
    async with db.session() as s:
        rows = (await s.scalars(select(ProxyEndpoint).order_by(ProxyEndpoint.created_at, ProxyEndpoint.id))).all()
        counts = dict((await s.execute(select(AntigravityCredential.proxy_id, func.count())
            .where(AntigravityCredential.proxy_id.is_not(None)).group_by(AntigravityCredential.proxy_id))).all())
        return [public_proxy(row, counts.get(row.id, 0)) for row in rows]


async def _bound_count(session, proxy_id):
    return await session.scalar(select(func.count()).select_from(AntigravityCredential)
                                .where(AntigravityCredential.proxy_id == proxy_id))


async def save_proxy(db, name, url=None, proxy_id=None):
    name = name.strip()
    if not name:
        raise ValueError("代理名称不能为空")
    if url is not None:
        url = validate_proxy_url(url)
    async with db.session() as s:
        if db.backend_type == "sqlite":
            await s.execute(text("BEGIN IMMEDIATE"))
        if proxy_id:
            row = await s.scalar(select(ProxyEndpoint).where(ProxyEndpoint.id == proxy_id).with_for_update())
            if row is None:
                raise LookupError("代理不存在")
            row.name = name
            if url is not None:
                row.url = url
        else:
            if not url:
                raise ValueError("请填写代理地址")
            row = ProxyEndpoint(id=uuid.uuid4().hex, name=name, url=url)
            s.add(row)
        return public_proxy(row, await _bound_count(s, proxy_id) if proxy_id else 0)


async def delete_proxy(db, proxy_id):
    async with db.session() as s:
        if db.backend_type == "sqlite":
            await s.execute(text("BEGIN IMMEDIATE"))
        row = await s.scalar(select(ProxyEndpoint).where(ProxyEndpoint.id == proxy_id).with_for_update())
        if row is None:
            raise LookupError("代理不存在")
        count = await _bound_count(s, proxy_id)
        if count:
            raise ValueError("该代理仍有凭证绑定，请先为这些凭证切换或取消代理")
        await s.delete(row)


async def bind_proxy(db, email, proxy_id):
    async with db.session() as s:
        if db.backend_type == "sqlite":
            await s.execute(text("BEGIN IMMEDIATE"))
        # Match the proxy deletion lock so deletion cannot race a new binding.
        if proxy_id is not None:
            proxy = await s.scalar(select(ProxyEndpoint).where(ProxyEndpoint.id == proxy_id).with_for_update())
            if proxy is None:
                raise LookupError("代理不存在")
        cred = await s.scalar(select(AntigravityCredential).where(AntigravityCredential.email == email).with_for_update())
        if cred is None:
            raise LookupError("凭证不存在")
        cred.proxy_id = proxy_id
        return {"email": email, "proxy_id": proxy_id}


async def registered_email_for_import(email):
    """Only existing identities can have a dedicated route before an import is saved."""
    if not email:
        return None
    db = await get_database()
    async with db.session() as s:
        # Retain the stored key (including case) for older accounts and their bindings.
        key = AntigravityCredential.email
        return await s.scalar(select(key).where(func.lower(key) == email.strip().lower())
                              .order_by((key == email).desc(), (key == email.strip().lower()).desc(), key).limit(1))


async def resolve_proxy(email, expected_revision=None):
    """Resolve the stored credential and its current route in one snapshot."""
    db = await get_database()
    async with db.session() as s:
        row = (await s.execute(
            select(AntigravityCredential.proxy_id, AntigravityCredential.credential_revision, ProxyEndpoint.url)
            .select_from(AntigravityCredential)
            .outerjoin(ProxyEndpoint, AntigravityCredential.proxy_id == ProxyEndpoint.id)
            .where(AntigravityCredential.email == email)
        )).one_or_none()
    if row is None:
        raise httpx.TransportError("凭证已删除，已停止发送本次请求")
    if expected_revision is not None and row.credential_revision != expected_revision:
        raise CredentialReplaced()
    if row.proxy_id is None:
        return None
    if not row.url:
        raise ValueError("凭证绑定的代理不存在，请重新选择代理")
    return validate_proxy_url(row.url)
