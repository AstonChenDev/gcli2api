"""Coalesce OAuth refreshes for a stored credential across application workers."""
import asyncio
import json
import time
import uuid

from anyio import CancelScope
from sqlalchemy import select, update

from log import log
from src.google_oauth_api import Credentials, TokenError
from src.storage.database import AntigravityCredential, _row_to_credential_dict


# A failed refresh wakes its existing waiters with an error. A cancelled owner
# instead releases the lease for one of the surviving requests to take over.
_FAILED_OWNER = "failed"


async def refresh_stored(db, email, previous, *, disable_on_rejection=False):
    token = uuid.uuid4().hex
    waited_for = None
    table = AntigravityCredential
    # Election and publication use short transactions. No DB lock is held during OAuth.
    while True:
        async with db.session() as s:
            row = await s.scalar(select(table).where(table.email == email))
        if row is None or (previous.credential_revision is not None and row.credential_revision != previous.credential_revision):
            raise TokenError("刷新期间凭证已被替换")
        current = Credentials.from_dict(_row_to_credential_dict(row))
        # The completed owner's marker also covers OAuth refresh-token rotation.
        if waited_for == (row.id, row.refresh_owner) and row.refresh_until == 0:
            return current
        rotated = row.refresh_token != previous.refresh_token
        if rotated and previous.credential_revision is None:
            raise TokenError("刷新期间凭证已被替换")
        changed = (current.access_token, current.expires_at) != (previous.access_token, previous.expires_at)
        if (rotated or changed) and not current.is_expired():
            return current
        now = time.time()
        if row.refresh_until > now:
            waited_for = (row.id, row.refresh_owner)
            await asyncio.sleep(0.1)
            continue
        if waited_for and row.refresh_owner == _FAILED_OWNER:
            raise TokenError("本次共享令牌刷新未成功")
        same = update(table).where(
            table.id == row.id, table.email == email, table.credential_revision == row.credential_revision,
            table.refresh_token == row.refresh_token,
            table.client_id == row.client_id, table.client_secret == row.client_secret,
            table.access_token == row.access_token, table.expiry == row.expiry)
        async with db.session() as s:
            claim = await s.execute(same.where(table.refresh_until <= now, table.refresh_owner == row.refresh_owner).values(refresh_owner=token, refresh_until=now + 45))
        if claim.rowcount:
            break

    try:
        async with asyncio.timeout(30):
            await current.refresh()
        async with db.session() as s:
            saved = await s.execute(same.where(table.refresh_owner == token).values(
                access_token=current.access_token, refresh_token=current.refresh_token,
                expiry=current.expires_at.isoformat() if current.expires_at else "",
                refresh_until=0))
        if not saved.rowcount:
            raise TokenError("令牌刷新结果已过期")
        return current
    except BaseException as exc:
        cancelled = isinstance(exc, asyncio.CancelledError)
        status_code = getattr(exc, "status_code", None)
        disable = disable_on_rejection and isinstance(exc, TokenError) and (
            not current.refresh_token or _is_permanent_refresh_failure(str(exc), status_code))
        outcome = {"refresh_owner": "" if cancelled else _FAILED_OWNER, "refresh_until": 0}
        with CancelScope(shield=True):
            async with db.session() as s:
                owner = table.refresh_owner == token
                if disable:
                    # Match the exact row and token snapshot that Google rejected.
                    values = {"disabled": True}
                    if current.refresh_token:
                        values.update(error_codes=json.dumps([status_code or 400]), error_messages="{}")
                    result = await s.execute(same.where(owner).values(**values, **outcome))
                    if result.rowcount:
                        log.warning(f"永久失效凭证已禁用: {email}")
                # Also release if a concurrent edit invalidated the snapshot above.
                await s.execute(update(table).where(table.email == email, owner).values(**outcome))
        raise


def _is_permanent_refresh_failure(error_msg: str, status_code: int | None = None) -> bool:
    """判断是否是凭证永久失效（客户端错误/明确失效错误）"""
    if status_code is not None:
        if status_code in (400, 401, 403):
            return True
        if status_code in (500, 502, 503, 504):
            return False
        if status_code == 429:
            return False

    permanent_patterns = (
        "invalid_grant",
        "refresh_token_expired",
        "invalid_refresh_token",
        "unauthorized_client",
        "access_denied",
    )
    error_msg_lower = error_msg.lower()
    return any(p in error_msg_lower for p in permanent_patterns)
