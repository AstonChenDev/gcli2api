"""Refresh upstream entitlements without putting metadata IO on generation paths."""

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager

from sqlalchemy import or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import get_antigravity_api_url
from log import log
from src.credential_tier import normalize_credential_tier
from src.google_oauth_api import Credentials
from src.httpx_client import post_async
from src.storage.database import (
    AntigravityCredential, CredentialEntitlement, get_database, _row_to_credential_dict,
)
from src.utils import ANTIGRAVITY_USER_AGENT

REFRESH_SECONDS = 300
RETRY_SECONDS = 60
LEASE_SECONDS = 180


def parse_tier(data):
    # currentTier is often free-tier even for paying Ultra customers.
    if not isinstance(data, dict):
        raise ValueError("Malformed entitlement response")
    paid = data.get("paidTier")
    if paid is not None and not isinstance(paid, dict):
        raise ValueError("Malformed paid tier")
    tier = paid if paid else data.get("currentTier")
    raw = tier.get("id") if isinstance(tier, dict) else None
    if not isinstance(raw, str):
        raise ValueError("Unrecognized entitlement tier")
    raw = raw.strip().lower()
    # Disabling an account requires an explicit known Free identifier. Do not
    # guess from missing IDs, display names, or future IDs containing "free".
    if raw in ("free", "free-tier"):
        return "free"
    if not any(k in raw for k in ("pro", "ultra")):
        raise ValueError("Unrecognized entitlement tier")
    return normalize_credential_tier(raw)


def parse_catalog(data):
    models = data.get("models")
    if not isinstance(models, dict) or not models or any(not isinstance(v, dict) for v in models.values()):
        raise ValueError("Missing or malformed model catalog")
    return sorted(name for name, spec in models.items()
                  if not spec.get("disabled") and "disabled" not in name.lower())


@asynccontextmanager
async def locked(db, email):
    # The same credential -> entitlement lock order as replacement/deletion.
    async with db.session() as session:
        if db.backend_type == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        cred = await session.scalar(select(AntigravityCredential).where(
            AntigravityCredential.email == email).with_for_update())
        if cred is None:
            yield None, None
            return
        insert = pg_insert if db.backend_type == "postgresql" else sqlite_insert
        await session.execute(insert(CredentialEntitlement).values(email=email).on_conflict_do_nothing())
        state = await session.scalar(select(CredentialEntitlement).where(
            CredentialEntitlement.email == email).with_for_update())
        yield cred, state


async def queue_refresh(email, database=None):
    """404 is a refresh hint, never proof that the whole account is invalid."""
    db = database or await get_database()
    async with locked(db, email) as (cred, state):
        if cred is not None:
            snapshot = json.loads(state.snapshot)
            earliest = max(time.time(), snapshot.get("attempted_at", 0) + RETRY_SECONDS)
            state.next_refresh = min(state.next_refresh, earliest)


async def refresh_one(email, database=None):
    db = database or await get_database()
    now, token = time.time(), uuid.uuid4().hex
    async with locked(db, email) as (cred, state):
        if cred is None:
            return False
        snapshot = json.loads(state.snapshot)
        if state.lease_until > now or state.next_refresh > now or snapshot.get("attempted_at", 0) + RETRY_SECONDS > now:
            return False
        identity = (cred.id, cred.refresh_token, cred.client_id, cred.project_id)
        credentials = Credentials.from_dict(_row_to_credential_dict(cred))
        state.lease_token, state.lease_until = token, now + LEASE_SECONDS
        snapshot["attempted_at"] = now
        state.snapshot = json.dumps(snapshot)

    changes, errors, refreshed = {}, [], False
    try:
        # No database transaction or global lock is held across network IO.
        if not credentials.access_token or credentials.is_expired():
            await credentials.refresh()
            refreshed = True
        base = await get_antigravity_api_url()
        for endpoint, body, parser, field in (
            ("loadCodeAssist", {"metadata": {"ideType": "ANTIGRAVITY"}}, parse_tier, "tier"),
            ("fetchAvailableModels", {}, parse_catalog, "models"),
        ):
            try:
                for attempt in range(2):
                    response = await post_async(
                        url=f"{base.rstrip('/')}/v1internal:{endpoint}", json=body, timeout=20,
                        headers={"Authorization": f"Bearer {credentials.access_token}",
                                 "User-Agent": ANTIGRAVITY_USER_AGENT,
                                 "Content-Type": "application/json", "requestType": "agent"},
                    )
                    if response.status_code == 401 and not refreshed and attempt == 0:
                        await credentials.refresh()
                        refreshed = True
                        continue
                    response.raise_for_status()
                    changes[field] = parser(response.json())
                    changes[field + "_checked_at"] = time.time()
                    break
            except Exception as exc:
                # Never retain raw responses, tokens, prompt templates or URLs.
                errors.append(field + ":" + type(exc).__name__)
    except Exception as exc:
        errors.append("auth:" + type(exc).__name__)

    auto_disabled_id = None
    async with locked(db, email) as (cred, state):
        if cred is None or state.lease_token != token or identity != (
            cred.id, cred.refresh_token, cred.client_id, cred.project_id
        ):
            return False
        current = json.loads(state.snapshot)
        current.update(changes, errors=errors)
        state.snapshot = json.dumps(current)
        state.lease_token, state.lease_until = "", 0
        state.next_refresh = time.time() + (RETRY_SECONDS if errors else REFRESH_SECONDS)
        if "tier" in changes:
            cred.tier = changes["tier"]
            if changes["tier"] == "free":
                if not cred.disabled:
                    auto_disabled_id = cred.id
                cred.disabled = True
        if refreshed:
            cred.access_token = credentials.access_token
            if credentials.expires_at:
                cred.expiry = credentials.expires_at.isoformat()
        # A later upgrade updates membership but never silently re-enables an
        # account. Notes, project IDs and existing cooldowns stay untouched.
        if changes or refreshed:
            cred.updated_at = time.time()
    if auto_disabled_id is not None:
        log.info(f"权益刷新：凭证 ID {auto_disabled_id} 已确认为 Free，会员等级已更新并自动禁用")
    return not errors


async def refresh_due(database=None):
    db = database or await get_database()
    now = time.time()
    async with db.session() as session:
        emails = list(await session.scalars(select(AntigravityCredential.email).outerjoin(
            CredentialEntitlement, CredentialEntitlement.email == AntigravityCredential.email
        ).where(
            or_(CredentialEntitlement.email.is_(None),
                (CredentialEntitlement.next_refresh <= now) & (CredentialEntitlement.lease_until <= now))
        ).order_by(CredentialEntitlement.next_refresh.asc().nullsfirst(), AntigravityCredential.id).limit(2)))
    return await asyncio.gather(*(refresh_one(email, db) for email in emails))


async def refresh_loop():
    while True:
        try:
            await refresh_due()
        except Exception as exc:
            log.warning(f"权益刷新暂时失败: {type(exc).__name__}")
        await asyncio.sleep(5)
