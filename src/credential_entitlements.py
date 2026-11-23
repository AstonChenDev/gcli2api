"""Refresh upstream entitlements without putting metadata IO on generation paths."""

import asyncio
import json
import math
import time
import uuid
from contextlib import asynccontextmanager

from anyio import CancelScope
from sqlalchemy import or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config import get_antigravity_api_url
from log import log
from src.credential_refresh import refresh_stored
from src.credential_tier import parse_tier
from src.google_oauth_api import Credentials
from src.httpx_client import post_async
from src.storage.database import (
    AntigravityCredential, CredentialEntitlement, get_database, get_cooldown_group,
    MODEL_NOT_FOUND_TTL, catalog_model_key, catalog_models, _row_to_credential_dict,
)
from src.utils import ANTIGRAVITY_USER_AGENT

REFRESH_SECONDS = 300
RETRY_SECONDS = 60
LEASE_SECONDS = 180


def parse_catalog(data):
    models = data.get("models")
    if not isinstance(models, dict) or not models:
        raise ValueError("Missing or malformed model catalog")
    available = []
    for name, spec in models.items():
        if not isinstance(name, str) or not name.strip() or not isinstance(spec, dict):
            raise ValueError("Malformed model entry")
        disabled = spec.get("disabled")
        if disabled is not None and not isinstance(disabled, bool):
            raise ValueError("Malformed model disabled flag")
        if not disabled and "disabled" not in name.lower():
            available.append(name)
    return sorted(available)


def _timestamp(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0 else 0


async def get_model_view(email, database=None):
    """Read only public panel fields; never return credentials or raw snapshots."""
    db = database or await get_database()
    async with db.session() as session:
        row = (await session.execute(select(
            AntigravityCredential.email, AntigravityCredential.note,
            AntigravityCredential.tier, AntigravityCredential.disabled,
            AntigravityCredential.model_cooldowns, CredentialEntitlement.snapshot,
            CredentialEntitlement.next_refresh, CredentialEntitlement.lease_until,
        ).outerjoin(CredentialEntitlement, CredentialEntitlement.email == AntigravityCredential.email)
            .where(AntigravityCredential.email == email))).mappings().one_or_none()
    if row is None:
        return None
    try:
        snapshot = json.loads(row['snapshot'] or '{}')
        if not isinstance(snapshot, dict):
            snapshot = {}
    except (TypeError, ValueError):
        snapshot = {}
    try:
        cooldowns = json.loads(row['model_cooldowns'] or '{}')
        if not isinstance(cooldowns, dict):
            cooldowns = {}
    except (TypeError, ValueError):
        cooldowns = {}
    now = time.time()
    raw_models = catalog_models(row['snapshot'])
    known = isinstance(raw_models, list) and all(isinstance(m, str) for m in raw_models)
    models = sorted(set(m for m in raw_models if m and 'disabled' not in m.lower())) if known else []
    checked_at = _timestamp(snapshot.get('models_checked_at'))
    attempted_at = _timestamp(snapshot.get('attempted_at'))
    # Expose categories, not arbitrary exception messages or snapshot contents.
    errors = snapshot.get('errors', [])
    if not isinstance(errors, list):
        errors = []
    failed = sorted({e.split(':', 1)[0] for e in errors
                     if isinstance(e, str) and e.split(':', 1)[0] in ('auth', 'tier', 'models')})
    return {
        'email': row['email'], 'note': row['note'] or '', 'tier': row['tier'],
        'disabled': bool(row['disabled']), 'models_known': known,
        'models': [{'name': m, 'quota_cooldown_until': until if until > now else None}
                   for m in models
                   for until in [_timestamp(cooldowns.get(get_cooldown_group(m)))]],
        'checked_at': checked_at or None, 'last_attempt_at': attempted_at or None,
        'stale': not known or not checked_at or now - checked_at >= REFRESH_SECONDS,
        'refresh_in_progress': _timestamp(row['lease_until']) > now,
        'retry_after_seconds': max(0, math.ceil(max(attempted_at + RETRY_SECONDS,
                                                   _timestamp(row['lease_until'])) - now)),
        'failed_checks': failed,
    }


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


def _next_tier_check(snapshot):
    # Both verification paths reserve their order under the same credential lock.
    sequence = snapshot.get("tier_check_sequence", 0) + 1
    snapshot["tier_check_sequence"] = sequence
    return sequence


async def begin_manual_tier_check(email, database):
    async with locked(database, email) as (cred, state):
        if cred is None:
            return None
        snapshot = json.loads(state.snapshot)
        sequence = _next_tier_check(snapshot)
        state.snapshot = json.dumps(snapshot)
        return sequence


async def queue_refresh(email, database=None, *, expected_revision=None):
    """404 is a refresh hint, never proof that the whole account is invalid."""
    db = database or await get_database()
    async with locked(db, email) as (cred, state):
        if cred is None or (expected_revision is not None and cred.credential_revision != expected_revision):
            return False
        snapshot = json.loads(state.snapshot)
        earliest = max(time.time(), snapshot.get("attempted_at", 0) + RETRY_SECONDS)
        state.next_refresh = min(state.next_refresh, earliest)
    return True


async def mark_model_unavailable(email, model, access_token, database=None, *, expected_revision=None):
    """Quarantine a model, without disabling the credential or clearing its catalog."""
    db = database or await get_database()
    now = time.time()
    async with locked(db, email) as (cred, state):
        if cred is None:
            return
        if expected_revision is not None:
            if cred.credential_revision != expected_revision:
                return
        elif cred.access_token != access_token:
            return
        snapshot = json.loads(state.snapshot)
        rejected = snapshot.get("model_not_found") or {}
        rejected = {name: seen for name, seen in rejected.items()
                    if isinstance(seen, (int, float)) and now - MODEL_NOT_FOUND_TTL < seen <= now}
        rejected[catalog_model_key(model)] = now
        snapshot["model_not_found"] = dict(list(rejected.items())[-128:])
        state.snapshot = json.dumps(snapshot)
        earliest = max(now, snapshot.get("attempted_at", 0) + RETRY_SECONDS)
        state.next_refresh = min(state.next_refresh, earliest)


async def refresh_one(email, database=None):
    db = database or await get_database()
    now, token = time.time(), uuid.uuid4().hex
    try:
        return await _refresh_one(db, email, now, token)
    except asyncio.CancelledError:
        # Release only our lease, even if the caller has already disconnected.
        with CancelScope(shield=True):
            async with db.session() as session:
                await session.execute(update(CredentialEntitlement).where(
                    CredentialEntitlement.email == email, CredentialEntitlement.lease_token == token
                ).values(lease_token="", lease_until=0, next_refresh=now + RETRY_SECONDS))
        raise


async def _refresh_one(db, email, now, token):
    async with locked(db, email) as (cred, state):
        if cred is None:
            return False
        snapshot = json.loads(state.snapshot)
        if state.lease_until > now or state.next_refresh > now or snapshot.get("attempted_at", 0) + RETRY_SECONDS > now:
            return False
        credentials = Credentials.from_dict(_row_to_credential_dict(cred))
        state.lease_token, state.lease_until = token, now + LEASE_SECONDS
        sequence = _next_tier_check(snapshot)
        snapshot["attempted_at"] = now
        state.snapshot = json.dumps(snapshot)

    changes, errors, refreshed = {}, [], False
    try:
        # No database transaction or global lock is held across network IO.
        if not credentials.access_token or credentials.is_expired():
            credentials = await refresh_stored(db, email, credentials)
            refreshed = True
        base = await get_antigravity_api_url()
        for endpoint, body, parser, field in (
            ("loadCodeAssist", {"metadata": {"ideType": "ANTIGRAVITY"}}, parse_tier, "tier"),
            ("fetchAvailableModels", {}, parse_catalog, "models"),
        ):
            try:
                for attempt in range(2):
                    response = await post_async(
                        credential_email=email, credential_revision=credentials.credential_revision,
                        url=f"{base.rstrip('/')}/v1internal:{endpoint}", json=body, timeout=20,
                        headers={"Authorization": f"Bearer {credentials.access_token}",
                                 "User-Agent": ANTIGRAVITY_USER_AGENT,
                                 "Content-Type": "application/json", "requestType": "agent"},
                    )
                    if response.status_code == 401 and not refreshed and attempt == 0:
                        credentials = await refresh_stored(db, email, credentials)
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
        if cred is None or state.lease_token != token or cred.credential_revision != credentials.credential_revision:
            return False
        current = json.loads(state.snapshot)
        if "models" in changes and "model_not_found" in current:
            # A refresh only supersedes failures seen before it started.
            current["model_not_found"] = {
                name: seen for name, seen in current["model_not_found"].items() if seen > now
            }
        if "tier" in changes:
            if sequence < current.get("applied_tier_check", 0):
                # A later check already verified the tier. The catalog is independent.
                del changes["tier"], changes["tier_checked_at"]
            else:
                current["applied_tier_check"] = sequence
        current.update(changes, errors=errors)
        state.snapshot = json.dumps(current)
        state.lease_token, state.lease_until = "", 0
        finished_at = time.time()
        state.next_refresh = finished_at + (RETRY_SECONDS if errors else REFRESH_SECONDS)
        if any(seen > now for seen in current.get("model_not_found", {}).values()):
            state.next_refresh = min(state.next_refresh, max(finished_at, now + RETRY_SECONDS))
        if "tier" in changes:
            cred.tier = changes["tier"]
            if changes["tier"] == "free":
                if not cred.disabled:
                    auto_disabled_id = cred.id
                cred.disabled = True
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
