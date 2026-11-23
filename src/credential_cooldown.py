"""Account-local 499 circuit breaker; persistent, atomic and independent of logs.

499 is NOT proof of rate limiting. Only a downstream cancellation after a long
wait with no upstream event/output qualifies. Observe mode never blocks traffic.
Account rows serialize admission/completion across workers on SQLite/PostgreSQL.
"""
import asyncio
import json
import math
import time
import uuid
from contextlib import asynccontextmanager
from fnmatch import fnmatchcase

from anyio import CancelScope
from sqlalchemy import or_, select
from sqlalchemy.orm import load_only

from log import log
from src.storage.database import (
    AntigravityCredential, ConfigItem, Credential499State, get_cooldown_group,
    CredentialEntitlement, PoolModelUnavailable, UnsupportedCredentialModel, catalog_supports_model, dispatch_limit_wait,
    _load_json, get_database,
)

PREFIX = "cooldown_499_"
DEFAULTS = {
    PREFIX + "mode": "observe",
    PREFIX + "threshold": 3,
    PREFIX + "window_seconds": 300,
    PREFIX + "min_wait_seconds": 90,
    PREFIX + "duration_seconds": 1200,
    PREFIX + "scope": "account",
    PREFIX + "probe_concurrency": 1,
    PREFIX + "custom_families_enabled": False,
    PREFIX + "family_rules": [],
    PREFIX + "family_fallback": "default",
}
LIMITS = {
    "threshold": (1, 100), "window_seconds": (1, 86400),
    "min_wait_seconds": (0, 3600), "duration_seconds": (1, 86400),
    "probe_concurrency": (1, 32),
}
# Renewable ownership, not the account's cooldown duration.
LEASE_SECONDS = 120
RENEW_SECONDS = 30


def validate_settings(values):
    for key, value in values.items():
        if not key.startswith(PREFIX):
            continue
        name = key[len(PREFIX):]
        if name in ("mode", "scope", "family_fallback"):
            allowed = {"mode": ("off", "observe", "enforce"), "scope": ("account", "family"),
                       "family_fallback": ("default", "model")}[name]
            if value not in allowed:
                raise ValueError(f"{key} 必须选择 {' / '.join(allowed)}")
        elif name == "custom_families_enabled":
            if type(value) is not bool:
                raise ValueError("自定义模型家族开关必须是布尔值")
        elif name == "family_rules":
            validate_family_rules(value)
        elif name in LIMITS:
            lo, hi = LIMITS[name]
            if type(value) is not int or not lo <= value <= hi:
                raise ValueError(f"{key} 必须是 {lo}–{hi} 范围内的整数")
        else:
            raise ValueError(f"未知的 499 保护配置项: {key}")


def validate_family_rules(rules):
    if not isinstance(rules, list) or len(rules) > 32:
        raise ValueError("自定义模型家族必须是列表，最多 32 组")
    names = set()
    for index, rule in enumerate(rules, 1):
        if not isinstance(rule, dict) or set(rule) != {"name", "patterns"}:
            raise ValueError(f"第 {index} 组必须包含分组名称 name 和匹配列表 patterns")
        name = rule["name"]
        if (not isinstance(name, str) or not 1 <= len(name) <= 64 or name != name.strip()
                or any(ord(c) < 32 for c in name)):
            raise ValueError(f"第 {index} 组名称需为 1–64 字，不能有首尾空白或控制字符")
        if name.casefold() in names:
            raise ValueError(f"第 {index} 组名称重复：{name}")
        names.add(name.casefold())
        patterns = rule["patterns"]
        if not isinstance(patterns, list) or not 1 <= len(patterns) <= 32:
            raise ValueError(f"第 {index} 组需填写 1–32 个模型匹配规则")
        for pattern in patterns:
            if (not isinstance(pattern, str) or not 1 <= len(pattern) <= 128
                    or any(c.isspace() or ord(c) < 32 or c in "[]" for c in pattern)):
                raise ValueError(f"第 {index} 组模型规则需为 1–128 字，不含空白或方括号；支持 * 和 ?")


def resolve_family(model, cfg):
    """499-only grouping; never reuse this key for upstream quota cooldowns."""
    if cfg.get("custom_families_enabled", False):
        for rule in cfg.get("family_rules", []):
            if any(fnmatchcase(model.lower(), pattern.lower()) for pattern in rule["patterns"]):
                return "custom:" + rule["name"]
        if cfg.get("family_fallback", "default") == "model":
            return "model:" + model.lower()
        default = get_cooldown_group(model)
        return default if default in ("image", "argon", "gemini", "claude_gpt") else "model:" + model.lower()
    return get_cooldown_group(model)


def family_label(group):
    if group.startswith("custom:"):
        return group[7:] + "（自定义）"
    if group.startswith("model:"):
        return group[6:] + "（单独模型）"
    return {"*": "整个账号", "image": "图片", "argon": "Argon", "gemini": "Gemini",
            "claude_gpt": "Claude / GPT"}.get(group, group)


def _policy(cfg):
    return [cfg["mode"], cfg["scope"], cfg.get("revision", "0")]


def _breaker_deadline_wait(state, cfg, model, now):
    """Only an enforced cooling phase has a known reopening time."""
    if cfg["mode"] != "enforce" or state.get("policy") != _policy(cfg):
        return 0
    group = "*" if cfg["scope"] == "account" else resolve_family(model, cfg)
    value = state.get("groups", {}).get(group, {})
    return max(0, math.ceil(value["until"] - now)) if value.get("phase") == "cooling" else 0


async def read_settings(session):
    values = dict(DEFAULTS)
    rows = (await session.scalars(select(ConfigItem).where(ConfigItem.key.like(PREFIX + "%")))).all()
    values.update({row.key: json.loads(row.value) for row in rows})
    return {key[len(PREFIX):]: value for key, value in values.items()}


async def public_settings(database):
    async with database.session() as session:
        cfg = await read_settings(session)
    return {key: cfg[key[len(PREFIX):]] for key in DEFAULTS}


async def save_settings(database, values):
    """Commit all submitted settings and any cooldown revision in one transaction."""
    validate_settings(values)
    if not values:
        return
    async with database.config_write_session() as session:
        updates = dict(values)
        policy_keys = [key for key in (
            "mode", "scope", "custom_families_enabled", "family_rules", "family_fallback"
        ) if PREFIX + key in values]
        if policy_keys:
            old = await read_settings(session)
            if any(values[PREFIX + key] != old[key] for key in policy_keys):
                updates[PREFIX + "revision"] = uuid.uuid4().hex
        await database.write_config_values(session, updates)


def _new_group():
    return dict(phase="closed", epoch=uuid.uuid4().hex, failures=[], until=0,
                duration=0, triggered_at=0, would_block=0, trips=0)


class _CredentialNotFound(Exception):
    pass


class CooldownStore:
    def __init__(self, database, clock=time.time):
        self.database = database
        self.clock = clock
        # Retain the last policy read even when a subsequent SQL write fails.
        # A database outage must not turn observation into enforcement.
        self.last_known_mode = None
        self.concurrency_limited = False

    @asynccontextmanager
    async def locked(self, email):
        async with self.database.locked_cooldown_state(email) as (session, row):
            cfg = await read_settings(session)
            self.last_known_mode = cfg["mode"]
            state = json.loads(row.state)
            state.setdefault("leases", {})
            table = AntigravityCredential
            cred = await session.scalar(select(table).where(table.email == email).options(
                load_only(table.credential_revision, table.concurrency_limit, table.rpm_limit,
                          table.model_cooldowns, table.disabled)
            ).with_for_update())
            revision = cred.credential_revision if cred else None
            if state.get("credential_revision", "") != revision:
                # Old transports still occupy their slots, but cannot trip or reopen the replacement's gate.
                for lease in state["leases"].values():
                    lease.update(policy=None, probe=False)
                state["credential_revision"] = revision
            policy = _policy(cfg)
            if state.get("policy") != policy:
                # Keep outstanding transports for draining, fence their outcomes.
                state.update(policy=policy, groups={}, seen={})
                for lease in state["leases"].values():
                    # Regroup outstanding transports under the new rules, but
                    # retain their old policy/epoch so outcomes cannot reopen it.
                    # Pre-feature leases lack a model: conservatively drain them
                    # in every family until completion/expiry.
                    lease["family"] = resolve_family(lease["model"], cfg) if "model" in lease else None
            state["seen"] = {k: v for k, v in state["seen"].items()
                             if v > self.clock() - cfg["window_seconds"]}
            self._reap(state)
            yield session, state, cfg, cred
            row.state = json.dumps(state, separators=(",", ":"))

    def _active(self, state, group):
        return [v for v in state["leases"].values()
                if group == "*" or v["family"] is None or v["family"] == group]

    def _trip(self, state, group, cfg):
        value = state["groups"][group]
        value.update(phase="draining", epoch=uuid.uuid4().hex, failures=[], until=0,
                     duration=cfg["duration_seconds"], triggered_at=self.clock(),
                     trips=value["trips"] + 1)
        self._advance(state)

    def _advance(self, state):
        now = self.clock()
        for group, value in state["groups"].items():
            if value["phase"] == "draining" and not self._active(state, group):
                value.update(phase="cooling", until=now + value["duration"])
            if value["phase"] == "cooling" and now >= value["until"]:
                value["phase"] = "probe"

    def _reap(self, state):
        now = self.clock()
        for key, lease in list(state["leases"].items()):
            if lease["expires"] > now:
                continue
            del state["leases"][key]
            # Lost probe ownership is unknown, never proof of recovery.
            value = state["groups"].get(lease["group"])
            if value and lease["probe"] and lease["epoch"] == value["epoch"]:
                value.update(phase="draining", epoch=uuid.uuid4().hex, until=0)
        self._advance(state)

    async def admit(self, email, model, request_id, *, manual=False, expected_revision=None):
        async with self.locked(email) as (session, state, cfg, cred):
            # Recheck after cache/sticky/prefetch selection, immediately at dispatch.
            if cred and expected_revision is not None and cred.credential_revision != expected_revision:
                raise CredentialReplaced()
            snapshot = await session.scalar(select(CredentialEntitlement.snapshot).where(CredentialEntitlement.email == email))
            if catalog_supports_model(snapshot, model) is False:
                raise UnsupportedCredentialModel(model)
            quota_family = get_cooldown_group(model)
            family = resolve_family(model, cfg)
            if not cred or (not manual and cred.disabled):
                return None, 0
            now = self.clock()
            if not manual:
                remaining = json.loads(cred.model_cooldowns or "{}").get(quota_family, 0) - now
                if remaining > 0:
                    local_wait = dispatch_limit_wait(cred.concurrency_limit, cred.rpm_limit, state, now)
                    raise QuotaCooling(max(math.ceil(remaining), local_wait, _breaker_deadline_wait(state, cfg, model, now)))
            group = "*" if cfg["scope"] == "account" else family
            value = state["groups"].setdefault(group, _new_group())
            active = self._active(state, group)
            probes = sum(v["probe"] and v["epoch"] == value["epoch"] for v in active)
            blocked = value["phase"] in ("draining", "cooling") or (
                value["phase"] == "probe" and probes >= cfg["probe_concurrency"])
            if blocked:
                value["would_block"] += 1
                if cfg["mode"] == "enforce":
                    local_wait = dispatch_limit_wait(cred.concurrency_limit, cred.rpm_limit, state, now)
                    return None, max(1, math.ceil(value["until"] - now), local_wait)
            self.concurrency_limited = bool(cred.concurrency_limit)
            wait = dispatch_limit_wait(cred.concurrency_limit, cred.rpm_limit, state, now)
            if wait:
                raise DispatchLimited(wait)
            # Include retries, panel tests and recent traffic before a limit edit.
            state["dispatch_starts"] = [t for t in state.get("dispatch_starts", ()) if t > now - 60]
            state["dispatch_starts"].append(now)
            key = uuid.uuid4().hex
            lease = dict(model=model, family=family, group=group, epoch=value["epoch"],
                         probe=value["phase"] == "probe", request_id=request_id,
                         expires=self.clock() + LEASE_SECONDS, started=self.clock(),
                         min_wait=cfg["min_wait_seconds"], policy=state["policy"])
            state["leases"][key] = lease
            return key, 0

    async def renew(self, email, key):
        async with self.locked(email) as (_, state, _, cred):
            self.concurrency_limited = bool(cred and cred.concurrency_limit)
            lease = state["leases"].get(key)
            if lease is None:
                return False
            lease["expires"] = self.clock() + LEASE_SECONDS
            return True

    async def finish(self, email, key, *, success=False, cancelled_empty=False, elapsed=0):
        async with self.locked(email) as (_, state, cfg, _):
            lease = state["leases"].pop(key, None)
            if lease is None:
                return
            value = state["groups"].get(lease["group"])
            valid = (value and cfg["mode"] != "off" and lease["policy"] == state["policy"]
                     and lease["epoch"] == value["epoch"])
            qualified = cancelled_empty and elapsed >= lease["min_wait"]
            if valid and success:
                if lease["probe"]:
                    value.update(phase="closed", epoch=uuid.uuid4().hex, failures=[], until=0)
                elif value["phase"] == "closed":
                    value["failures"] = []
            elif valid and qualified and lease["request_id"] not in state["seen"]:
                state["seen"][lease["request_id"]] = self.clock()
                value["failures"] = [t for t in value["failures"] if t > self.clock() - cfg["window_seconds"]]
                value["failures"].append(self.clock())
                if lease["probe"] or len(value["failures"]) >= cfg["threshold"]:
                    self._trip(state, lease["group"], cfg)
            # Other probe failures leave the gate half-open for the next natural
            # request; existing auth/quota logic remains authoritative.
            self._advance(state)

    async def clear(self, email, scope="all"):
        """Manual reset: retain transports, fence old outcomes, preserve history."""
        if scope not in ("all", "quota", "protection"):
            raise ValueError("无效的冷却类型")
        if scope == "quota":
            return await self.database.clear_all_model_cooldowns(email)
        try:
            async with self.locked(email) as (_, state, _, cred):
                if cred is None:
                    # Roll back the state-row insert as well.
                    raise _CredentialNotFound(email)
                if scope == "all":
                    cred.model_cooldowns = "{}"
                    cred.updated_at = self.clock()
                state["groups"] = {
                    group: dict(_new_group(), trips=value["trips"],
                                would_block=value["would_block"])
                    for group, value in state["groups"].items()
                }
                state["seen"] = {}
                # Keep lease keys/ownership alive; new epochs prevent outcomes
                # from before the reset from tripping or reopening the new gate.
            return True
        except _CredentialNotFound:
            return False

    async def status(self):
        # Persist expired ownership so repeated refreshes cannot move the
        # displayed idle deadline forward without ever starting the timer.
        async with self.database.session() as session:
            cfg = await read_settings(session)
            emails = list(await session.scalars(select(Credential499State.email)))
        items = []
        for email in emails:
            async with self.locked(email) as (_, state, cfg, _):
                for group, value in state["groups"].items():
                    failures = [t for t in value["failures"] if t > self.clock() - cfg["window_seconds"]]
                    if value["phase"] != "closed" or failures or value["trips"]:
                        items.append(dict(email=email, group=group,
                                          group_label=family_label(group),
                                          **{k: value[k] for k in ("phase", "until", "trips", "would_block")},
                                          failure_count=len(failures), active=len(self._active(state, group))))
        return dict(mode=cfg["mode"], items=items)


class Lease:
    """Transport close AND final ASGI outcome are required before releasing."""
    def __init__(self, store, email, key):
        self.store, self.email, self.key = store, email, key
        self.closed = self.finished = False
        self.lost = False
        self.outcome = None
        self.started = time.monotonic()
        self.owner = asyncio.current_task()
        self.heartbeat = asyncio.create_task(self._renew())

    async def _renew(self):
        while True:
            await asyncio.sleep(RENEW_SECONDS)
            if self.closed and self.owner and self.owner.done() and self.outcome is None:
                return  # Missing finalizer: let ownership expire, never renew forever.
            try:
                owned = await self.store.renew(self.email, self.key)
                if owned:
                    continue
                reason = "ownership lost"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = type(exc).__name__
            if self.store.last_known_mode in ("off", "observe") and not self.store.concurrency_limited:
                log.warning(f"499 protection tracking unavailable ({reason}); continuing in {self.store.last_known_mode} mode")
                # Retry tracking, including policy reads, on the next heartbeat.
                # Do not recreate an expired lease or replay a missing outcome.
                continue
            log.error(f"499 protection lease renewal failed ({reason}); cancelling upstream owner")
            self.lost = True
            if self.owner and not self.owner.done():
                self.owner.cancel()
            return

    async def iterate(self, stream):
        while True:
            self.owner = asyncio.current_task()
            if self.lost:
                raise RuntimeError("499 protection lease ownership lost")
            try:
                chunk = await anext(stream)
            except StopAsyncIteration:
                return
            yield chunk

    async def result(self, *, success=False, cancelled_empty=False):
        if self.outcome is None:
            self.outcome = dict(success=success, cancelled_empty=cancelled_empty,
                                elapsed=time.monotonic() - self.started)
        await self._finish()

    async def close(self):
        self.closed = True
        await self._finish()

    async def _finish(self):
        if self.finished or not self.closed or self.outcome is None:
            return
        with CancelScope(shield=True):
            self.finished = True
            try:
                await self.store.finish(self.email, self.key, **self.outcome)
            except Exception as exc:
                if self.store.last_known_mode not in ("off", "observe"):
                    raise
                # An observation write failure cannot replace the API result.
                # Leave failed persistence to lease expiry; never claim recovery.
                log.warning(f"499 protection result not persisted ({type(exc).__name__}); continuing in {self.store.last_known_mode} mode")
            finally:
                self.heartbeat.cancel()
                await asyncio.gather(self.heartbeat, return_exceptions=True)


class PoolCooling(Exception):
    status_code = 503
    message = "499 保护：当前没有可以派发的凭证，请稍后重试"

    def __init__(self, retry_after=1):
        self.retry_after = retry_after
        super().__init__(self.message)


class CredentialReplaced(PoolCooling):
    status_code = 409
    message = "凭证已更换，请重试"


class RateLimited(PoolCooling):
    status_code = 429


class QuotaCooling(RateLimited):
    message = "可用凭证的该模型家族仍在上游冷却期，请稍后重试"


class DispatchLimited(RateLimited):
    message = "本地凭证并发或 RPM 已达上限，暂无可派发的凭证，请稍后重试"


async def report_pool_limit(database, model, excluded, limited):
    """After selection fails, compare the overlapping limits of each eligible account."""
    if not model:
        return
    cred = AntigravityCredential
    async with database.session() as session:
        rows = (await session.execute(
            select(cred.email, cred.model_cooldowns, cred.concurrency_limit, cred.rpm_limit,
                   Credential499State.state, CredentialEntitlement.snapshot)
            .outerjoin(CredentialEntitlement, CredentialEntitlement.email == cred.email)
            .outerjoin(Credential499State, Credential499State.email == cred.email)
            .where(cred.disabled.is_(False), cred.email.not_in(excluded),
                   or_(cred.model_cooldowns != "{}", cred.email.in_(limited)))
        )).all()
        if not rows:
            return
        cfg = await read_settings(session)
    now = time.time()
    group = get_cooldown_group(model)
    limits = []
    for email, cooldowns, concurrency, rpm, raw_state, catalog in rows:
        if catalog_supports_model(catalog, model, now=now) is False:
            continue
        state = _load_json(raw_state, {})
        quota = math.ceil(_load_json(cooldowns, {}).get(group, 0) - now)
        breaker = _breaker_deadline_wait(state, cfg, model, now)
        if quota > 0:
            limits.append(QuotaCooling(max(quota, breaker, dispatch_limit_wait(concurrency, rpm, state, now))))
        elif email in limited:
            limits.append(DispatchLimited(max(limited[email], breaker)))
    if limits:
        raise min(limits, key=lambda error: (error.retry_after, isinstance(error, QuotaCooling)))


async def acquire_dispatch(candidate, model, session_key, request_id, selector):
    """Admission is after credential prefetch; cached tokens cannot bypass it."""
    store = CooldownStore(await get_database())
    excluded = set()
    limits = []
    catalog_changed = False
    reselected = set()
    while candidate and candidate[0] not in excluded:
        email, data = candidate
        skip_email = True
        with CancelScope(shield=True):
            try:
                key, wait = await store.admit(email, model, request_id, expected_revision=data.get("credential_revision"))
                if key:
                    return candidate, Lease(store, email, key)
                if wait:
                    limits.append(PoolCooling(wait))
            except UnsupportedCredentialModel:
                catalog_changed = True
            except CredentialReplaced:
                # Reselect once even in a single-account pool; repeated replacement must not spin.
                skip_email = email in reselected
                reselected.add(email)
            except RateLimited as exc:
                limits.append(exc)
        if skip_email:
            excluded.add(email)
        try:
            candidate = await selector(model_name=model, session_key=session_key, exclude_emails=excluded)
        except RateLimited as exc:
            limits.append(exc)
            break
    if catalog_changed and await store.database.all_enabled_credentials_lack_model(model):
        raise PoolModelUnavailable(model)
    if limits:
        raise min(limits, key=lambda error: (error.retry_after, not isinstance(error, RateLimited)))
    raise PoolCooling()


async def acquire_test_dispatch(email, model, expected_revision=None):
    """An explicit panel test must also respect the account's idle interval."""
    store = CooldownStore(await get_database())
    with CancelScope(shield=True):
        key, wait = await store.admit(email, model, uuid.uuid4().hex, manual=True, expected_revision=expected_revision)
        if key:
            return Lease(store, email, key)
    raise PoolCooling(wait or 1)
