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
from sqlalchemy import select, text

from log import log
from src.storage.database import (
    AntigravityCredential, ConfigItem, Credential499State, get_cooldown_group,
    get_database,
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
        return default if default in ("argon", "gemini", "claude_gpt") else "model:" + model.lower()
    return get_cooldown_group(model)


def family_label(group):
    if group.startswith("custom:"):
        return group[7:] + "（自定义）"
    if group.startswith("model:"):
        return group[6:] + "（单独模型）"
    return {"*": "整个账号", "argon": "Argon", "gemini": "Gemini",
            "claude_gpt": "Claude / GPT"}.get(group, group)


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
    """One transaction for the entire policy; workers never see a partial edit."""
    validate_settings(values)
    if not values:
        return
    async with database.session() as session:
        if database.backend_type == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        else:
            # Serialize simultaneous panel saves even before any setting exists.
            await session.execute(text("SELECT pg_advisory_xact_lock(4997861)"))
        old = await read_settings(session)
        updates = dict(values)
        if any(values.get(PREFIX + key, old[key]) != old[key] for key in (
            "mode", "scope", "custom_families_enabled", "family_rules", "family_fallback"
        )):
            updates[PREFIX + "revision"] = uuid.uuid4().hex
        for key, value in updates.items():
            row = await session.get(ConfigItem, key)
            if row is None:
                session.add(ConfigItem(key=key, value=json.dumps(value)))
            else:
                row.value = json.dumps(value)
    # Generic config/get merges this cache; breaker decisions always read SQL.
    await database.reload_config_cache()


def _new_group():
    return dict(phase="closed", epoch=uuid.uuid4().hex, failures=[], until=0,
                duration=0, triggered_at=0, would_block=0, trips=0)


class _CredentialNotFound(Exception):
    pass


class CooldownStore:
    def __init__(self, database, clock=time.time):
        self.database = database
        self.clock = clock

    @asynccontextmanager
    async def locked(self, email):
        async with self.database.locked_cooldown_state(email) as (session, row):
            cfg = await read_settings(session)
            state = json.loads(row.state)
            state.setdefault("leases", {})
            policy = [cfg["mode"], cfg["scope"], cfg.get("revision", "0")]
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
            yield session, state, cfg
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

    async def admit(self, email, model, request_id, *, manual=False):
        async with self.locked(email) as (session, state, cfg):
            # Recheck after cache/sticky/prefetch selection, immediately at dispatch.
            cred = await session.scalar(select(AntigravityCredential).where(AntigravityCredential.email == email))
            quota_family = get_cooldown_group(model)
            family = resolve_family(model, cfg)
            if not cred or (not manual and (cred.disabled or json.loads(cred.model_cooldowns or "{}").get(quota_family, 0) > self.clock())):
                return None, 1
            group = "*" if cfg["scope"] == "account" else family
            value = state["groups"].setdefault(group, _new_group())
            active = self._active(state, group)
            probes = sum(v["probe"] and v["epoch"] == value["epoch"] for v in active)
            blocked = value["phase"] in ("draining", "cooling") or (
                value["phase"] == "probe" and probes >= cfg["probe_concurrency"])
            if blocked:
                value["would_block"] += 1
                if cfg["mode"] == "enforce":
                    return None, max(1, math.ceil(value["until"] - self.clock()))
            key = uuid.uuid4().hex
            lease = dict(model=model, family=family, group=group, epoch=value["epoch"],
                         probe=value["phase"] == "probe", request_id=request_id,
                         expires=self.clock() + LEASE_SECONDS, started=self.clock(),
                         min_wait=cfg["min_wait_seconds"], policy=state["policy"])
            state["leases"][key] = lease
            return key, 0

    async def renew(self, email, key):
        async with self.locked(email) as (_, state, cfg):
            lease = state["leases"].get(key)
            if lease is None:
                return False
            lease["expires"] = self.clock() + LEASE_SECONDS
            return True

    async def finish(self, email, key, *, success=False, cancelled_empty=False, elapsed=0):
        async with self.locked(email) as (_, state, cfg):
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
            async with self.locked(email) as (session, state, _):
                cred = await session.scalar(select(AntigravityCredential).where(
                    AntigravityCredential.email == email).with_for_update())
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
            async with self.locked(email) as (_, state, cfg):
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
        try:
            while True:
                await asyncio.sleep(RENEW_SECONDS)
                if self.closed and self.owner and self.owner.done() and self.outcome is None:
                    return  # Missing finalizer: let ownership expire, never renew forever.
                if not await self.store.renew(self.email, self.key):
                    # Never continue an upstream call after losing ownership.
                    self.lost = True
                    if self.owner and not self.owner.done():
                        self.owner.cancel()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(f"499 protection lease renewal failed ({type(exc).__name__}); cancelling upstream owner")
            self.lost = True
            if self.owner and not self.owner.done():
                self.owner.cancel()

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
            finally:
                self.heartbeat.cancel()
                await asyncio.gather(self.heartbeat, return_exceptions=True)


class PoolCooling(Exception):
    def __init__(self, retry_after=1):
        self.retry_after = retry_after
        super().__init__("499 保护：当前没有可以派发的凭证，请稍后重试")


async def acquire_dispatch(candidate, model, session_key, request_id, selector):
    """Admission is after credential prefetch; cached tokens cannot bypass it."""
    store = CooldownStore(await get_database())
    excluded, waits = set(), []
    while candidate and candidate[0] not in excluded:
        email = candidate[0]
        with CancelScope(shield=True):
            key, wait = await store.admit(email, model, request_id)
            if key:
                return candidate, Lease(store, email, key)
        excluded.add(email)
        waits.append(wait)
        candidate = await selector(model_name=model, session_key=session_key, exclude_emails=excluded)
    raise PoolCooling(min(waits) if waits else 1)


async def acquire_test_dispatch(email, model):
    """An explicit panel test must also respect the account's idle interval."""
    store = CooldownStore(await get_database())
    with CancelScope(shield=True):
        key, wait = await store.admit(email, model, uuid.uuid4().hex, manual=True)
        if key:
            return Lease(store, email, key)
    raise PoolCooling(wait)
