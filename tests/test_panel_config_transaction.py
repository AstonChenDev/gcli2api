import asyncio
import json

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text

import config
from src.models import ConfigSaveRequest
from src.panel import config_routes
from src.storage.database import ConfigItem


async def save(values):
    return await config_routes.save_config(ConfigSaveRequest(config=values), token="test")


async def stored_values(db):
    async with db.session() as session:
        rows = await session.execute(select(ConfigItem.key, ConfigItem.value))
        return {key: json.loads(value) for key, value in rows}


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [r"\2", r"\g<missing>", r"\q"])
async def test_invalid_model_replacement_preserves_active_config(panel_config_db, monkeypatch, replacement):
    monkeypatch.setattr(config_routes, "get_env_locked_keys", lambda: set())
    rule = [r"^alias-(?P<level>.+)$", r"gemini-4-argon-\g<level>"]
    await save({"model_rewrite_enabled": True, "model_rewrite_rules": [rule]})
    before = await stored_values(panel_config_db)
    with pytest.raises(HTTPException) as failure:
        await save({"model_rewrite_enabled": False, "model_rewrite_rules": [rule, ["(argon)", replacement]]})
    assert failure.value.status_code == 400
    assert "第 2 条" in failure.value.detail
    assert before == await stored_values(panel_config_db) == config._config_cache


def test_legacy_model_rewrites_skip_bad_rule_and_keep_first_valid_match():
    pattern = r"^alias-(low|high)$"
    bad = [pattern, r"\g<missing>"]
    good = [pattern, r"gemini-4-argon-\1"]
    compiled, invalid = config.compile_model_rewrite_rules([bad, good, [pattern, "fallback"]])
    assert invalid == [bad]
    assert config.rewrite_model_name("alias-low", compiled) == "gemini-4-argon-low"
    assert config.rewrite_model_name("unrelated", compiled) == "unrelated"
    compiled, _ = config.compile_model_rewrite_rules([bad])
    assert config.rewrite_model_name("alias-low", compiled) == "alias-low"


@pytest.mark.asyncio
async def test_late_write_failure_rolls_back_config_and_policy_revision(panel_config_db, monkeypatch):
    db = panel_config_db
    monkeypatch.setattr(config_routes, "get_env_locked_keys", lambda: set())
    await save({"cooldown_499_mode": "off", "retry_429_interval": 2, "panel_password": "original"})
    original = await stored_values(db)
    async with db.session() as session:
        await session.execute(text("""
            CREATE TRIGGER reject_last_field BEFORE UPDATE ON config WHEN NEW.key = 'panel_password'
            BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END
        """))
    updates = {"cooldown_499_mode": "enforce", "retry_429_interval": 4, "panel_password": "synthetic-new"}
    with pytest.raises(HTTPException) as failure:
        await save(updates)
    assert failure.value.status_code == 500
    assert "synthetic-new" not in failure.value.detail
    assert await stored_values(db) == await db.get_all_config() == config._config_cache == original
    async with db.session() as session:
        await session.execute(text("DROP TRIGGER reject_last_field"))
    assert (await save(updates)).status_code == 200
    committed = await stored_values(db)
    assert committed == await db.get_all_config() == config._config_cache
    assert committed["cooldown_499_revision"] != original["cooldown_499_revision"]
    assert all(committed[key] == value for key, value in updates.items())


@pytest.mark.asyncio
async def test_concurrent_saves_keep_disjoint_changes_and_a_complete_policy(panel_config_db, monkeypatch):
    monkeypatch.setattr(config_routes, "get_env_locked_keys", lambda: set())
    first = {"cooldown_499_mode": "off", "retry_429_interval": 1, "usage_log_enabled": False}
    second = {"cooldown_499_mode": "enforce", "retry_429_interval": 2, "request_log_enabled": True}
    await asyncio.gather(save(first), save(second))
    current = await stored_values(panel_config_db)
    assert current == await panel_config_db.get_all_config() == config._config_cache
    assert (current["cooldown_499_mode"], current["retry_429_interval"]) in {("off", 1), ("enforce", 2)}
    assert current["usage_log_enabled"] is False
    assert current["request_log_enabled"] is True
