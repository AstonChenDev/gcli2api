import json

import pytest

from src.api import utils


def _quota_error(message: str, *, status: str = "RESOURCE_EXHAUSTED") -> dict:
    """构造 Google 风格限额错误，避免各测试重复拼装响应。"""
    return {"error": {"status": status, "message": message}}


@pytest.mark.parametrize(
    ("duration", "expected_seconds"),
    [
        ("6s", 6),
        ("6h 30m 15s", 6 * 3600 + 30 * 60 + 15),
        ("1d 2h 3m 4s", 86400 + 2 * 3600 + 3 * 60 + 4),
    ],
)
def test_geminicli_parses_quota_reset_duration(monkeypatch, duration, expected_seconds):
    monkeypatch.setattr(utils.time, "time", lambda: 1000.0)

    cooldown_until = utils.parse_quota_reset_timestamp(
        _quota_error(f"Your quota will reset after {duration}."),
        mode="geminicli",
    )

    assert cooldown_until == 1000.0 + expected_seconds


def test_antigravity_does_not_use_geminicli_duration_parser(monkeypatch):
    """Antigravity 仍由独立可配置策略接管，不能被上游通用解析覆盖。"""
    monkeypatch.setattr(utils.time, "time", lambda: 1000.0)

    cooldown_until = utils.parse_quota_reset_timestamp(
        _quota_error("Your quota will reset after 6h 30m."),
        mode="antigravity",
    )

    assert cooldown_until is None


def test_generic_resource_exhausted_keeps_existing_ten_minute_default(monkeypatch):
    """解决上游冲突后继续保留本分支既有的 10 分钟默认值。"""
    monkeypatch.setattr(utils.time, "time", lambda: 1000.0)

    cooldown_until = utils.parse_quota_reset_timestamp(
        _quota_error("Resource has been exhausted (e.g. check quota)."),
        mode="geminicli",
    )

    assert cooldown_until == 1600.0


@pytest.mark.asyncio
async def test_parse_and_log_cooldown_uses_duration_parser(monkeypatch):
    monkeypatch.setattr(utils.time, "time", lambda: 1000.0)
    error_text = json.dumps(_quota_error("Your quota will reset after 90s."))

    cooldown_until = await utils.parse_and_log_cooldown(error_text, mode="geminicli")

    assert cooldown_until == 1090.0
