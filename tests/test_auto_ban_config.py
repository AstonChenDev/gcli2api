from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import config
from src.api.utils import check_should_auto_ban
from src.models import ConfigSaveRequest
from src.panel import config_routes


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", ["[400, 401]", "400,401", [400, 401]])
async def test_configured_codes_match_the_auto_ban_decision(monkeypatch, stored):
    monkeypatch.delenv("AUTO_BAN_ERROR_CODES", raising=False)
    monkeypatch.delenv("AUTO_BAN", raising=False)
    monkeypatch.setattr(config, "_config_initialized", True)
    monkeypatch.setattr(config, "_config_cache", {"auto_ban_enabled": True, "auto_ban_error_codes": stored})
    assert await check_should_auto_ban(400)
    assert await check_should_auto_ban(401)
    assert not await check_should_auto_ban(403)
    monkeypatch.setenv("AUTO_BAN_ERROR_CODES", "403")
    assert not await check_should_auto_ban(400)
    assert await check_should_auto_ban(403)
    monkeypatch.setenv("AUTO_BAN", "false")
    assert not await check_should_auto_ban(403)


@pytest.mark.parametrize("empty", [[], "", "[]"])
def test_empty_auto_ban_codes_keep_existing_default(empty):
    assert config.parse_auto_ban_error_codes(empty) == [403]


@pytest.mark.asyncio
async def test_invalid_auto_ban_codes_reject_the_entire_save_before_writing(monkeypatch):
    get_database = AsyncMock()
    monkeypatch.setattr(config_routes, "get_database", get_database)
    for value in ("[400,", "400,bad", [True], [600], "[403.0]"):
        with pytest.raises(HTTPException) as exc:
            await config_routes.save_config(ConfigSaveRequest(config={
                "auto_ban_enabled": True, "auto_ban_error_codes": value,
            }), token="test")
        assert exc.value.status_code == 400
    get_database.assert_not_awaited()
