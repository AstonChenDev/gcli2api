from unittest.mock import Mock

import pytest

from src.models import ConfigSaveRequest
from src.panel import config_routes


@pytest.mark.asyncio
async def test_config_save_keeps_passwords_out_of_debug_messages(monkeypatch, panel_config_db):
    values = {"api_password": "test-api-secret", "panel_password": "test-panel-secret"}
    debug = Mock()
    monkeypatch.setattr(config_routes.log, "debug", debug)
    monkeypatch.setattr(config_routes, "get_env_locked_keys", lambda: set())

    for updates in (values, {"retry_429_interval": 0}):
        debug.reset_mock()
        response = await config_routes.save_config(ConfigSaveRequest(config=updates), token="test")
        assert response.status_code == 200
        for key, value in updates.items():
            assert await panel_config_db.get_config(key) == value
        for call in debug.call_args_list:
            assert all(secret not in str(call) for secret in values.values())
