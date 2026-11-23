from unittest.mock import AsyncMock

import pytest

import config
from src.storage import database


@pytest.mark.asyncio
async def test_worker_picks_up_committed_edits_and_deletions(panel_config_db, monkeypatch):
    writer = panel_config_db
    peer = database.Database()
    await peer.initialize()
    try:
        monkeypatch.setattr(database, "get_database", AsyncMock(return_value=peer))
        read_values = AsyncMock(wraps=peer._config_snapshot)
        monkeypatch.setattr(peer, "_config_snapshot", read_values)

        assert await config.reload_config(only_if_changed=True)
        read_values.assert_not_awaited()
        await writer.set_config("retry_429_interval", 7)
        assert await config.reload_config(only_if_changed=True)
        assert await config.get_config_value("retry_429_interval") == 7
        with monkeypatch.context() as patch:
            # A peer's wall clock can be behind the previous writer's.
            patch.setattr(database.time, "time", lambda: 1.0)
            await writer.set_config("retry_429_interval", 0)
            assert await config.reload_config(only_if_changed=True)
            assert await config.get_config_value("retry_429_interval") == 0
            await writer.set_config("retry_429_interval", 2)
        assert await config.reload_config(only_if_changed=True)
        assert await config.get_config_value("retry_429_interval") == 2

        # Steady-state polling reads only metadata.
        read_values.reset_mock()
        assert await config.reload_config(only_if_changed=True)
        read_values.assert_not_awaited()
        await writer.delete_config("retry_429_interval")
        assert await config.reload_config(only_if_changed=True)
        assert await config.get_config_value("retry_429_interval", 1) == 1
    finally:
        await peer.close()


@pytest.mark.asyncio
async def test_sync_failure_keeps_previous_settings_and_recovers(panel_config_db, monkeypatch):
    await panel_config_db.set_config("retry_429_interval", 3)
    monkeypatch.setattr(database, "get_database", AsyncMock(return_value=panel_config_db))
    assert await config.reload_config(only_if_changed=True)
    await panel_config_db.set_config("retry_429_interval", 9)
    with monkeypatch.context() as patch:
        patch.setattr(panel_config_db, "reload_config_cache", AsyncMock(side_effect=OSError()))
        assert not await config.reload_config(only_if_changed=True)
        assert await config.get_config_value("retry_429_interval") == 3
    assert await config.reload_config(only_if_changed=True)
    assert await config.get_config_value("retry_429_interval") == 9
