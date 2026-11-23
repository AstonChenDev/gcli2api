"""Legacy transport tests use synthetic credentials, never the operator's DB.

The dedicated cooldown suite exercises the real admission path and SQL store.
Other suites continue to isolate transport/conversion behavior from persistence.
"""
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

import config
from src.panel import config_routes
from src.storage.database import Database


@pytest_asyncio.fixture
async def panel_config_db(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    monkeypatch.setenv("POSTGRESQL_URI", "")
    monkeypatch.setattr(config, "_config_cache", {})
    monkeypatch.setattr(config, "_config_initialized", True)
    db = Database()
    await db.initialize()
    monkeypatch.setattr(config_routes, "get_database", AsyncMock(return_value=db))
    try:
        yield db
    finally:
        await db.close()


class TransportOnlyLease:
    async def result(self, **kwargs):
        pass

    async def close(self):
        pass

    def iterate(self, stream):
        return stream


@pytest.fixture(autouse=True)
def isolate_legacy_transport_tests(request):
    if request.node.path.name.startswith("test_cooldown_499"):
        yield
        return

    async def admit(candidate, *args):
        return candidate, TransportOnlyLease()

    async def admit_test(*args):
        return TransportOnlyLease()

    with patch("src.api.antigravity.acquire_dispatch", admit), patch("src.panel.creds.acquire_test_dispatch", admit_test):
        yield
