from pathlib import Path

import pytest

from src.storage.psql_manager import PSQLManager


class FakeTierStatsConnection:
    def __init__(self):
        self.calls = []

    async def fetchrow(self, query, *params):
        self.calls.append((query, params))
        assert params == ("antigravity", 100, 200)
        return {"total": 20, "success": 15, "fail": 5}

    async def fetch(self, query, *params):
        self.calls.append((query, params))
        assert params == ("antigravity", 100, 200)
        if "LOWER(c.tier)" in query:
            return [
                {
                    "model_name": "gemini-image",
                    "tier": "pro",
                    "total": 12,
                    "success": 7,
                    "fail": 5,
                },
                {
                    "model_name": "gemini-image",
                    "tier": "ultra",
                    "total": 8,
                    "success": 8,
                    "fail": 0,
                },
            ]
        if "GROUP BY model_name" in query:
            return [
                {
                    "model_name": "gemini-image",
                    "total": 20,
                    "success": 15,
                    "fail": 5,
                }
            ]
        if "GROUP BY s.filename, c.user_email" in query:
            return [
                {
                    "filename": "credential.json",
                    "user_email": "masked@example.invalid",
                    "total": 20,
                    "success": 15,
                    "fail": 5,
                }
            ]
        raise AssertionError(f"Unexpected query: {query}")


class FakeAcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return FakeAcquireContext(self.connection)


@pytest.mark.asyncio
async def test_postgres_summary_groups_each_model_by_current_credential_tier():
    connection = FakeTierStatsConnection()
    manager = PSQLManager()
    manager._initialized = True
    manager._pool = FakePool(connection)

    summary = await manager.get_stats_summary(
        mode="antigravity",
        start_time=100,
        end_time=200,
    )

    assert summary["tier_models"] == [
        {
            "model_name": "gemini-image",
            "tier": "pro",
            "total": 12,
            "success": 7,
            "fail": 5,
        },
        {
            "model_name": "gemini-image",
            "tier": "ultra",
            "total": 8,
            "success": 8,
            "fail": 0,
        },
    ]
    tier_query = next(query for query, _ in connection.calls if "LOWER(c.tier)" in query)
    assert "LEFT JOIN antigravity_credentials c" in tier_query
    assert "s.time_bucket >= $2" in tier_query
    assert "s.time_bucket <= $3" in tier_query


def test_desktop_and_mobile_panels_load_isolated_tier_model_renderer():
    root = Path(__file__).resolve().parent
    for filename in ("control_panel.html", "control_panel_mobile.html"):
        html = (root / "front" / filename).read_text(encoding="utf-8")
        assert "front/tier_model_stats.js" in html

    renderer = (root / "front" / "tier_model_stats.js").read_text(encoding="utf-8")
    assert "gcli:stats-loaded" in renderer
    assert "凭证等级 × 模型成功率" in renderer
    assert "escapeHtml(model.modelName)" in renderer
