import pytest

from src import capacity_fallback_stats
from src.stats_collector import StatsCollector
from src.storage.psql_manager import PSQLManager


class FakeStatsCollector:
    def __init__(self):
        self.error_calls = []
        self.request_calls = []
        self.credential_calls = []

    def record_error_code(self, *args):
        self.error_calls.append(args)

    def record_request(self, *args):
        self.request_calls.append(args)

    def record(self, *args, **kwargs):
        self.credential_calls.append((args, kwargs))


def test_capacity_stats_reuse_existing_per_model_pipeline(monkeypatch):
    collector = FakeStatsCollector()
    monkeypatch.setattr(capacity_fallback_stats, "stats_collector", collector)

    capacity_fallback_stats.record_capacity_exhausted("gemini-image")
    capacity_fallback_stats.record_fallback_attempt("gemini-image", "jump-sg", success=True)
    capacity_fallback_stats.record_fallback_attempt("gemini-image", "jump-sg", success=False)

    assert collector.error_calls == [
        (
            "gemini-image",
            "capacity_fallback",
            503,
            "MODEL_CAPACITY_EXHAUSTED",
        )
    ]
    assert collector.request_calls == [
        ("gemini-image", "capacity_fallback", True),
        ("gemini-image", "capacity_fallback", False),
    ]
    assert collector.credential_calls == [
        (("jump-sg", "gemini-image", "capacity_fallback", True), {}),
        (("jump-sg", "gemini-image", "capacity_fallback", False), {}),
    ]


def test_capacity_stats_expose_per_model_success_rate_inputs(monkeypatch):
    collector = StatsCollector()
    monkeypatch.setattr(capacity_fallback_stats, "stats_collector", collector)

    capacity_fallback_stats.record_fallback_attempt("gemini-image", "jump-sg", success=True)
    capacity_fallback_stats.record_fallback_attempt("gemini-image", "jump-sg", success=True)
    capacity_fallback_stats.record_fallback_attempt("gemini-image", "jump-sg", success=False)

    counts = collector._request_counters[
        ("gemini-image", capacity_fallback_stats.CAPACITY_FALLBACK_STATS_MODE)
    ]
    assert counts == {"total": 3, "success": 2, "fail": 1}
    assert counts["success"] / counts["total"] * 100 == pytest.approx(66.6666667)


class FakeStatsConnection:
    def __init__(self):
        self.queries = []

    async def fetchrow(self, query, *params):
        self.queries.append(query)
        assert params == ("capacity_fallback",)
        return {"total": 3, "success": 2, "fail": 1}

    async def fetch(self, query, *params):
        self.queries.append(query)
        assert params == ("capacity_fallback",)
        if "GROUP BY model_name" in query:
            return [
                {
                    "model_name": "gemini-image",
                    "total": 3,
                    "success": 2,
                    "fail": 1,
                }
            ]
        assert "GROUP BY filename" in query
        return [
            {
                "filename": "jump-sg",
                "user_email": None,
                "total": 3,
                "success": 2,
                "fail": 1,
            }
        ]


class FakeAcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeStatsPool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return FakeAcquireContext(self.connection)


@pytest.mark.asyncio
async def test_postgresql_summary_supports_operational_stats_mode_without_credential_table():
    connection = FakeStatsConnection()
    manager = PSQLManager()
    manager._initialized = True
    manager._pool = FakeStatsPool(connection)

    summary = await manager.get_stats_summary(mode="capacity_fallback")

    assert summary["global"] == {"total": 3, "success": 2, "fail": 1}
    assert summary["models"][0]["model_name"] == "gemini-image"
    assert summary["credentials"][0] == {
        "filename": "jump-sg",
        "user_email": None,
        "display_name": "jump-sg",
        "total": 3,
        "success": 2,
        "fail": 1,
    }
    assert not any("LEFT JOIN" in query for query in connection.queries)
