import pytest

from src import capacity_fallback_stats
from src.stats_collector import StatsCollector


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
