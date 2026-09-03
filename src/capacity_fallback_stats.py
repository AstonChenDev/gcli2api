"""Statistics facade for Antigravity capacity fallback attempts.

The project already has a storage-independent, multi-worker-safe statistics
pipeline.  Capacity fallback uses a dedicated mode in that pipeline instead of
adding another set of database tables and migrations.  This keeps the feature
isolated from upstream storage implementations while preserving per-model,
per-time-bucket and per-IP aggregation.
"""

from typing import Optional

from src.stats_collector import stats_collector

CAPACITY_FALLBACK_STATS_MODE = "capacity_fallback"
CAPACITY_EXHAUSTED_ERROR_CODE = 503
CAPACITY_EXHAUSTED_DESCRIPTION = "MODEL_CAPACITY_EXHAUSTED"


def record_capacity_exhausted(model_name: Optional[str]) -> None:
    """Record a direct upstream capacity response for a model."""
    stats_collector.record_error_code(
        model_name,
        CAPACITY_FALLBACK_STATS_MODE,
        CAPACITY_EXHAUSTED_ERROR_CODE,
        CAPACITY_EXHAUSTED_DESCRIPTION,
    )


def record_fallback_attempt(
    model_name: Optional[str],
    route_name: str,
    *,
    success: bool,
) -> None:
    """Record exactly one completed fallback attempt and its final outcome."""
    # request_stats provides the per-model retry count and success rate.  The
    # credential-shaped table is reused with a non-secret route label so the
    # existing dashboard's API-call section remains useful in this mode.
    stats_collector.record_request(
        model_name,
        CAPACITY_FALLBACK_STATS_MODE,
        success,
    )
    stats_collector.record(
        route_name,
        model_name,
        CAPACITY_FALLBACK_STATS_MODE,
        success,
    )
