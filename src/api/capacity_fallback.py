"""Configurable, one-way egress fallback for Antigravity capacity errors.

This module intentionally owns parsing, configuration, retry state, proxy
transport selection and fallback-specific statistics.  The frequently-updated
upstream Antigravity client only needs two thin call-site hooks.
"""

import fnmatch
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, Iterable, MutableMapping, Optional, Tuple
from urllib.parse import urlsplit

import httpx
from fastapi import Response

import config
from log import log
from src.capacity_fallback_stats import (
    record_capacity_exhausted,
    record_fallback_attempt,
)
from src.httpx_client import post_async, stream_post_async

DEFAULT_HTTP_STATUSES = (503,)
DEFAULT_ERROR_STATUSES = ("UNAVAILABLE",)
DEFAULT_REASONS = ("MODEL_CAPACITY_EXHAUSTED",)
DEFAULT_MODELS = ("*",)
DEFAULT_MAX_ATTEMPTS = 1
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 900.0
DEFAULT_ROUTE_NAME = "capacity-egress"
MAX_ATTEMPTS_SAFETY_LIMIT = 3

CONFIG_KEYS = {
    "enabled": "antigravity_capacity_fallback_enabled",
    "proxy_url": "antigravity_capacity_fallback_proxy_url",
    "http_statuses": "antigravity_capacity_fallback_http_statuses",
    "error_statuses": "antigravity_capacity_fallback_error_statuses",
    "reasons": "antigravity_capacity_fallback_reasons",
    "models": "antigravity_capacity_fallback_models",
    "max_attempts": "antigravity_capacity_fallback_max_attempts",
    "connect_timeout": "antigravity_capacity_fallback_connect_timeout_seconds",
    "request_timeout": "antigravity_capacity_fallback_request_timeout_seconds",
    "stats_enabled": "antigravity_capacity_fallback_stats_enabled",
    "route_name": "antigravity_capacity_fallback_route_name",
}

ENV_NAMES = {name: key.upper() for name, key in CONFIG_KEYS.items()}

_PROXY_SCHEMES = {"http", "https", "socks5", "socks5h"}
_ROUTE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass(frozen=True)
class CapacityFallbackSettings:
    """Validated runtime settings for one fallback decision."""

    enabled: bool = False
    proxy_url: str = ""
    http_statuses: Tuple[int, ...] = DEFAULT_HTTP_STATUSES
    error_statuses: Tuple[str, ...] = DEFAULT_ERROR_STATUSES
    reasons: Tuple[str, ...] = DEFAULT_REASONS
    models: Tuple[str, ...] = DEFAULT_MODELS
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    stats_enabled: bool = True
    route_name: str = DEFAULT_ROUTE_NAME

    def allows_model(self, model_name: str) -> bool:
        normalized = model_name or "unknown"
        return any(fnmatch.fnmatchcase(normalized, pattern) for pattern in self.models)


@dataclass
class CapacityFallbackState:
    """Per-logical-request guard; it is never shared between user requests."""

    attempts: int = 0

    def reserve(self, configured_limit: int) -> bool:
        limit = min(configured_limit, MAX_ATTEMPTS_SAFETY_LIMIT)
        if self.attempts >= limit:
            return False
        self.attempts += 1
        return True


def _parse_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"{field} must be a boolean")


def _parse_items(value: Any, *, field: str, uppercase: bool = False) -> Tuple[str, ...]:
    if isinstance(value, str):
        raw_items: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raise ValueError(f"{field} must be a comma-separated string or list")

    items = []
    for raw_item in raw_items:
        if not isinstance(raw_item, str):
            raise ValueError(f"{field} entries must be strings")
        item = raw_item.strip()
        if item:
            items.append(item.upper() if uppercase else item)
    if not items:
        raise ValueError(f"{field} must not be empty")
    return tuple(dict.fromkeys(items))


def _parse_http_statuses(value: Any) -> Tuple[int, ...]:
    if isinstance(value, str):
        raw_items: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raise ValueError("http statuses must be a comma-separated string or list")

    statuses = []
    for raw_item in raw_items:
        if isinstance(raw_item, bool):
            raise ValueError("HTTP status entries must be integers")
        try:
            status = int(str(raw_item).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("HTTP status entries must be integers") from exc
        if status < 400 or status > 599:
            raise ValueError("HTTP statuses must be between 400 and 599")
        statuses.append(status)
    if not statuses:
        raise ValueError("HTTP statuses must not be empty")
    return tuple(dict.fromkeys(statuses))


def _parse_int(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return parsed


def _parse_float(value: Any, *, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number") from exc
    if not math.isfinite(parsed) or parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} must be between {minimum:g} and {maximum:g}")
    return parsed


def validate_proxy_url(value: Any) -> str:
    """Validate a forward-proxy URL without logging embedded credentials."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("proxy URL must be a string")
    proxy_url = value.strip()
    if not proxy_url:
        return ""
    parsed = urlsplit(proxy_url)
    if parsed.scheme.lower() not in _PROXY_SCHEMES:
        raise ValueError("proxy URL scheme must be http, https, socks5 or socks5h")
    if not parsed.hostname or parsed.port is None:
        raise ValueError("proxy URL must include a host and port")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("proxy URL must not include a path, query or fragment")
    return proxy_url


def _parse_route_name(value: Any) -> str:
    if not isinstance(value, str) or not _ROUTE_NAME_PATTERN.fullmatch(value.strip()):
        raise ValueError("route name must use 1-64 letters, numbers, dots, underscores or dashes")
    return value.strip()


def _normalize_values(values: Dict[str, Any]) -> CapacityFallbackSettings:
    return CapacityFallbackSettings(
        enabled=_parse_bool(values["enabled"], field="enabled"),
        proxy_url=validate_proxy_url(values["proxy_url"]),
        http_statuses=_parse_http_statuses(values["http_statuses"]),
        error_statuses=_parse_items(
            values["error_statuses"], field="error statuses", uppercase=True
        ),
        reasons=_parse_items(values["reasons"], field="reasons", uppercase=True),
        models=_parse_items(values["models"], field="models"),
        max_attempts=_parse_int(
            values["max_attempts"],
            field="max attempts",
            minimum=1,
            maximum=MAX_ATTEMPTS_SAFETY_LIMIT,
        ),
        connect_timeout_seconds=_parse_float(
            values["connect_timeout"],
            field="connect timeout",
            minimum=0.1,
            maximum=60.0,
        ),
        request_timeout_seconds=_parse_float(
            values["request_timeout"],
            field="request timeout",
            minimum=1.0,
            maximum=3600.0,
        ),
        stats_enabled=_parse_bool(values["stats_enabled"], field="stats enabled"),
        route_name=_parse_route_name(values["route_name"]),
    )


async def get_capacity_fallback_settings(*, refresh: bool = False) -> CapacityFallbackSettings:
    """Load and validate settings using ENV > shared storage > defaults."""
    if refresh:
        # This path only runs for upstream errors, not successful requests.  A
        # refresh here makes panel changes visible across all worker processes.
        await config.reload_config()

    defaults: Dict[str, Any] = {
        "enabled": False,
        "proxy_url": "",
        "http_statuses": DEFAULT_HTTP_STATUSES,
        "error_statuses": DEFAULT_ERROR_STATUSES,
        "reasons": DEFAULT_REASONS,
        "models": DEFAULT_MODELS,
        "max_attempts": DEFAULT_MAX_ATTEMPTS,
        "connect_timeout": DEFAULT_CONNECT_TIMEOUT_SECONDS,
        "request_timeout": DEFAULT_REQUEST_TIMEOUT_SECONDS,
        "stats_enabled": True,
        "route_name": DEFAULT_ROUTE_NAME,
    }
    values = {
        name: await config.get_config_value(CONFIG_KEYS[name], default, ENV_NAMES[name])
        for name, default in defaults.items()
    }
    try:
        settings = _normalize_values(values)
    except ValueError as exc:
        log.error(f"[CAPACITY FALLBACK] Invalid configuration; fallback disabled: {exc}")
        return CapacityFallbackSettings(enabled=False)

    if settings.enabled and not settings.proxy_url:
        log.error("[CAPACITY FALLBACK] Enabled without a proxy URL; fallback disabled")
        return CapacityFallbackSettings(
            enabled=False,
            proxy_url="",
            http_statuses=settings.http_statuses,
            error_statuses=settings.error_statuses,
            reasons=settings.reasons,
            models=settings.models,
            max_attempts=settings.max_attempts,
            connect_timeout_seconds=settings.connect_timeout_seconds,
            request_timeout_seconds=settings.request_timeout_seconds,
            stats_enabled=settings.stats_enabled,
            route_name=settings.route_name,
        )
    return settings


async def get_capacity_fallback_config() -> Dict[str, Any]:
    """Return JSON-compatible values for the authenticated config API."""
    settings = await get_capacity_fallback_settings()
    return {
        CONFIG_KEYS["enabled"]: settings.enabled,
        CONFIG_KEYS["proxy_url"]: settings.proxy_url,
        CONFIG_KEYS["http_statuses"]: list(settings.http_statuses),
        CONFIG_KEYS["error_statuses"]: list(settings.error_statuses),
        CONFIG_KEYS["reasons"]: list(settings.reasons),
        CONFIG_KEYS["models"]: list(settings.models),
        CONFIG_KEYS["max_attempts"]: settings.max_attempts,
        CONFIG_KEYS["connect_timeout"]: settings.connect_timeout_seconds,
        CONFIG_KEYS["request_timeout"]: settings.request_timeout_seconds,
        CONFIG_KEYS["stats_enabled"]: settings.stats_enabled,
        CONFIG_KEYS["route_name"]: settings.route_name,
    }


def validate_capacity_fallback_updates(values: MutableMapping[str, Any]) -> None:
    """Validate and normalize capacity fallback keys in a config-save payload."""
    parsers = {
        CONFIG_KEYS["enabled"]: lambda value: _parse_bool(value, field="enabled"),
        CONFIG_KEYS["proxy_url"]: validate_proxy_url,
        CONFIG_KEYS["http_statuses"]: _parse_http_statuses,
        CONFIG_KEYS["error_statuses"]: lambda value: _parse_items(
            value, field="error statuses", uppercase=True
        ),
        CONFIG_KEYS["reasons"]: lambda value: _parse_items(value, field="reasons", uppercase=True),
        CONFIG_KEYS["models"]: lambda value: _parse_items(value, field="models"),
        CONFIG_KEYS["max_attempts"]: lambda value: _parse_int(
            value,
            field="max attempts",
            minimum=1,
            maximum=MAX_ATTEMPTS_SAFETY_LIMIT,
        ),
        CONFIG_KEYS["connect_timeout"]: lambda value: _parse_float(
            value, field="connect timeout", minimum=0.1, maximum=60.0
        ),
        CONFIG_KEYS["request_timeout"]: lambda value: _parse_float(
            value, field="request timeout", minimum=1.0, maximum=3600.0
        ),
        CONFIG_KEYS["stats_enabled"]: lambda value: _parse_bool(value, field="stats enabled"),
        CONFIG_KEYS["route_name"]: _parse_route_name,
    }
    for key, parser in parsers.items():
        if key not in values:
            continue
        normalized = parser(values[key])
        values[key] = list(normalized) if isinstance(normalized, tuple) else normalized

    enabled_key = CONFIG_KEYS["enabled"]
    proxy_key = CONFIG_KEYS["proxy_url"]
    if values.get(enabled_key) is True and proxy_key in values and not values[proxy_key]:
        raise ValueError("proxy URL is required when fallback is enabled")


def _extract_error_markers(error_text: str) -> Tuple[Optional[str], Tuple[str, ...]]:
    if not error_text:
        return None, ()
    try:
        payload = json.loads(error_text)
    except (TypeError, json.JSONDecodeError):
        return None, ()
    if not isinstance(payload, dict):
        return None, ()
    error_obj = payload.get("error", payload)
    if not isinstance(error_obj, dict):
        return None, ()

    raw_status = error_obj.get("status")
    error_status = raw_status.strip().upper() if isinstance(raw_status, str) else None
    reasons = []
    details = error_obj.get("details")
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, dict):
                continue
            raw_reason = detail.get("reason")
            if isinstance(raw_reason, str) and raw_reason.strip():
                reasons.append(raw_reason.strip().upper())
    return error_status, tuple(dict.fromkeys(reasons))


def is_capacity_exhausted_response(
    status_code: int,
    error_text: str,
    settings: CapacityFallbackSettings,
) -> bool:
    """Match the configured Google status and ErrorInfo reason exactly."""
    if status_code not in settings.http_statuses:
        return False
    error_status, reasons = _extract_error_markers(error_text)
    if error_status not in settings.error_statuses:
        return False
    return any(reason in settings.reasons for reason in reasons)


def _response_text(response: Response) -> str:
    body = response.body
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace")
    return str(body or "")


def _fallback_timeout(settings: CapacityFallbackSettings, *, streaming: bool) -> httpx.Timeout:
    total_timeout: Optional[float] = None if streaming else settings.request_timeout_seconds
    return httpx.Timeout(total_timeout, connect=settings.connect_timeout_seconds)


async def _matching_settings(
    *,
    status_code: int,
    error_text: str,
    model_name: str,
) -> Optional[CapacityFallbackSettings]:
    if status_code < 400:
        return None
    settings = await get_capacity_fallback_settings(refresh=True)
    if not is_capacity_exhausted_response(status_code, error_text, settings):
        return None
    if settings.stats_enabled:
        record_capacity_exhausted(model_name)
    return settings


def _can_attempt(
    settings: CapacityFallbackSettings,
    state: CapacityFallbackState,
    model_name: str,
) -> bool:
    return (
        settings.enabled
        and bool(settings.proxy_url)
        and settings.allows_model(model_name)
        and state.reserve(settings.max_attempts)
    )


async def post_with_capacity_fallback(
    *,
    url: str,
    model_name: str,
    state: CapacityFallbackState,
    data: Any = None,
    json_body: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    sender: Any = None,
    **kwargs: Any,
) -> httpx.Response:
    """POST directly, then retry a matching capacity response via one proxy route."""
    request_sender = sender or post_async
    response = await request_sender(
        url=url,
        data=data,
        json=json_body,
        headers=headers,
        timeout=timeout,
        **kwargs,
    )
    settings = await _matching_settings(
        status_code=response.status_code,
        error_text=response.text,
        model_name=model_name,
    )
    if settings is None or not _can_attempt(settings, state, model_name):
        return response

    started = time.monotonic()
    log.warning(
        f"[CAPACITY FALLBACK] Starting route={settings.route_name}, "
        f"model={model_name}, attempt={state.attempts}/{settings.max_attempts}"
    )
    try:
        fallback_response = await request_sender(
            url=url,
            data=data,
            json=json_body,
            headers=headers,
            timeout=_fallback_timeout(settings, streaming=False),
            proxy=settings.proxy_url,
            **kwargs,
        )
    except Exception as exc:
        if settings.stats_enabled:
            record_fallback_attempt(model_name, settings.route_name, success=False)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        log.error(
            f"[CAPACITY FALLBACK] Proxy transport failed route={settings.route_name}, "
            f"model={model_name}, elapsed_ms={elapsed_ms}, error={type(exc).__name__}"
        )
        return response

    success = fallback_response.status_code == 200 and bool(fallback_response.content)
    if settings.stats_enabled:
        record_fallback_attempt(model_name, settings.route_name, success=success)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    log.info(
        f"[CAPACITY FALLBACK] Completed route={settings.route_name}, model={model_name}, "
        f"status={fallback_response.status_code}, success={success}, elapsed_ms={elapsed_ms}"
    )
    return fallback_response


async def stream_with_capacity_fallback(
    *,
    url: str,
    body: Dict[str, Any],
    model_name: str,
    state: CapacityFallbackState,
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
    sender: Any = None,
    **kwargs: Any,
) -> AsyncGenerator[Any, None]:
    """Stream directly and use the proxy only for a pre-body capacity response."""
    request_sender = sender or stream_post_async
    direct_data_received = False
    async for item in request_sender(
        url=url,
        body=body,
        native=native,
        headers=headers,
        **kwargs,
    ):
        if not isinstance(item, Response):
            direct_data_received = True
            yield item
            continue

        if direct_data_received:
            # Once direct bytes reached the caller, switching routes would
            # concatenate two generations into one response.
            yield item
            return

        settings = await _matching_settings(
            status_code=item.status_code,
            error_text=_response_text(item),
            model_name=model_name,
        )
        if settings is None or not _can_attempt(settings, state, model_name):
            yield item
            return

        started = time.monotonic()
        outcome_recorded = False
        received_data = False
        log.warning(
            f"[CAPACITY FALLBACK] Starting streaming route={settings.route_name}, "
            f"model={model_name}, attempt={state.attempts}/{settings.max_attempts}"
        )
        try:
            async for fallback_item in request_sender(
                url=url,
                body=body,
                native=native,
                headers=headers,
                timeout=_fallback_timeout(settings, streaming=True),
                proxy=settings.proxy_url,
                **kwargs,
            ):
                if isinstance(fallback_item, Response):
                    if settings.stats_enabled:
                        record_fallback_attempt(model_name, settings.route_name, success=False)
                    outcome_recorded = True
                    elapsed_ms = int((time.monotonic() - started) * 1000)
                    log.info(
                        f"[CAPACITY FALLBACK] Streaming route completed "
                        f"route={settings.route_name}, model={model_name}, "
                        f"status={fallback_item.status_code}, success=False, "
                        f"elapsed_ms={elapsed_ms}"
                    )
                    yield fallback_item
                    return
                received_data = True
                yield fallback_item

            if settings.stats_enabled:
                record_fallback_attempt(model_name, settings.route_name, success=received_data)
            outcome_recorded = True
            elapsed_ms = int((time.monotonic() - started) * 1000)
            log.info(
                f"[CAPACITY FALLBACK] Streaming route completed "
                f"route={settings.route_name}, model={model_name}, status=200, "
                f"success={received_data}, elapsed_ms={elapsed_ms}"
            )
            return
        except Exception as exc:
            if settings.stats_enabled and not outcome_recorded:
                record_fallback_attempt(model_name, settings.route_name, success=False)
                outcome_recorded = True
            elapsed_ms = int((time.monotonic() - started) * 1000)
            log.error(
                f"[CAPACITY FALLBACK] Streaming proxy transport failed "
                f"route={settings.route_name}, model={model_name}, elapsed_ms={elapsed_ms}, "
                f"error={type(exc).__name__}"
            )
            if received_data:
                # HTTP headers/body have already reached the client.  Retrying
                # here would concatenate two generations into one stream.
                return
            yield item
            return
        finally:
            if settings.stats_enabled and not outcome_recorded:
                # Covers client cancellation/early generator close.
                record_fallback_attempt(model_name, settings.route_name, success=False)
        return
