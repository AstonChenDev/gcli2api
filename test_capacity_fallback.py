import json
from dataclasses import replace

import httpx
import pytest
from fastapi import HTTPException, Response

import config
from src.api import antigravity
from src.api import capacity_fallback
from src.api.capacity_fallback import (
    CapacityFallbackSettings,
    CapacityFallbackState,
    get_capacity_fallback_settings,
    is_capacity_exhausted_response,
    post_with_capacity_fallback,
    stream_with_capacity_fallback,
    validate_capacity_fallback_updates,
    validate_proxy_url,
)
from src.httpx_client import HttpxClientManager
from src.models import ConfigSaveRequest
from src.panel import config_routes

CAPACITY_ERROR = json.dumps(
    {
        "error": {
            "code": 503,
            "message": "No capacity available for model gemini-image",
            "status": "UNAVAILABLE",
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "MODEL_CAPACITY_EXHAUSTED",
                    "domain": "cloudcode-pa.googleapis.com",
                },
                {
                    "@type": "type.googleapis.com/google.rpc.RetryInfo",
                    "retryDelay": "63388s",
                },
            ],
        }
    }
)


def enabled_settings(**changes):
    return replace(
        CapacityFallbackSettings(
            enabled=True,
            proxy_url="socks5h://capacity-egress:1080",
        ),
        **changes,
    )


def test_capacity_error_matching_is_exact_and_configurable():
    settings = enabled_settings()
    assert is_capacity_exhausted_response(503, CAPACITY_ERROR, settings)
    assert not is_capacity_exhausted_response(500, CAPACITY_ERROR, settings)
    assert not is_capacity_exhausted_response(503, "not-json", settings)

    wrong_status = CAPACITY_ERROR.replace("UNAVAILABLE", "RESOURCE_EXHAUSTED")
    assert not is_capacity_exhausted_response(503, wrong_status, settings)

    wrong_reason = CAPACITY_ERROR.replace("MODEL_CAPACITY_EXHAUSTED", "SOME_OTHER_REASON")
    assert not is_capacity_exhausted_response(503, wrong_reason, settings)

    custom = enabled_settings(
        http_statuses=(429,),
        error_statuses=("RESOURCE_EXHAUSTED",),
        reasons=("CUSTOM_REASON",),
    )
    custom_body = json.dumps(
        {
            "error": {
                "status": "RESOURCE_EXHAUSTED",
                "details": [{"reason": "CUSTOM_REASON"}],
            }
        }
    )
    assert is_capacity_exhausted_response(429, custom_body, custom)


@pytest.mark.parametrize(
    "value",
    [
        "http://proxy.example:3128",
        "https://proxy.example:443",
        "socks5://capacity-egress:1080",
        "socks5h://capacity-egress:1080",
    ],
)
def test_proxy_url_accepts_supported_configured_routes(value):
    assert validate_proxy_url(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "proxy.example:1080",
        "ftp://proxy.example:21",
        "socks5://proxy.example",
        "socks5://proxy.example:1080/path",
        "socks5://proxy.example:1080?secret=value",
    ],
)
def test_proxy_url_rejects_unsafe_or_ambiguous_values(value):
    with pytest.raises(ValueError):
        validate_proxy_url(value)


def test_config_update_validation_normalizes_all_operational_fields():
    values = {
        "antigravity_capacity_fallback_enabled": True,
        "antigravity_capacity_fallback_proxy_url": "socks5h://egress:1080",
        "antigravity_capacity_fallback_http_statuses": "503, 502,503",
        "antigravity_capacity_fallback_error_statuses": "unavailable",
        "antigravity_capacity_fallback_reasons": ["model_capacity_exhausted"],
        "antigravity_capacity_fallback_models": "gemini-*-image,other-model",
        "antigravity_capacity_fallback_max_attempts": 2,
        "antigravity_capacity_fallback_connect_timeout_seconds": 2.5,
        "antigravity_capacity_fallback_request_timeout_seconds": 120,
        "antigravity_capacity_fallback_stats_enabled": True,
        "antigravity_capacity_fallback_route_name": "jump-sg",
    }

    validate_capacity_fallback_updates(values)

    assert values["antigravity_capacity_fallback_http_statuses"] == [503, 502]
    assert values["antigravity_capacity_fallback_error_statuses"] == ["UNAVAILABLE"]
    assert values["antigravity_capacity_fallback_reasons"] == ["MODEL_CAPACITY_EXHAUSTED"]
    assert values["antigravity_capacity_fallback_models"] == [
        "gemini-*-image",
        "other-model",
    ]


def test_config_update_validation_requires_proxy_when_enabling():
    with pytest.raises(ValueError, match="proxy URL is required"):
        validate_capacity_fallback_updates(
            {
                "antigravity_capacity_fallback_enabled": True,
                "antigravity_capacity_fallback_proxy_url": "",
            }
        )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("antigravity_capacity_fallback_enabled", "maybe"),
        ("antigravity_capacity_fallback_http_statuses", "200"),
        ("antigravity_capacity_fallback_max_attempts", 4),
        ("antigravity_capacity_fallback_connect_timeout_seconds", 0),
        ("antigravity_capacity_fallback_request_timeout_seconds", 5000),
        ("antigravity_capacity_fallback_route_name", "bad route"),
    ],
)
def test_config_update_validation_rejects_invalid_values(key, value):
    with pytest.raises(ValueError):
        validate_capacity_fallback_updates({key: value})


@pytest.mark.asyncio
async def test_runtime_settings_use_environment_priority_and_refresh(monkeypatch):
    refresh_calls = 0

    async def reload_config():
        nonlocal refresh_calls
        refresh_calls += 1

    monkeypatch.setattr(config, "reload_config", reload_config)
    monkeypatch.setenv("ANTIGRAVITY_CAPACITY_FALLBACK_ENABLED", "true")
    monkeypatch.setenv(
        "ANTIGRAVITY_CAPACITY_FALLBACK_PROXY_URL",
        "socks5h://configured-egress:1080",
    )
    monkeypatch.setenv("ANTIGRAVITY_CAPACITY_FALLBACK_MODELS", "gemini-*-image")

    settings = await get_capacity_fallback_settings(refresh=True)

    assert refresh_calls == 1
    assert settings.enabled is True
    assert settings.proxy_url == "socks5h://configured-egress:1080"
    assert settings.models == ("gemini-*-image",)


@pytest.mark.asyncio
async def test_invalid_runtime_configuration_fails_closed(monkeypatch):
    monkeypatch.setenv("ANTIGRAVITY_CAPACITY_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("ANTIGRAVITY_CAPACITY_FALLBACK_PROXY_URL", "invalid")

    settings = await get_capacity_fallback_settings()

    assert settings.enabled is False
    assert settings.proxy_url == ""


@pytest.mark.asyncio
async def test_panel_save_normalizes_capacity_fallback_config(monkeypatch):
    class Adapter:
        def __init__(self):
            self.saved = {}

        async def set_config(self, key, value):
            self.saved[key] = value

    adapter = Adapter()

    async def get_adapter():
        return adapter

    async def no_reload():
        return None

    async def password():
        return "test-password"

    monkeypatch.setattr(config_routes, "get_storage_adapter", get_adapter)
    monkeypatch.setattr(config, "reload_config", no_reload)
    monkeypatch.setattr(config, "get_api_password", password)
    monkeypatch.setattr(config, "get_panel_password", password)
    monkeypatch.setattr(config, "get_server_password", password)

    response = await config_routes.save_config(
        ConfigSaveRequest(
            config={
                "antigravity_capacity_fallback_enabled": True,
                "antigravity_capacity_fallback_proxy_url": "socks5h://egress:1080",
                "antigravity_capacity_fallback_http_statuses": "503",
                "antigravity_capacity_fallback_error_statuses": "unavailable",
                "antigravity_capacity_fallback_reasons": "model_capacity_exhausted",
                "antigravity_capacity_fallback_models": "gemini-*-image",
                "antigravity_capacity_fallback_max_attempts": 1,
                "antigravity_capacity_fallback_connect_timeout_seconds": 5,
                "antigravity_capacity_fallback_request_timeout_seconds": 900,
                "antigravity_capacity_fallback_stats_enabled": True,
                "antigravity_capacity_fallback_route_name": "jump-sg",
            }
        ),
        token="test-token",
    )

    assert response.status_code == 200
    assert adapter.saved["antigravity_capacity_fallback_http_statuses"] == [503]
    assert adapter.saved["antigravity_capacity_fallback_error_statuses"] == ["UNAVAILABLE"]
    assert adapter.saved["antigravity_capacity_fallback_reasons"] == ["MODEL_CAPACITY_EXHAUSTED"]


@pytest.mark.asyncio
async def test_panel_save_rejects_invalid_capacity_fallback_config():
    with pytest.raises(HTTPException) as exc_info:
        await config_routes.save_config(
            ConfigSaveRequest(config={"antigravity_capacity_fallback_max_attempts": 999}),
            token="test-token",
        )

    assert exc_info.value.status_code == 400
    assert "容量回退配置无效" in exc_info.value.detail


@pytest.mark.asyncio
async def test_http_client_explicit_proxy_overrides_global_proxy(monkeypatch):
    async def global_proxy():
        return "http://global-proxy:3128"

    monkeypatch.setattr("src.httpx_client.get_proxy_config", global_proxy)
    manager = HttpxClientManager()

    kwargs = await manager.get_client_kwargs(proxy="socks5h://fallback-proxy:1080")

    assert kwargs["proxy"] == "socks5h://fallback-proxy:1080"


@pytest.mark.asyncio
async def test_non_stream_capacity_error_retries_same_request_through_proxy(monkeypatch):
    calls = []
    metrics = []

    async def fake_post(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return httpx.Response(503, content=CAPACITY_ERROR)
        return httpx.Response(200, content=b'{"ok":true}')

    async def fake_settings(*, refresh=False):
        assert refresh is True
        return enabled_settings(route_name="jump-sg")

    monkeypatch.setattr(capacity_fallback, "post_async", fake_post)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(
        capacity_fallback,
        "record_capacity_exhausted",
        lambda model: metrics.append(("capacity", model)),
    )
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: metrics.append(("fallback", model, route, success)),
    )
    state = CapacityFallbackState()
    payload = {"model": "gemini-image", "request": {"parts": []}}
    headers = {"Authorization": "Bearer redacted"}

    response = await post_with_capacity_fallback(
        url="https://upstream.example/generate",
        model_name="gemini-image",
        state=state,
        json_body=payload,
        headers=headers,
    )

    assert response.status_code == 200
    assert state.attempts == 1
    assert len(calls) == 2
    assert calls[0]["json"] is payload
    assert "proxy" not in calls[0]
    assert calls[1]["json"] is payload
    assert calls[1]["headers"] is headers
    assert calls[1]["proxy"] == "socks5h://capacity-egress:1080"
    assert isinstance(calls[1]["timeout"], httpx.Timeout)
    assert metrics == [
        ("capacity", "gemini-image"),
        ("fallback", "gemini-image", "jump-sg", True),
    ]


@pytest.mark.asyncio
async def test_per_request_state_prevents_unbounded_proxy_retries(monkeypatch):
    calls = []
    retry_results = []

    async def fake_post(**kwargs):
        calls.append(kwargs)
        return httpx.Response(503, content=CAPACITY_ERROR)

    async def fake_settings(*, refresh=False):
        return enabled_settings(max_attempts=1)

    monkeypatch.setattr(capacity_fallback, "post_async", fake_post)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: retry_results.append(success),
    )
    state = CapacityFallbackState()

    first = await post_with_capacity_fallback(
        url="https://upstream.example/generate",
        model_name="gemini-image",
        state=state,
        json_body={},
    )
    second = await post_with_capacity_fallback(
        url="https://upstream.example/generate",
        model_name="gemini-image",
        state=state,
        json_body={},
    )

    assert first.status_code == second.status_code == 503
    # First direct + one proxy + second direct. There is no second proxy attempt.
    assert len(calls) == 3
    assert sum("proxy" in call for call in calls) == 1
    assert retry_results == [False]


@pytest.mark.asyncio
async def test_disabled_or_non_matching_model_records_503_without_proxy(monkeypatch):
    calls = []
    encounters = []

    async def fake_post(**kwargs):
        calls.append(kwargs)
        return httpx.Response(503, content=CAPACITY_ERROR)

    async def fake_settings(*, refresh=False):
        return enabled_settings(models=("allowed-*",))

    monkeypatch.setattr(capacity_fallback, "post_async", fake_post)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(
        capacity_fallback,
        "record_capacity_exhausted",
        lambda model: encounters.append(model),
    )

    response = await post_with_capacity_fallback(
        url="https://upstream.example/generate",
        model_name="blocked-model",
        state=CapacityFallbackState(),
        json_body={},
    )

    assert response.status_code == 503
    assert len(calls) == 1
    assert encounters == ["blocked-model"]


@pytest.mark.asyncio
async def test_proxy_transport_error_returns_original_response_and_counts_failure(monkeypatch):
    calls = 0
    retry_results = []

    async def fake_post(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, content=CAPACITY_ERROR)
        raise httpx.ConnectError("proxy unavailable")

    async def fake_settings(*, refresh=False):
        return enabled_settings()

    monkeypatch.setattr(capacity_fallback, "post_async", fake_post)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: retry_results.append(success),
    )

    response = await post_with_capacity_fallback(
        url="https://upstream.example/generate",
        model_name="gemini-image",
        state=CapacityFallbackState(),
        json_body={},
    )

    assert response.status_code == 503
    assert response.text == CAPACITY_ERROR
    assert retry_results == [False]


@pytest.mark.asyncio
async def test_stream_capacity_error_switches_before_any_direct_body(monkeypatch):
    calls = []
    retry_results = []

    async def fake_stream(**kwargs):
        calls.append(kwargs)
        if "proxy" not in kwargs:
            yield Response(content=CAPACITY_ERROR, status_code=503)
            return
        yield b"first"
        yield b"second"

    async def fake_settings(*, refresh=False):
        return enabled_settings(route_name="jump-sg")

    monkeypatch.setattr(capacity_fallback, "stream_post_async", fake_stream)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: retry_results.append(success),
    )

    chunks = [
        item
        async for item in stream_with_capacity_fallback(
            url="https://upstream.example/stream",
            body={"model": "gemini-image"},
            model_name="gemini-image",
            state=CapacityFallbackState(),
            native=True,
        )
    ]

    assert chunks == [b"first", b"second"]
    assert len(calls) == 2
    assert calls[1]["proxy"] == "socks5h://capacity-egress:1080"
    assert retry_results == [True]


@pytest.mark.asyncio
async def test_stream_proxy_error_is_returned_once_and_counted_failed(monkeypatch):
    retry_results = []

    async def fake_stream(**kwargs):
        if "proxy" not in kwargs:
            yield Response(content=CAPACITY_ERROR, status_code=503)
            return
        yield Response(content="still unavailable", status_code=503)

    async def fake_settings(*, refresh=False):
        return enabled_settings()

    monkeypatch.setattr(capacity_fallback, "stream_post_async", fake_stream)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: retry_results.append(success),
    )

    chunks = [
        item
        async for item in stream_with_capacity_fallback(
            url="https://upstream.example/stream",
            body={},
            model_name="gemini-image",
            state=CapacityFallbackState(),
        )
    ]

    assert len(chunks) == 1
    assert isinstance(chunks[0], Response)
    assert chunks[0].status_code == 503
    assert retry_results == [False]


@pytest.mark.asyncio
async def test_stream_proxy_failure_after_output_never_replays_original_503(monkeypatch):
    retry_results = []

    async def fake_stream(**kwargs):
        if "proxy" not in kwargs:
            yield Response(content=CAPACITY_ERROR, status_code=503)
            return
        yield b"already-sent"
        raise httpx.ReadError("stream interrupted")

    async def fake_settings(*, refresh=False):
        return enabled_settings()

    monkeypatch.setattr(capacity_fallback, "stream_post_async", fake_stream)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fake_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: retry_results.append(success),
    )

    chunks = [
        item
        async for item in stream_with_capacity_fallback(
            url="https://upstream.example/stream",
            body={},
            model_name="gemini-image",
            state=CapacityFallbackState(),
        )
    ]

    assert chunks == [b"already-sent"]
    assert retry_results == [False]


@pytest.mark.asyncio
async def test_direct_stream_error_after_output_never_switches_route(monkeypatch):
    calls = []

    async def fake_stream(**kwargs):
        calls.append(kwargs)
        yield b"already-sent-direct"
        yield Response(content=CAPACITY_ERROR, status_code=503)

    async def unexpected_settings(*, refresh=False):
        raise AssertionError("fallback must not be evaluated after direct output")

    monkeypatch.setattr(capacity_fallback, "stream_post_async", fake_stream)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", unexpected_settings)

    chunks = [
        item
        async for item in stream_with_capacity_fallback(
            url="https://upstream.example/stream",
            body={},
            model_name="gemini-image",
            state=CapacityFallbackState(),
        )
    ]

    assert chunks[0] == b"already-sent-direct"
    assert isinstance(chunks[1], Response)
    assert chunks[1].status_code == 503
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_normal_stream_does_not_load_fallback_configuration(monkeypatch):
    async def fake_stream(**kwargs):
        yield b"normal"

    async def unexpected_settings(*, refresh=False):
        raise AssertionError("settings must not load for successful streams")

    monkeypatch.setattr(capacity_fallback, "stream_post_async", fake_stream)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", unexpected_settings)

    chunks = [
        item
        async for item in stream_with_capacity_fallback(
            url="https://upstream.example/stream",
            body={},
            model_name="gemini-image",
            state=CapacityFallbackState(),
        )
    ]

    assert chunks == [b"normal"]


@pytest.mark.asyncio
async def test_antigravity_non_stream_hook_keeps_same_credential_on_proxy_success(
    monkeypatch,
):
    class Manager:
        def __init__(self):
            self.selections = 0

        async def get_valid_credential(self, *, mode, model_name):
            self.selections += 1
            return (
                "same-credential.json",
                {"access_token": "same-token", "project_id": "same-project"},
            )

    class ParentStats:
        def __init__(self):
            self.requests = []

        def record_request(self, *args):
            self.requests.append(args)

        def record_error_code(self, *args):
            return None

    manager = Manager()
    parent_stats = ParentStats()
    calls = []
    credential_results = []

    async def stream_to_non_stream_disabled():
        return False

    async def api_url():
        return "https://upstream.example"

    async def retry_config():
        return {"retry_enabled": True, "max_retries": 5, "retry_interval": 0}

    async def auto_ban_codes():
        return []

    async def wrapped_request(request, model, project_id, enable_credit):
        return {"request": request, "model": model, "project": project_id}, "id"

    async def post_response(**kwargs):
        calls.append(kwargs)
        if "proxy" not in kwargs:
            return httpx.Response(503, content=CAPACITY_ERROR)
        return httpx.Response(200, content=b'{"response":{"candidates":[{}]}}')

    async def fallback_settings(*, refresh=False):
        return enabled_settings(route_name="jump-sg")

    async def record_success(*args, **kwargs):
        credential_results.append(("success", args, kwargs))

    async def record_error(*args, **kwargs):
        credential_results.append(("error", args, kwargs))

    monkeypatch.setattr(antigravity, "credential_manager", manager)
    monkeypatch.setattr(antigravity, "stats_collector", parent_stats)
    monkeypatch.setattr(
        antigravity,
        "get_antigravity_stream2nostream",
        stream_to_non_stream_disabled,
    )
    monkeypatch.setattr(antigravity, "get_antigravity_api_url", api_url)
    monkeypatch.setattr(antigravity, "get_retry_config", retry_config)
    monkeypatch.setattr(antigravity, "get_auto_ban_error_codes", auto_ban_codes)
    monkeypatch.setattr(antigravity, "wrap_cli_request", wrapped_request)
    monkeypatch.setattr(antigravity, "post_async", post_response)
    monkeypatch.setattr(antigravity, "record_api_call_success", record_success)
    monkeypatch.setattr(antigravity, "record_api_call_error", record_error)
    monkeypatch.setattr(capacity_fallback, "get_capacity_fallback_settings", fallback_settings)
    monkeypatch.setattr(capacity_fallback, "record_capacity_exhausted", lambda model: None)
    monkeypatch.setattr(
        capacity_fallback,
        "record_fallback_attempt",
        lambda model, route, success: None,
    )

    response = await antigravity.non_stream_request(
        body={"model": "gemini-image", "request": {"contents": []}}
    )

    assert response.status_code == 200
    assert manager.selections == 1
    assert len(calls) == 2
    assert calls[0]["headers"] is calls[1]["headers"]
    assert calls[0]["json"] is calls[1]["json"]
    assert calls[0]["headers"]["Authorization"] == "Bearer same-token"
    assert [entry[0] for entry in credential_results] == ["success"]
    assert parent_stats.requests == [("gemini-image", "antigravity", True)]
