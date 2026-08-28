import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response

import config
from src.api import antigravity, cooldown_policy
from src.models import ConfigSaveRequest
from src.panel import config_routes

CONFIG_KEY = "antigravity_resource_exhausted_cooldown_minutes"
ENV_KEY = "ANTIGRAVITY_RESOURCE_EXHAUSTED_COOLDOWN_MINUTES"


@pytest.fixture(autouse=True)
def isolate_cooldown_config(monkeypatch):
    """隔离全局配置缓存和环境变量，避免测试之间相互污染。"""

    async def keep_isolated_cache():
        return None

    monkeypatch.delenv(ENV_KEY, raising=False)
    monkeypatch.setattr(config, "_config_initialized", True)
    monkeypatch.setattr(config, "_config_cache", {})
    monkeypatch.setattr(config, "reload_config", keep_isolated_cache)


@pytest.mark.asyncio
async def test_cooldown_config_defaults_to_ten_minutes():
    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 10.0


@pytest.mark.asyncio
async def test_explicit_null_config_disables_cooldown(monkeypatch):
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: None})

    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() is None


@pytest.mark.asyncio
async def test_numeric_database_config_is_normalized(monkeypatch):
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: 2.5})

    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 2.5


@pytest.mark.asyncio
async def test_each_read_refreshes_shared_storage_to_avoid_stale_worker_cache(monkeypatch):
    """模拟另一个 worker 已保存新值，当前 worker 必须在使用前刷新旧缓存。"""
    shared_storage = {CONFIG_KEY: None}

    async def refresh_from_shared_storage():
        monkeypatch.setattr(config, "_config_cache", shared_storage.copy())

    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: 10.0})
    monkeypatch.setattr(config, "reload_config", refresh_from_shared_storage)

    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() is None

    shared_storage[CONFIG_KEY] = 2.5
    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 2.5


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled_value", ["none", "NULL", "off", "disabled"])
async def test_environment_can_disable_cooldown(monkeypatch, disabled_value):
    monkeypatch.setenv(ENV_KEY, disabled_value)
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: 20})

    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() is None


@pytest.mark.asyncio
async def test_environment_numeric_value_has_highest_priority(monkeypatch):
    monkeypatch.setenv(ENV_KEY, "1.25")
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: None})

    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 1.25


@pytest.mark.asyncio
async def test_invalid_external_config_falls_back_to_safe_default(monkeypatch):
    monkeypatch.setenv(ENV_KEY, "not-a-number")
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: None})
    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 10.0

    monkeypatch.delenv(ENV_KEY)
    monkeypatch.setattr(config, "_config_cache", {CONFIG_KEY: "invalid"})
    assert await config.get_antigravity_resource_exhausted_cooldown_minutes() == 10.0


@pytest.mark.parametrize(
    "invalid_value",
    [True, "10", 0, -1, float("inf"), float("nan"), 525600.1],
)
def test_cooldown_config_validation_rejects_ambiguous_or_unsafe_values(invalid_value):
    with pytest.raises(ValueError):
        config.validate_antigravity_resource_exhausted_cooldown_minutes(invalid_value)


@pytest.mark.asyncio
async def test_non_json_http_429_uses_fixed_configured_cooldown(monkeypatch):
    async def configured_minutes():
        return 2.5

    monkeypatch.setattr(
        cooldown_policy,
        "get_antigravity_resource_exhausted_cooldown_minutes",
        configured_minutes,
    )

    cooldown_until = await cooldown_policy.resolve_antigravity_cooldown_until(
        status_code=429,
        error_text="upstream rate limited",
        now=1000.0,
    )

    assert cooldown_until == 1150.0


@pytest.mark.asyncio
async def test_null_config_keeps_429_without_cooldown(monkeypatch):
    async def disabled_cooldown():
        return None

    monkeypatch.setattr(
        cooldown_policy,
        "get_antigravity_resource_exhausted_cooldown_minutes",
        disabled_cooldown,
    )

    cooldown_until = await cooldown_policy.resolve_antigravity_cooldown_until(
        status_code=429,
        error_text="",
        now=1000.0,
    )

    assert cooldown_until is None


@pytest.mark.asyncio
async def test_resource_exhausted_body_uses_config_even_with_http_503(monkeypatch):
    async def configured_minutes():
        return 1.0

    monkeypatch.setattr(
        cooldown_policy,
        "get_antigravity_resource_exhausted_cooldown_minutes",
        configured_minutes,
    )
    error_text = json.dumps({"error": {"status": " resource_exhausted ", "message": "quota"}})

    cooldown_until = await cooldown_policy.resolve_antigravity_cooldown_until(
        status_code=503,
        error_text=error_text,
        now=1000.0,
    )

    assert cooldown_until == 1060.0


@pytest.mark.asyncio
async def test_non_quota_error_delegates_to_existing_parser(monkeypatch):
    calls = []

    async def existing_parser(error_text, mode):
        calls.append((error_text, mode))
        return 1234.0

    monkeypatch.setattr(cooldown_policy, "parse_and_log_cooldown", existing_parser)
    error_text = json.dumps({"error": {"status": "UNAVAILABLE"}})

    cooldown_until = await cooldown_policy.resolve_antigravity_cooldown_until(
        status_code=503,
        error_text=error_text,
        now=1000.0,
    )

    assert cooldown_until == 1234.0
    assert calls == [(error_text, "antigravity")]


class _DummyStorageAdapter:
    def __init__(self):
        self.saved = {}

    async def set_config(self, key, value):
        self.saved[key] = value
        return True


async def _constant_password():
    return "test-password"


@pytest.fixture
def panel_save_dependencies(monkeypatch):
    """为配置保存接口提供无外部数据库依赖的替身。"""
    adapter = _DummyStorageAdapter()

    async def get_adapter():
        return adapter

    async def reload_config():
        return None

    monkeypatch.setattr(config_routes, "get_storage_adapter", get_adapter)
    monkeypatch.setattr(config, "reload_config", reload_config)
    monkeypatch.setattr(config, "get_api_password", _constant_password)
    monkeypatch.setattr(config, "get_panel_password", _constant_password)
    monkeypatch.setattr(config, "get_server_password", _constant_password)
    return adapter


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, 0.5, 10, 525600])
async def test_panel_accepts_valid_cooldown_values(panel_save_dependencies, value):
    response = await config_routes.save_config(
        ConfigSaveRequest(config={CONFIG_KEY: value}),
        token="test-token",
    )

    assert response.status_code == 200
    expected = None if value is None else float(value)
    assert panel_save_dependencies.saved[CONFIG_KEY] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [True, "10", 0, -1, 525600.1])
async def test_panel_rejects_invalid_cooldown_values(value):
    with pytest.raises(HTTPException) as exc_info:
        await config_routes.save_config(
            ConfigSaveRequest(config={CONFIG_KEY: value}),
            token="test-token",
        )

    assert exc_info.value.status_code == 400
    assert "Antigravity限额冷却配置无效" in exc_info.value.detail


class _SequencedCredentialManager:
    """按顺序返回凭证，并记录选择发生的时机。"""

    def __init__(self, events):
        self.events = events
        self.selection_count = 0

    async def get_valid_credential(self, *, mode, model_name):
        self.selection_count += 1
        self.events.append(f"select:{self.selection_count}")
        index = self.selection_count
        return (
            f"credential-{index}.json",
            {
                "access_token": f"token-{index}",
                "project_id": f"project-{index}",
            },
        )


@pytest.mark.asyncio
async def test_non_stream_429_persists_cooldown_before_preheating_next_credential(
    monkeypatch,
):
    """覆盖真实非流式调用链，防止以后重构重新引入预热竞态。"""
    events = []
    manager = _SequencedCredentialManager(events)
    responses = iter(
        [
            SimpleNamespace(
                status_code=429,
                text="rate limited",
                content=b"rate limited",
                headers={},
            ),
            SimpleNamespace(
                status_code=200,
                text='{"response":"ok"}',
                content=b'{"response":"ok"}',
                headers={},
            ),
        ]
    )

    async def stream_to_non_stream_disabled():
        return False

    async def api_url():
        return "https://example.invalid"

    async def retry_config():
        return {"retry_enabled": True, "max_retries": 1, "retry_interval": 0}

    async def auto_ban_codes():
        return []

    async def wrapped_request(request, model, project_id, enable_credit):
        return {"request": request, "model": model, "project": project_id}, "request-id"

    async def post_response(**kwargs):
        return next(responses)

    async def configured_cooldown(**kwargs):
        return 1600.0

    async def record_error(*args, **kwargs):
        events.append("record-cooldown")
        assert args[3] == 1600.0

    async def should_retry(*args, **kwargs):
        return True

    async def record_success(*args, **kwargs):
        events.append("record-success")

    monkeypatch.setattr(antigravity, "credential_manager", manager)
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
    monkeypatch.setattr(
        antigravity,
        "resolve_antigravity_cooldown_until",
        configured_cooldown,
    )
    monkeypatch.setattr(antigravity, "record_api_call_error", record_error)
    monkeypatch.setattr(antigravity, "handle_error_with_retry", should_retry)
    monkeypatch.setattr(antigravity, "record_api_call_success", record_success)

    response = await antigravity.non_stream_request(
        body={"model": "gemini-test", "request": {"contents": []}},
    )

    assert response.status_code == 200
    assert events[:3] == ["select:1", "record-cooldown", "select:2"]
    assert events[-1] == "record-success"


@pytest.mark.asyncio
async def test_stream_429_passes_configured_cooldown_to_error_recorder(monkeypatch):
    """覆盖流式429调用点，确保流式和非流式采用同一冷却策略。"""
    events = []
    manager = _SequencedCredentialManager(events)

    async def api_url():
        return "https://example.invalid"

    async def retry_config():
        return {"retry_enabled": True, "max_retries": 0, "retry_interval": 0}

    async def auto_ban_codes():
        return []

    async def wrapped_request(request, model, project_id, enable_credit):
        return {"request": request, "model": model, "project": project_id}, "request-id"

    async def stream_response(**kwargs):
        yield Response(content=b"rate limited", status_code=429)

    async def configured_cooldown(**kwargs):
        assert kwargs["status_code"] == 429
        return 1700.0

    async def record_error(*args, **kwargs):
        events.append("record-cooldown")
        assert args[3] == 1700.0

    async def should_not_retry(*args, **kwargs):
        return False

    monkeypatch.setattr(antigravity, "credential_manager", manager)
    monkeypatch.setattr(antigravity, "get_antigravity_api_url", api_url)
    monkeypatch.setattr(antigravity, "get_retry_config", retry_config)
    monkeypatch.setattr(antigravity, "get_auto_ban_error_codes", auto_ban_codes)
    monkeypatch.setattr(antigravity, "wrap_cli_request", wrapped_request)
    monkeypatch.setattr(antigravity, "stream_post_async", stream_response)
    monkeypatch.setattr(
        antigravity,
        "resolve_antigravity_cooldown_until",
        configured_cooldown,
    )
    monkeypatch.setattr(antigravity, "record_api_call_error", record_error)
    monkeypatch.setattr(antigravity, "handle_error_with_retry", should_not_retry)

    chunks = [
        chunk
        async for chunk in antigravity.stream_request(
            body={"model": "gemini-test", "request": {"contents": []}},
        )
    ]

    assert len(chunks) == 1
    assert isinstance(chunks[0], Response)
    assert chunks[0].status_code == 429
    assert events == ["select:1", "record-cooldown"]
