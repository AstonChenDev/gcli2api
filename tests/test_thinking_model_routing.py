"""Local tests: API calls are mocked; no credentials or upstream generation."""
import copy
import json
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import config
from src.thinking_model_routing import route_thinking_model, add_thinking_model_aliases
from src.router.antigravity import openai, gemini, anthropic, model_list
from src.utils import authenticate_bearer, authenticate_gemini_flexible


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    monkeypatch.delenv("THINKING_MODEL_ROUTING_ENABLED", raising=False)
    monkeypatch.delenv("THINKING_MODEL_ROUTING_BASES", raising=False)
    monkeypatch.setattr(config, "_config_initialized", True)
    monkeypatch.setattr(config, "_config_cache", {"thinking_model_routing_enabled": True})


@pytest.mark.asyncio
@pytest.mark.parametrize("level", ["high", "medium", "low", " HIGH "])
async def test_levels(level):
    body = {"reasoning_effort": level}
    original = copy.deepcopy(body)
    assert await route_thinking_model("gemini-4-argon", body, "openai") == "gemini-4-argon-" + level.strip().lower()
    assert body == original


@pytest.mark.asyncio
async def test_compatibility_and_env(monkeypatch):
    body = {"reasoning_effort": "high"}
    for model in ["gemini-4-argon-low", "gemini-4-other", "gpt-test"]:
        assert await route_thinking_model(model, body, "openai") == model
    assert await route_thinking_model("gemini-4-argon", {}, "openai") == "gemini-4-argon-medium"
    config._config_cache.clear()
    assert await route_thinking_model("gemini-4-argon", {}, "openai") == "gemini-4-argon"
    assert await route_thinking_model("gemini-4-argon", body, "openai") == "gemini-4-argon"
    monkeypatch.setenv("THINKING_MODEL_ROUTING_ENABLED", "true")
    assert await route_thinking_model("gemini-4-argon", body, "openai") == "gemini-4-argon-high"
    monkeypatch.setenv("THINKING_MODEL_ROUTING_ENABLED", "false")
    assert await route_thinking_model("gemini-4-argon", {"reasoning_effort": "invalid"}, "openai") == "gemini-4-argon"
    monkeypatch.setenv("THINKING_MODEL_ROUTING_ENABLED", "true")
    monkeypatch.setenv("THINKING_MODEL_ROUTING_BASES", "gemini-4-custom")
    assert await route_thinking_model("gemini-4-custom", body, "openai") == "gemini-4-custom-high"


@pytest.mark.asyncio
async def test_model_aliases():
    models = ["gemini-4-argon-" + level for level in ("high", "medium", "low")]
    assert await add_thinking_model_aliases(models) == models + ["gemini-4-argon"]
    assert await add_thinking_model_aliases(models[:2]) == models[:2]
    config._config_cache["thinking_model_routing_enabled"] = False
    assert await add_thinking_model_aliases(models) == models


@pytest.fixture
def client(monkeypatch):
    from src.api import antigravity as api
    captured = []
    answer = {"candidates": [{"content": {"role": "model", "parts": [{"text": "answer"}]}, "finishReason": "STOP"}]}

    async def non_stream(**kwargs):
        captured.append(kwargs)
        return JSONResponse(answer)

    async def stream(**kwargs):
        captured.append(kwargs)
        yield "data: " + json.dumps(answer) + "\n\n"

    monkeypatch.setattr(api, "non_stream_request", non_stream)
    monkeypatch.setattr(api, "stream_request", stream)
    app = FastAPI()
    for module in (openai, gemini, anthropic, model_list):
        app.include_router(module.router)
    for dependency in (authenticate_bearer, authenticate_gemini_flexible):
        app.dependency_overrides[dependency] = lambda: "local-test"
    with TestClient(app) as test_client:
        yield test_client, captured


@pytest.mark.parametrize("protocol", ["openai", "gemini", "anthropic"])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("prefix", ["", "抗截断-"])
@pytest.mark.parametrize("level", ["high", "medium", "low", None, "omitted"])
def test_actual_routes(client, protocol, stream, prefix, level):
    http, calls = client
    model = prefix + "gemini-4-argon"
    if protocol == "gemini":
        url = f"/v1beta/models/{model}:" + ("streamGenerateContent" if stream else "generateContent")
        body = {"contents": [{"role": "user", "parts": [{"text": "Write a short sentence."}]}], "generationConfig": {"thinkingConfig": {"thinkingLevel": level}}}
    else:
        url = "/v1/chat/completions" if protocol == "openai" else "/v1/messages"
        body = {"model": model, "messages": [{"role": "user", "content": "Write a short sentence."}], "stream": stream}
        body.update({"reasoning_effort": level} if protocol == "openai" else {"output_config": {"effort": level}})
    if level == "omitted":
        for field in ("reasoning_effort", "output_config", "generationConfig"):
            body.pop(field, None)
    response = http.post(url, json=body)
    assert response.status_code == 200, response.text
    assert "answer" in response.text
    assert calls[0]["body"]["model"] == "gemini-4-argon-" + ("medium" if level in (None, "omitted") else level)
    assert calls[0]["log_ctx"]["client_model"] == model
    assert "thinkingLevel" not in calls[0]["body"]["request"].get("generationConfig", {}).get("thinkingConfig", {})


@pytest.mark.parametrize("effort", ["invalid", "", "none", "minimal"])
def test_invalid_effort_rejected_before_upstream(client, effort):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": effort, "stream": True, "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 400
    assert not calls


def test_conflicting_effort(client):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": "high", "reasoning": {"effort": "low"}, "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 400
    assert not calls


@pytest.mark.asyncio
async def test_alias_fields():
    assert await route_thinking_model("gemini-4-argon", {"reasoning": {"effort": "medium"}}, "openai") == "gemini-4-argon-medium"
    assert await route_thinking_model("gemini-4-argon", {"generation_config": {"thinking_config": {"thinking_level": "LOW"}}}, "gemini") == "gemini-4-argon-low"


def test_existing_rewrite_applies_after_effort(client):
    config._config_cache.update({"model_rewrite_enabled": True, "model_rewrite_rules": [["^gemini-4-argon-high$", "gemini-4-argon-low"]]})
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": "high", "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 200
    assert calls[0]["body"]["model"] == "gemini-4-argon-low"


@pytest.mark.asyncio
async def test_panel_save_hot_reload_and_environment_lock(monkeypatch):
    from src.panel import config_routes
    from src.storage import database
    from src.models import ConfigSaveRequest
    stored = {"api_password": "test-only", "panel_password": "test-only"}
    db = AsyncMock()
    db.get_all_config.side_effect = lambda: dict(stored)
    db.set_config.side_effect = lambda key, value: stored.update({key: value})
    monkeypatch.setattr(config_routes, "get_database", AsyncMock(return_value=db))
    monkeypatch.setattr(database, "get_database", AsyncMock(return_value=db))
    for enabled in (True, False):
        await config_routes.save_config(ConfigSaveRequest(config={"thinking_model_routing_enabled": enabled, "thinking_model_routing_bases": ["gemini-4-custom"]}), token="test")
        assert await config.get_thinking_model_routing_enabled() is enabled
        assert await config.get_thinking_model_routing_bases() == ["gemini-4-custom"]
    monkeypatch.setenv("THINKING_MODEL_ROUTING_ENABLED", "true")
    await config_routes.save_config(ConfigSaveRequest(config={"thinking_model_routing_enabled": False}), token="test")
    assert await config.get_thinking_model_routing_enabled() is True
    assert "thinking_model_routing_enabled" in config_routes.get_env_locked_keys()


@pytest.mark.asyncio
@pytest.mark.parametrize("values", [
    {"thinking_model_routing_enabled": "false"},
    {"thinking_model_routing_bases": "gemini-4-argon"},
    {"thinking_model_routing_bases": ["gemini-4-argon-high"]},
    {"thinking_model_routing_bases": ["bad/model"]},
])
async def test_invalid_panel_config(values):
    from fastapi import HTTPException
    from src.panel.config_routes import save_config
    from src.models import ConfigSaveRequest
    with pytest.raises(HTTPException) as error:
        await save_config(ConfigSaveRequest(config=values), token="test")
    assert error.value.status_code == 400
