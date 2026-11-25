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
from src.panel import config_routes
from src.models import ConfigSaveRequest


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
    assert await route_thinking_model("gemini-4-hydrogen", body, "openai") == "gemini-4-hydrogen"


@pytest.mark.asyncio
async def test_existing_and_empty_model_lists_are_preserved():
    for bases in (["gemini-4-argon", "gemini-4-carbon"], []):
        config._config_cache["thinking_model_routing_bases"] = bases
        assert await config.get_thinking_model_routing_bases() == bases
        assert await route_thinking_model("gemini-4-hydrogen", {"reasoning_effort": "xhigh"}, "openai") == "gemini-4-hydrogen"


@pytest.mark.asyncio
@pytest.mark.parametrize("base", ["gemini-4-argon", "gemini-3.8-flash"])
async def test_model_aliases(base):
    models = [base + "-" + level for level in ("high", "medium", "low")]
    assert await add_thinking_model_aliases(models) == models + [base]
    assert await add_thinking_model_aliases(models[:2]) == models[:2]
    config._config_cache["thinking_model_routing_enabled"] = False
    assert await add_thinking_model_aliases(models) == models


@pytest.mark.asyncio
@pytest.mark.parametrize("base", ["gemini-4-hydrogen", "gemini-4-neon"])
async def test_four_level_alias_requires_all_variants(base):
    models = [base + "-" + level for level in ("low", "medium", "high", "xhigh")]
    assert await add_thinking_model_aliases(models[:3]) == models[:3]
    assert await add_thinking_model_aliases(models) == models + [base]
    assert await add_thinking_model_aliases(models + [base]) == models + [base]


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
@pytest.mark.parametrize("base,level", [
    (base, level)
    for base in ("gemini-4-argon", "gemini-4-carbon", "gemini-4-hydrogen", "gemini-4-neon", "gemini-3.8-flash")
    for level in (["high", "medium", "low", None, "omitted"]
                  + (["xhigh"] if base in ("gemini-4-hydrogen", "gemini-4-neon") else []))
])
def test_actual_routes(client, protocol, stream, prefix, base, level):
    http, calls = client
    model = prefix + base
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
    assert calls[0]["body"]["model"] == base + "-" + ("medium" if level in (None, "omitted") else level)
    assert calls[0]["log_ctx"]["client_model"] == model
    assert "thinkingLevel" not in calls[0]["body"]["request"].get("generationConfig", {}).get("thinkingConfig", {})


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("root_key", ["thinkingConfig", "thinking_config"])
def test_gemini_root_effort_and_opt_out_reach_upstream(client, stream, root_key):
    http, calls = client
    path = "/v1beta/models/gemini-4-argon:" + ("streamGenerateContent" if stream else "generateContent")
    body = {"contents": [{"role": "user", "parts": [{"text": "test"}]}],
            root_key: {"thinking_level": "high", "include_thoughts": False}}
    response = http.post(path, json=body)
    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]["body"]["model"] == "gemini-4-argon-high"
    assert calls[0]["body"]["request"]["generationConfig"]["thinkingConfig"]["includeThoughts"] is False
    assert root_key not in calls[0]["body"]["request"]

    body[root_key] = "invalid"
    assert http.post(path, json=body).status_code == 400
    assert len(calls) == 1

    body.update({"thinking_config": {"thinking_level": "low"},
                 "thinkingConfig": {"thinkingLevel": "medium"},
                 "generationConfig": {"thinkingConfig": {"thinkingLevel": "high", "includeThoughts": False}}})
    assert http.post(path, json=body).status_code == 200
    assert calls[-1]["body"]["model"] == "gemini-4-argon-high"
    assert calls[-1]["body"]["request"]["generationConfig"]["thinkingConfig"]["includeThoughts"] is False


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize(("limits", "expected"), [
    ({}, 65536),
    ({"max_completion_tokens": 24}, 24),
    ({"max_tokens": 17, "max_completion_tokens": 24}, 24),
    ({"max_tokens": 17, "max_completion_tokens": None}, 17),
    ({"max_completion_tokens": 90000}, 65536),
])
def test_openai_output_token_limit_reaches_upstream(client, stream, limits, expected):
    http, calls = client
    response = http.post("/v1/chat/completions", json={
        "model": "gemini-4-argon-medium", "stream": stream,
        "messages": [{"role": "user", "content": "Write a sentence."}], **limits,
    })
    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]["body"]["request"]["generationConfig"]["maxOutputTokens"] == expected


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_completion_limit_rejected_before_upstream(client, limit):
    http, calls = client
    response = http.post("/v1/chat/completions", json={
        "model": "gemini-4-argon-medium", "max_tokens": 17, "max_completion_tokens": limit,
        "messages": [{"role": "user", "content": "Write a sentence."}],
    })
    assert response.status_code == 422
    assert not calls


@pytest.mark.parametrize("effort", ["invalid", "", "none", "minimal"])
def test_invalid_effort_rejected_before_upstream(client, effort):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": effort, "stream": True, "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 400
    assert not calls


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("format_type", ["text", "json_object", "json_schema"])
def test_openai_response_format_reaches_generation(client, stream, format_type):
    http, calls = client
    schema = {
        "type": "object", "properties": {"top_k": {"$ref": "#/$defs/value"}},
        "$defs": {"value": {"type": ["integer", "null"], "default": None}},
        "required": ["top_k"], "additionalProperties": False,
    }
    requested = {"type": format_type}
    if format_type == "json_schema":
        requested["json_schema"] = {"name": "result", "strict": True, "schema": schema}
    response = http.post("/v1/chat/completions", json={
        "model": "gemini-4-argon-medium", "stream": stream, "response_format": requested,
        "messages": [{"role": "user", "content": "Say OK" if format_type == "text" else "Hi"}],
    })
    assert response.status_code == 200
    assert len(calls) == 1
    generation = calls[0]["body"]["request"]["generationConfig"]
    assert generation.get("responseMimeType") == (None if format_type == "text" else "application/json")
    assert generation.get("responseJsonSchema") == (schema if format_type == "json_schema" else None)


@pytest.mark.parametrize("requested", [{}, {"type": "json_schema"}, {"type": "json_schema", "json_schema": {"schema": []}}])
def test_invalid_response_format_skips_dispatch(client, requested):
    http, calls = client
    response = http.post("/v1/chat/completions", json={
        "model": "gemini-4-argon-medium", "response_format": requested,
        "messages": [{"role": "user", "content": "Hi"}],
    })
    assert response.status_code == 400
    assert not calls


@pytest.mark.parametrize("model", ["gemini-4-argon", "gemini-4-carbon", "gemini-3.8-flash"])
def test_xhigh_rejected_for_three_level_families(client, model):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": model, "reasoning_effort": "xhigh", "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 400
    assert not calls


@pytest.mark.parametrize("model", ["gemini-4-hydrogen-xhigh", "gemini-4-neon-xhigh", "gemini-3.8-flash-tiered"])
def test_explicit_suffix_takes_precedence(client, model):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": model, "reasoning_effort": "low", "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 200
    assert calls[0]["body"]["model"] == model


@pytest.mark.parametrize("stream", [False, True])
def test_gemini_compares_normalized_aliases_before_dispatch(client, stream):
    http, calls = client
    path = "/v1beta/models/gemini-4-argon:" + ("streamGenerateContent" if stream else "generateContent")
    body = {"contents": [{"parts": [{"text": "audit"}]}]}
    response = http.post(path, json={**body, "thinking_config": {"thinking_level": "low"},
                                     "thinkingConfig": {"thinkingLevel": "high"}})
    assert response.status_code == 400
    assert calls == []
    schema = {"type": "object", "properties": {"thinking_level": {"type": "string"}}}
    response = http.post(path, json={
        **body, "generation_config": {"thinking_config": {"thinking_level": "low"}, "response_json_schema": schema},
        "generationConfig": {"thinkingConfig": {"thinkingLevel": " LOW "}, "responseJsonSchema": schema}
    })
    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]["body"]["model"] == "gemini-4-argon-low"
    assert calls[0]["body"]["request"]["generationConfig"]["responseJsonSchema"] == schema


def test_conflicting_effort(client):
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": "high", "reasoning": {"effort": "low"}, "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 400
    assert not calls


@pytest.mark.asyncio
async def test_alias_fields():
    assert await route_thinking_model("gemini-4-argon", {"reasoning": {"effort": "medium"}}, "openai") == "gemini-4-argon-medium"
    assert await route_thinking_model("gemini-4-argon", {"generation_config": {"thinking_config": {"thinking_level": "LOW"}}}, "gemini") == "gemini-4-argon-low"
    assert await route_thinking_model("gemini-4-hydrogen", {"reasoning": {"effort": " XHIGH "}}, "openai") == "gemini-4-hydrogen-xhigh"
    assert await route_thinking_model("gemini-4-hydrogen", {"generation_config": {"thinking_config": {"thinking_level": "XHIGH"}}}, "gemini") == "gemini-4-hydrogen-xhigh"


def test_existing_rewrite_applies_after_effort(client):
    config._config_cache.update({"model_rewrite_enabled": True, "model_rewrite_rules": [["^gemini-4-argon-high$", "gemini-4-argon-low"]]})
    http, calls = client
    response = http.post("/v1/chat/completions", json={"model": "gemini-4-argon", "reasoning_effort": "high", "messages": [{"role": "user", "content": "Write a sentence."}]})
    assert response.status_code == 200
    assert calls[0]["body"]["model"] == "gemini-4-argon-low"


@pytest.mark.asyncio
async def test_panel_save_hot_reload_and_environment_lock(monkeypatch, panel_config_db):
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
    {"thinking_model_routing_bases": ["gemini-4-hydrogen-xhigh"]},
    {"thinking_model_routing_bases": ["gemini-4-hydrogen-XHIGH"]},
    {"thinking_model_routing_bases": ["bad/model"]},
])
async def test_invalid_panel_config(values):
    from fastapi import HTTPException
    from src.panel.config_routes import save_config
    from src.models import ConfigSaveRequest
    with pytest.raises(HTTPException) as error:
        await save_config(ConfigSaveRequest(config=values), token="test")
    assert error.value.status_code == 400
