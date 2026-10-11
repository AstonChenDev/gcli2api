import copy

import pytest

from src.converter.antigravity_fix import (
    LITE_SAFETY_SETTINGS as ANTIGRAVITY_LITE_SAFETY_SETTINGS,
    _ensure_empty_tool_schema_for_claude,
    normalize_antigravity_request,
    prepare_image_generation_request,
)
from src.converter.gemini_fix import (
    LITE_SAFETY_SETTINGS as GEMINI_LITE_SAFETY_SETTINGS,
    normalize_gemini_request,
)
from src.models import GeminiGenerationConfig, model_to_dict


def test_antigravity_claude_tools_keep_schema_in_parameters():
    tools = [
        {
            "functionDeclarations": [
                {
                    "name": "test_tool",
                    "description": "A test tool.",
                    "parametersJsonSchema": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                    },
                }
            ]
        }
    ]

    result = _ensure_empty_tool_schema_for_claude(tools, "claude-opus-4-6-thinking", "antigravity")
    declaration = result[0]["functionDeclarations"][0]

    assert declaration["parameters"]["type"] == "object"
    assert "parametersJsonSchema" not in declaration


def test_image_request_keeps_base_model_and_client_generation_config():
    request = {
        "model": "gemini-3.1-flash-image-4k-16x9",
        "generationConfig": {
            "temperature": 0.7,
            "imageConfig": {"aspectRatio": "1:1", "imageSize": "2K"},
        },
        "systemInstruction": {"parts": [{"text": "ignored"}]},
        "tools": [{"googleSearch": {}}],
    }

    result = prepare_image_generation_request(request, request["model"])

    assert result["model"] == "gemini-3.1-flash-image"
    assert result["generationConfig"] == {
        "temperature": 0.7,
        "imageConfig": {"aspectRatio": "1:1", "imageSize": "2K"},
        "candidateCount": 1,
    }
    assert "systemInstruction" not in result
    assert "tools" not in result


def test_image_request_derives_image_config_from_model_suffixes():
    model = "gemini-3.1-flash-image-4k-16x9"

    result = prepare_image_generation_request(
        {"model": model, "generationConfig": {"temperature": 0.3}},
        model,
    )

    assert result["model"] == "gemini-3.1-flash-image"
    assert result["generationConfig"]["temperature"] == 0.3
    assert result["generationConfig"]["imageConfig"] == {
        "aspectRatio": "16:9",
        "imageSize": "4K",
    }


@pytest.mark.asyncio
async def test_antigravity_model_mapping_and_client_safety_settings(monkeypatch):
    async def return_thoughts():
        return True

    monkeypatch.setattr("config.get_return_thoughts_to_frontend", return_thoughts)
    client_safety = [{"category": "CUSTOM", "threshold": "BLOCK_LOW_AND_ABOVE"}]

    result = await normalize_antigravity_request(
        {
            "model": "gemini-3.1-pro-high",
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
            "generationConfig": {"temperature": 0.5},
            "safetySettings": client_safety,
        }
    )

    assert result["model"] == "gemini-pro-agent"
    assert result["safetySettings"] == client_safety
    assert result["generationConfig"]["temperature"] == 0.5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("normalizer", "expected_settings"),
    [
        (normalize_antigravity_request, ANTIGRAVITY_LITE_SAFETY_SETTINGS),
        (normalize_gemini_request, GEMINI_LITE_SAFETY_SETTINGS),
    ],
)
async def test_normal_models_use_compatible_default_safety_settings(
    monkeypatch, normalizer, expected_settings
):
    async def return_thoughts():
        return True

    monkeypatch.setattr("config.get_return_thoughts_to_frontend", return_thoughts)

    result = await normalizer(
        {
            "model": "gemini-2.5-flash",
            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        }
    )

    assert result["safetySettings"] == expected_settings
    assert all(setting["threshold"] == "OFF" for setting in expected_settings)
    assert all("IMAGE_" not in setting["category"] for setting in expected_settings)
    assert all("CIVIC_INTEGRITY" not in setting["category"] for setting in expected_settings)


@pytest.mark.parametrize(
    "payload",
    [
        {"responseModalities": ["TEXT", "IMAGE"], "imageConfig": {"imageSize": "2K"}},
        {"response_modalities": ["TEXT", "IMAGE"], "image_config": {"imageSize": "2K"}},
    ],
)
def test_generation_config_accepts_both_field_styles_and_emits_camel_case(payload):
    config = GeminiGenerationConfig(**payload)

    assert model_to_dict(config) == {
        "responseModalities": ["TEXT", "IMAGE"],
        "imageConfig": {"imageSize": "2K"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model",
    ["gemini-3-pro-high", "gemini-3-pro-high-thinking", "gemini-3.1-pro-high"],
)
async def test_antigravity_preserves_legacy_routes_and_thoughts_setting(monkeypatch, model):
    async def return_thoughts():
        return False

    monkeypatch.setattr("config.get_return_thoughts_to_frontend", return_thoughts)
    request = {
        "model": model,
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {
            "thinkingConfig": {"thinkingBudget": 8192, "thinkingLevel": "high"},
        },
    }
    original = copy.deepcopy(request)

    result = await normalize_antigravity_request(request)

    assert result["model"] == "gemini-pro-agent"
    assert result["generationConfig"]["thinkingConfig"] == {"includeThoughts": False}
    assert request == original


@pytest.mark.asyncio
async def test_antigravity_cleans_empty_claude_text_without_mutating_history(monkeypatch):
    async def return_thoughts():
        return True

    monkeypatch.setattr("config.get_return_thoughts_to_frontend", return_thoughts)
    request = {
        "model": "claude-opus-4-6-thinking",
        "contents": [
            {"role": "user", "parts": [{"text": "question"}]},
            {
                "role": "model",
                "parts": [{"text": "\n "}, {"text": [{"type": "text", "text": "answer"}]}],
            },
            {"role": "user", "parts": [{"text": "continue"}]},
            {"role": "model", "parts": [{"text": "prefill"}]},
        ],
    }
    original = copy.deepcopy(request)

    result = await normalize_antigravity_request(request)

    assert request == original
    assert result["contents"][-1]["role"] == "user"
    assert result["contents"][1]["parts"][-1]["text"] == "answer"
    assert all(part.get("text") for part in result["contents"][1]["parts"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "client_config", "expected"),
    [
        ("gemini-3.8-flash", {"thinkingBudget": 0}, {"thinkingLevel": "low"}),
        ("gemini-3.8-flash-high", {"thinkingLevel": "low"}, {"thinkingLevel": "high"}),
        ("gemini-2.5-pro", {"thinkingBudget": 0}, {"thinkingBudget": 128}),
        ("gemini-2.5-flash", {"thinkingBudget": 100000}, {"thinkingBudget": 24576}),
    ],
)
async def test_gemini_thinking_profiles_preserve_client_inputs(
    monkeypatch, model, client_config, expected
):
    async def return_thoughts():
        return False

    monkeypatch.setattr("config.get_return_thoughts_to_frontend", return_thoughts)
    request = {
        "model": model,
        "generationConfig": {"thinkingConfig": client_config},
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "safetySettings": [{"category": "CUSTOM", "threshold": "BLOCK_LOW_AND_ABOVE"}],
    }
    original = copy.deepcopy(request)

    result = await normalize_gemini_request(request)

    assert result["generationConfig"]["thinkingConfig"] == {
        **expected,
        "includeThoughts": False,
    }
    assert result["safetySettings"] == request["safetySettings"]
    assert request == original
