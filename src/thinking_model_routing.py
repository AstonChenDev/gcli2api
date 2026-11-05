"""Resolve explicit effort levels before protocol conversion discards them."""

from fastapi import HTTPException

from config import get_thinking_model_routing_bases, get_thinking_model_routing_enabled

LEVELS = ("high", "medium", "low")


def _read_path(payload: dict, *keys):
    value = payload
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


async def route_thinking_model(model: str, payload: dict, protocol: str) -> str:
    # Exact base-name matching preserves explicit suffixes and unrelated models.
    if not await get_thinking_model_routing_enabled():
        return model
    if model not in await get_thinking_model_routing_bases():
        return model

    paths = {
        "openai": [("reasoning_effort",), ("reasoning", "effort")],
        "anthropic": [("output_config", "effort")],
        "gemini": [
            (generation, thinking, level)
            for generation in ("generationConfig", "generation_config")
            for thinking in ("thinkingConfig", "thinking_config")
            for level in ("thinkingLevel", "thinking_level")
        ],
    }
    levels = []
    for path in paths[protocol]:
        value = _read_path(payload, *path)
        if value is None:
            continue
        if not isinstance(value, str) or value.strip().lower() not in LEVELS:
            raise HTTPException(status_code=400, detail="思考等级仅支持 high、medium、low")
        levels.append(value.strip().lower())
    if len(set(levels)) > 1:
        raise HTTPException(status_code=400, detail="请求中存在冲突的思考等级")
    return f"{model}-{levels[0] if levels else 'medium'}"


async def add_thinking_model_aliases(model_ids: list[str]) -> list[str]:
    """Advertise configured aliases only when all supported variants are listed."""
    result = list(model_ids)
    if await get_thinking_model_routing_enabled():
        available = set(model_ids)
        for base in await get_thinking_model_routing_bases():
            if base not in result and all(f"{base}-{level}" in available for level in LEVELS):
                result.append(base)
    return result
