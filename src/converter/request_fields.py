"""Canonical Gemini system field before any prompt injection or forwarding."""

import copy

from fastapi import HTTPException


def normalize_system_instruction(request: dict) -> dict:
    """Keep one spelling without losing user instructions or mutating the input.

    Identical aliases are deduplicated. Conflicting aliases are rejected rather
    than silently dropping or reordering the caller's system instructions.
    """
    result = dict(request)
    snake = result.pop("system_instruction", None)
    camel = result.pop("systemInstruction", None)
    if snake is not None and camel is not None and snake != camel:
        raise HTTPException(400, "system_instruction 与 systemInstruction 内容冲突，请只提供其中一个系统提示词字段")
    system = camel if camel is not None else snake
    if system is not None:
        result["systemInstruction"] = copy.deepcopy(system)
    return result


def _comparison_value(key, value):
    # Effort routing ignores case and surrounding whitespace. Apply that rule
    # only to the protocol field, without rewriting the selected wire value.
    if key == 'thinkingLevel' and isinstance(value, str):
        return value.strip().lower()
    child = {'generationConfig': 'thinkingConfig', 'thinkingConfig': 'thinkingLevel'}.get(key)
    if child and isinstance(value, dict) and child in value:
        return {**value, child: _comparison_value(child, value[child])}
    return value


def _normalize_aliases(value: dict, names: tuple) -> dict:
    result = dict(value)
    for snake, camel in names:
        if snake not in result:
            continue
        supplied = result.pop(snake)
        existing = result.get(camel)
        if supplied is not None and existing is not None and _comparison_value(camel, supplied) != _comparison_value(camel, existing):
            raise HTTPException(400, f"{snake} 与 {camel} 内容冲突，请只提供其中一个字段")
        if existing is None and supplied is not None:
            result[camel] = supplied
    return result


def _normalize_container(owner, snake, camel, normalize):
    # Compare equivalent containers only after their protocol keys are canonical.
    values = dict(owner)
    for key in (snake, camel):
        if isinstance(values.get(key), dict):
            values[key] = normalize(values[key])
    return _normalize_aliases(values, ((snake, camel),))


def _thinking(value):
    return _normalize_aliases(value, (
        ('include_thoughts', 'includeThoughts'), ('thinking_budget', 'thinkingBudget'),
        ('thinking_level', 'thinkingLevel'),
    ))


def _generation(value):
    result = _normalize_container(value, 'thinking_config', 'thinkingConfig', _thinking)
    if result.get('thinkingConfig') is not None and not isinstance(result['thinkingConfig'], dict):
        raise HTTPException(400, "thinkingConfig 必须是对象")
    result = _normalize_container(result, 'image_config', 'imageConfig', lambda image: _normalize_aliases(
        image, (('aspect_ratio', 'aspectRatio'), ('image_size', 'imageSize'))
    ))
    return _normalize_aliases(result, (
        ('top_p', 'topP'), ('top_k', 'topK'), ('max_output_tokens', 'maxOutputTokens'),
        ('candidate_count', 'candidateCount'), ('stop_sequences', 'stopSequences'),
        ('response_mime_type', 'responseMimeType'), ('response_schema', 'responseSchema'),
        ('response_json_schema', 'responseJsonSchema'), ('response_modalities', 'responseModalities'),
        ('presence_penalty', 'presencePenalty'), ('frequency_penalty', 'frequencyPenalty'),
    ))


def _tool_config(value):
    return _normalize_container(value, 'function_calling_config', 'functionCallingConfig', lambda config:
        _normalize_aliases(config, (('allowed_function_names', 'allowedFunctionNames'),))
    )


def normalize_gemini_request_fields(request: dict) -> dict:
    """Normalize protocol containers only; never rename schema properties or tool arguments."""
    result = _normalize_aliases(normalize_system_instruction(request), (
        ('safety_settings', 'safetySettings'), ('session_id', 'sessionId'),
    ))
    result = _normalize_container(result, 'tool_config', 'toolConfig', _tool_config)
    result = _normalize_container(result, 'generation_config', 'generationConfig', _generation)
    # Explicit nested settings take precedence over both legacy root aliases.
    legacy = {key: result.pop(key) for key in ('thinking_config', 'thinkingConfig') if key in result}
    generation = result.get('generationConfig')
    if legacy and (generation is None or isinstance(generation, dict) and 'thinkingConfig' not in generation):
        thinking = _normalize_container(legacy, 'thinking_config', 'thinkingConfig', _thinking).get('thinkingConfig')
        if thinking is not None:
            if not isinstance(thinking, dict):
                raise HTTPException(400, "thinkingConfig 必须是对象")
            result['generationConfig'] = {**(generation or {}), 'thinkingConfig': thinking}
    return result
