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


def _normalize_aliases(value: dict, names: tuple) -> dict:
    result = dict(value)
    for snake, camel in names:
        if snake not in result:
            continue
        supplied = result.pop(snake)
        existing = result.get(camel)
        if supplied is not None and existing is not None and supplied != existing:
            raise HTTPException(400, f"{snake} 与 {camel} 内容冲突，请只提供其中一个字段")
        if existing is None and supplied is not None:
            result[camel] = supplied
    return result


def normalize_gemini_request_fields(request: dict) -> dict:
    """Normalize protocol containers only; never rename schema properties or tool arguments."""
    result = _normalize_aliases(normalize_system_instruction(request), (
        ('generation_config', 'generationConfig'), ('safety_settings', 'safetySettings'),
        ('tool_config', 'toolConfig'), ('session_id', 'sessionId'),
    ))
    generation = result.get('generationConfig')
    if isinstance(generation, dict):
        generation = _normalize_aliases(generation, (
            ('top_p', 'topP'), ('top_k', 'topK'), ('max_output_tokens', 'maxOutputTokens'),
            ('candidate_count', 'candidateCount'), ('stop_sequences', 'stopSequences'),
            ('response_mime_type', 'responseMimeType'), ('response_schema', 'responseSchema'),
            ('response_json_schema', 'responseJsonSchema'), ('thinking_config', 'thinkingConfig'),
            ('response_modalities', 'responseModalities'), ('image_config', 'imageConfig'),
            ('presence_penalty', 'presencePenalty'), ('frequency_penalty', 'frequencyPenalty'),
        ))
        if isinstance(generation.get('thinkingConfig'), dict):
            generation['thinkingConfig'] = _normalize_aliases(generation['thinkingConfig'], (
                ('include_thoughts', 'includeThoughts'), ('thinking_budget', 'thinkingBudget'), ('thinking_level', 'thinkingLevel'),
            ))
        if isinstance(generation.get('imageConfig'), dict):
            generation['imageConfig'] = _normalize_aliases(generation['imageConfig'], (
                ('aspect_ratio', 'aspectRatio'), ('image_size', 'imageSize'),
            ))
        result['generationConfig'] = generation
    if isinstance(result.get('toolConfig'), dict):
        tool = _normalize_aliases(result['toolConfig'], (('function_calling_config', 'functionCallingConfig'),))
        if isinstance(tool.get('functionCallingConfig'), dict):
            tool['functionCallingConfig'] = _normalize_aliases(tool['functionCallingConfig'], (('allowed_function_names', 'allowedFunctionNames'),))
        result['toolConfig'] = tool
    return result
