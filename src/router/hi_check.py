"""
统一的健康检查（Hi消息）处理模块

提供对OpenAI、Gemini和Anthropic格式的Hi消息的解析和响应
"""
import json
import time
from typing import Any, Dict, List, Literal

from fastapi.responses import JSONResponse, Response


# ==================== Hi消息检测 ====================

def is_health_check_request(request_data: dict, format: str = "openai") -> bool:
    """
    检查是否是健康检查请求（Hi消息）
    
    Args:
        request_data: 请求数据
        format: 请求格式（"openai"、"gemini" 或 "anthropic"）
        
    Returns:
        是否是健康检查请求
    """
    if any(request_data.get(key) for key in ("tools", "system", "systemInstruction")):
        return False

    if format == "openai":
        if (request_data.get("n", 1) != 1 or request_data.get("reasoning_effort") is not None
                or request_data.get("reasoning")):
            return False
        response_format = request_data.get("response_format")
        if response_format is not None and response_format.get("type") != "text":
            return False
        # OpenAI格式健康检查: {"messages": [{"role": "user", "content": "Hi"}]}
        messages = request_data.get("messages", [])
        if len(messages) == 1:
            msg = messages[0]
            if msg.get("role") == "user" and msg.get("content") == "Hi":
                return True
                
    elif format == "gemini":
        generation = request_data.get("generationConfig") or {}
        if (request_data.get("cachedContent") or request_data.get("cached_content")
                or generation.get("thinkingConfig") or generation.get("responseModalities")
                or generation.get("candidateCount", 1) != 1
                or generation.get("responseMimeType") == "application/json"
                or "responseJsonSchema" in generation or "responseSchema" in generation):
            return False
        # Gemini格式健康检查: {"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]}
        contents = request_data.get("contents", [])
        if len(contents) == 1:
            content = contents[0]
            if content.get("role") == "user" and content.get("parts") == [{"text": "Hi"}]:
                return True
    
    elif format == "anthropic":
        if request_data.get("output_config") or request_data.get("thinking"):
            return False
        # Anthropic格式健康检查: {"messages": [{"role": "user", "content": "Hi"}]}
        messages = request_data.get("messages", [])
        if (len(messages) == 1 
            and messages[0].get("role") == "user" 
            and messages[0].get("content") == "Hi"):
            return True
    
    return False


def is_health_check_message(messages: List[Dict[str, Any]]) -> bool:
    """
    直接检查消息列表是否为健康检查消息（Anthropic专用）
    
    这是一个便捷函数，用于已经提取出消息列表的场景。
    
    Args:
        messages: 消息列表
        
    Returns:
        是否为健康检查消息
    """
    return (
        len(messages) == 1 
        and messages[0].get("role") == "user" 
        and messages[0].get("content") == "Hi"
    )


# ==================== Hi消息响应生成 ====================

def create_health_check_response(format: str = "openai", **kwargs) -> dict:
    """
    创建健康检查响应
    
    Args:
        format: 响应格式（"openai"、"gemini" 或 "anthropic"）
        **kwargs: 格式特定的额外参数
            - model: 模型名称（anthropic格式需要）
            - message_id: 消息ID（anthropic格式需要）
        
    Returns:
        健康检查响应字典
    """
    if format == "openai":
        # OpenAI格式响应
        return {
            "id": "healthcheck",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "healthcheck",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "API is working"
                },
                "finish_reason": "stop"
            }]
        }
    
    elif format == "gemini":
        # Gemini格式响应
        return {
            "candidates": [{
                "content": {
                    "parts": [{"text": "agy2api工作中"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0,
            }]
        }
    
    elif format == "anthropic":
        # Anthropic格式响应
        model = kwargs.get("model", "claude-unknown")
        message_id = kwargs.get("message_id", "msg_healthcheck")
        return {
            "id": message_id,
            "type": "message",
            "role": "assistant",
            "model": str(model),
            "content": [{"type": "text", "text": "antigravity Anthropic Messages 正常工作中"}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 0, "output_tokens": 0},
        }
    
    # 未知格式返回空字典
    return {}


def greeting_probe_response(protocol: Literal["openai", "anthropic"], stream: bool) -> Response:
    message = create_health_check_response(format=protocol)
    if not stream:
        return JSONResponse(message)

    if protocol == "openai":
        choice = message["choices"][0]
        chunk = {**message, "object": "chat.completion.chunk"}
        frames = []
        for delta, finish in [(choice["message"], None), ({}, choice["finish_reason"])]:
            chunk["choices"] = [{"index": choice["index"], "delta": delta, "finish_reason": finish}]
            frames.append("data: " + json.dumps(chunk) + "\n\n")
        frames.append("data: [DONE]\n\n")
    else:
        start = {**message, "content": [], "stop_reason": None}
        events = [
            {"type": "message_start", "message": start},
            {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
            {"type": "content_block_delta", "index": 0,
             "delta": {"type": "text_delta", "text": message["content"][0]["text"]}},
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta",
             "delta": {"stop_reason": message["stop_reason"], "stop_sequence": message["stop_sequence"]},
             "usage": {"output_tokens": message["usage"]["output_tokens"]}},
            {"type": "message_stop"},
        ]
        frames = [f"event: {event['type']}\ndata: {json.dumps(event)}\n\n" for event in events]
    return Response(content="".join(frames), media_type="text/event-stream")
