"""Classify upstream SSE lines without guessing whether text is reasoning.

The transport uses httpx.aiter_lines(), so JSON data is one line at a time.
Content classification here never changes thought flags or thought signatures.
"""
import json
import copy
from fastapi import Response


BLOCK_REASONS = {
    "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII",
    "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION",
}


def stream_error(status, code, message, **details):
    return Response(status_code=status, media_type="application/json", content=json.dumps(
        {"error": {"code": status, "status": code, "message": message, **details}},
        ensure_ascii=False))


def _with_partial_content(error, event):
    """Keep valid same-event output without promoting the error to a successful finish."""
    partial = copy.deepcopy(event)
    inner = partial.get("response", partial)
    inner.pop("error", None)
    inner.pop("promptFeedback", None)
    candidates = inner.get("candidates")
    if not isinstance(candidates, list):
        return error
    for candidate in candidates:
        if isinstance(candidate, dict):
            candidate.pop("finishReason", None)
    # Reuse the shape/content checks; the error and block fields are removed,
    # so this validation cannot recursively enter another error branch.
    kind, line = inspect_line("data: " + json.dumps(partial, ensure_ascii=False))
    if kind == "content":
        error.upstream_content = line
    return error


def inspect_line(chunk):
    """Return (kind, normalized line or Response); no response text is retained."""
    try:
        raw = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
    except UnicodeDecodeError:
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游流事件不是有效 UTF-8")
    if not isinstance(raw, str) or not raw.strip():
        return "empty", None
    if raw.startswith(":"):
        return "heartbeat", raw.rstrip("\r\n") + "\n\n"
    if raw.startswith(("event:", "id:", "retry:")):
        return "empty", None
    if not raw.startswith("data:"):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游返回了无效的流事件")
    payload = raw[5:].strip()
    if payload == "[DONE]":
        return "done", "data: [DONE]\n\n"
    try:
        event = json.loads(payload)
    except (TypeError, ValueError):
        event = None
    response = event.get("response", event) if isinstance(event, dict) else None
    if not isinstance(response, dict):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游返回了无效的 JSON 流事件")
    if response.get("error"):
        error = response["error"]
        code = error.get("code") if isinstance(error, dict) else None
        status = code if isinstance(code, int) and not isinstance(code, bool) and 400 <= code <= 599 else 500
        result = Response(status_code=status, media_type="application/json",
                          content=json.dumps({"error": error}, ensure_ascii=False))
        return "error", _with_partial_content(result, event)
    feedback = response.get("promptFeedback") or {}
    blocked = feedback.get("blockReason") if isinstance(feedback, dict) else None
    if blocked == "BLOCK_REASON_UNSPECIFIED":
        blocked = None
    candidates = response.get("candidates") or []
    if not isinstance(candidates, list):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 candidates 格式无效")
    has_content = False
    for candidate in candidates:
        if not isinstance(candidate, dict):
            return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 candidate 格式无效")
        reason = candidate.get("finishReason")
        if isinstance(reason, str) and reason in BLOCK_REASONS:
            blocked = reason
        content = candidate.get("content") or {}
        parts = content.get("parts", []) if isinstance(content, dict) else None
        if not isinstance(parts, list) or any(not isinstance(p, dict) for p in parts):
            return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 parts 格式无效")
        has_content |= any(bool(p.get("text")) or any(k in p for k in
            ("functionCall", "inlineData", "fileData", "executableCode", "codeExecutionResult"))
            for p in parts)
    if blocked:
        error = stream_error(400, "UPSTREAM_CONTENT_BLOCKED", f"上游拦截了本次生成: {blocked}",
                             blockReason=blocked, promptFeedback=feedback,
                             finishReasons=[c.get("finishReason") for c in candidates if c.get("finishReason")])
        # Content and a block reason may arrive together. Preserve the partial
        # content before the error, without forwarding a successful terminator.
        if has_content:
            _with_partial_content(error, event)
        return "blocked", error
    return ("content" if has_content else "metadata"), "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"


def error_payload(response):
    """Retain structured error details across protocol adapters."""
    try:
        payload = json.loads(response.body)
        if isinstance(payload, dict) and "error" in payload:
            return payload
        message = payload
    except (ValueError, TypeError):
        message = response.body.decode("utf-8", errors="replace")
    return {"error": {"code": response.status_code, "message": str(message)}}
