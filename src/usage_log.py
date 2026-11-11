"""
请求日志服务：usage_log 表的写入与统计
- record_usage_log: 请求完成时写入一条日志（不抛异常，不影响主流程）
  写入先进入内存队列，由后台任务批量落库，避免在请求热路径上等待 SQLite 写锁
- extract_usage_tokens: 从 Gemini 响应/流式块中提取 usageMetadata token 计数
"""

import asyncio
import json
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary
import time
import uuid
from typing import Any, Dict, Optional

import config
from log import log

from src.storage.database import get_database
from src.request_body_log import retain_request_body_log, release_request_body_log


def new_request_id() -> str:
    """生成短请求 ID"""
    return uuid.uuid4().hex[:16]


def extract_response_id(source: Any) -> str:
    """从上游 JSON 或单个 SSE 事件提取 responseId。"""
    if isinstance(source, (str, bytes)):
        try:
            text = source.decode("utf-8") if isinstance(source, bytes) else source
            if text.startswith("data:"):
                text = text[5:].strip()
            source = json.loads(text)
        except (ValueError, UnicodeError):
            return ""
    if not isinstance(source, dict):
        return ""
    response = source.get("response", source)
    if not isinstance(response, dict):
        return ""
    value = response.get("responseId")
    return value[:160] if isinstance(value, str) else ""
# Per-loop queues prevent transports/tasks from leaking between application loops.
@dataclass
class _LogQueue:
    pending: list = field(default_factory=list)
    task: Optional[asyncio.Task] = None
    writer: Optional[asyncio.Task] = None
    accepted: int = 0
    flushed: int = 0


_queues = WeakKeyDictionary()
_FLUSH_INTERVAL = 2.0
_MAX_PENDING = 500


def _queue():
    return _queues.setdefault(asyncio.get_running_loop(), _LogQueue())


async def _persist_batch(state):
    database = await get_database()
    batch, state.pending = state.pending, []
    for entry in batch:
        try:
            await database.insert_usage_log(entry)
        except Exception as exc:
            log.warning(f"请求日志写入失败: {type(exc).__name__}")
        finally:
            release_request_body_log(entry.get("request_log_file", ""))
        state.flushed += 1


def _observe_writer_result(task):
    # A cancelled HTTP caller may no longer be waiting to retrieve an error.
    if not task.cancelled() and (exc := task.exception()) is not None:
        log.warning(f"请求日志批量落库异常: {type(exc).__name__}")


async def _flush_pending_logs(state=None):
    state = state or _queue()
    target = state.accepted
    # The queue owns one writer. Cancelling a request only cancels its wait,
    # never a SQL commit or the rest of a detached batch. Do not replay writes
    # whose commit may already have succeeded, which would double the usage.
    while state.flushed < target:
        if state.writer is None or state.writer.done():
            state.writer = asyncio.create_task(_persist_batch(state))
            state.writer.add_done_callback(_observe_writer_result)
        await asyncio.shield(state.writer)


async def _flush_loop(state):
    while True:
        await asyncio.sleep(_FLUSH_INTERVAL)
        try:
            await _flush_pending_logs(state)
        except Exception as exc:
            log.warning(f"请求日志批量落库异常: {type(exc).__name__}")


async def flush_usage_logs():
    await _flush_pending_logs()


async def close_usage_logs():
    """Drain an active writer before stopping the worker and closing the DB."""
    state = _queue()
    await _flush_pending_logs(state)
    if state.task and not state.task.done():
        state.task.cancel()
        try:
            await state.task
        except asyncio.CancelledError:
            pass
    await _flush_pending_logs(state)
    _queues.pop(asyncio.get_running_loop(), None)


async def _write_log(entry):
    path = entry.get("request_log_file", "")
    retain_request_body_log(path)
    accepted = False
    try:
        if not await config.get_usage_log_enabled():
            return
        state = _queue()
        if state.task is None or state.task.done():
            state.task = asyncio.create_task(_flush_loop(state))
        state.pending.append({**entry, "created_at": entry.get("created_at") or time.time()})
        state.accepted += 1
        accepted = True
        # Accept this record before waiting, so cancellation at the cap cannot
        # drop it. Only one managed writer runs, including concurrent flushes.
        if len(state.pending) >= _MAX_PENDING:
            await _flush_pending_logs(state)
    except Exception as exc:
        log.warning(f"请求日志入队失败: {type(exc).__name__}")
    finally:
        if not accepted:
            release_request_body_log(path)


def extract_usage_tokens(source: Any) -> Dict[str, int]:
    """
    从 Gemini 响应结构中提取 token 用量。

    兼容:
    - 非流式: {"usageMetadata": {...}} 或 {"response": {"usageMetadata": {...}}}
    - 流式块: data 字符串内的 usageMetadata（通常只在最后一个块出现）

    Gemini 语义（与 PR#432 对齐）:
    - promptTokenCount 含缓存 token（cachedContentTokenCount）
    - completion = candidatesTokenCount + thoughtsTokenCount

    同时兼容 v1internal 新版 Interactions 格式（对齐 Antigravity-Manager CanonicalUsage::from_gemini）:
    - total_input_tokens / total_output_tokens / total_thought_tokens / total_cached_tokens
    """
    prompt_tokens = completion_tokens = total_tokens = cached_tokens = 0

    if isinstance(source, (str, bytes)):
        # 流式 SSE 块：解析 JSON 提取 usageMetadata
        try:
            text = source.decode("utf-8") if isinstance(source, bytes) else source
            if text.startswith("data:"):
                text = text[len("data:"):].strip()
            if not text or text == "[DONE]":
                return {}
            source = json.loads(text)
        except Exception:
            return {}

    if not isinstance(source, dict):
        return {}

    metadata = source.get("usageMetadata")
    if not isinstance(metadata, dict):
        # 展开包装: {"response": {...}}
        response = source.get("response")
        if isinstance(response, dict):
            metadata = response.get("usageMetadata")
    if not isinstance(metadata, dict):
        return {}

    def _first_int(*keys: str) -> int:
        for key in keys:
            value = metadata.get(key)
            if value is not None and not isinstance(value, bool):
                try:
                    return max(0, int(value))
                except (TypeError, ValueError, OverflowError):
                    continue
        return 0

    prompt_tokens = _first_int("promptTokenCount", "total_input_tokens", "input_tokens")
    reasoning_tokens = _first_int("thoughtsTokenCount", "total_thought_tokens")
    candidates_tokens = _first_int("candidatesTokenCount", "total_output_tokens")
    completion_tokens = candidates_tokens + reasoning_tokens
    total_tokens = _first_int("totalTokenCount", "total_tokens") or (prompt_tokens + completion_tokens)

    # 缓存命中数：旧版 cachedContentTokenCount / 新版 total_cached_tokens，及 Anthropic 风格明细
    cached_tokens = _first_int(
        "cachedContentTokenCount",
        "cached_content_token_count",
        "total_cached_tokens",
        "cached_tokens",
        "cache_read_input_tokens",
    )
    if not cached_tokens:
        for details_key in ("prompt_tokens_details", "input_tokens_details"):
            details = metadata.get(details_key)
            if isinstance(details, dict):
                value = details.get("cached_tokens")
                if value is not None and not isinstance(value, bool):
                    try:
                        cached_tokens = max(0, int(value))
                    except (TypeError, ValueError, OverflowError):
                        cached_tokens = 0
                    break

    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cached_tokens": cached_tokens,
    }


async def record_usage_log(entry: Dict[str, Any]) -> None:
    """
    写入一条请求日志。任何异常仅记录告警，绝不影响主请求流程。

    日志先进内存队列由后台任务批量落库，不阻塞请求热路径。

    必要字段: model_name, success
    可选字段: api_format, client_ip, request_id, credential_email, is_stream,
              status_code, use_time, prompt_tokens, completion_tokens, total_tokens, cached_tokens, error_message,
              request_log_file（请求体详细日志的磁盘文件索引，空=未记录）
    """
    await _write_log(entry)


async def clear_usage_logs(days: int) -> int:
    """清理 N 天前的日志，返回删除条数"""
    await flush_usage_logs()
    database = await get_database()
    cutoff = time.time() - days * 86400
    return await database.delete_usage_logs_before(cutoff)
