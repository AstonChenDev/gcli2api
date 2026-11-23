"""
Antigravity API Client - Handles communication with Google's Antigravity API
处理与 Google Antigravity API 的通信
"""

import asyncio
import copy
import httpx
import hashlib
import json
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Callable, Tuple

from fastapi import Response
from httpx import TransportError
from config import (
    get_antigravity_api_url,
    get_auto_ban_error_codes,
    get_request_log_enabled,
    get_request_log_all_requests,
    get_request_log_error_codes,
)
from log import log

from src.credential_manager import (
    add_credential,
    get_valid_credential,
)
from src.httpx_client import decoded_response_headers, retry_after_headers, strip_json_bom, stream_post_async, post_async
from src.credential_cooldown import acquire_dispatch, CredentialReplaced, PoolCooling, RateLimited, QuotaCooling
from src.credential_entitlements import mark_model_unavailable
from src.credential_tier import generation_project_id
from src.storage.database import PoolModelUnavailable, get_database
from src.models import Model, model_to_dict
from anyio import CancelScope
from src.stream_metadata import StreamMetadata, StreamCompletion
from src.stream_events import CandidateBlocks, StreamUsage, finish_data_record, gemini_metadata_frames, inspect_line, stream_error
from src.stream_delivery import current_delivery
from src.converter.antigravity_fix import _split_tools_single_function
from src.converter.request_fields import normalize_gemini_request_fields
from src import usage_log
from src.usage_log import extract_response_id
from src.utils import ANTIGRAVITY_USER_AGENT
from src.session_cache import derive_conversation_anchor, derive_session_key
from src.usage_log import (
    extract_usage_tokens,
    new_request_id,
)
from src.request_body_log import record_request_body

# 导入共同的基础功能
from src.api.utils import (
    is_model_not_found,
    handle_error_with_retry,
    get_retry_config,
    record_api_call_success,
    record_api_call_error,
    parse_and_log_cooldown,
)

# ==================== 全局凭证管理器 ====================


def _derive_upstream_session_ids(
    gemini_request: Dict[str, Any], model: str
) -> Tuple[str, str]:
    """
    从联合锚点摘要确定性派生上游 (sessionId, trajectory_id)。

    联合锚点 = SHA256(model + systemInstruction 文本 + 前两条消息特征)，
    与 derive_session_key 同口径，保证:
    - 同一会话跨轮次/跨请求 sessionId、trajectory_id 恒定（KV 缓存路由亲和）；
    - 共用相同 systemInstruction 的不同会话（多 Agent / 主子会话）不会撞车；
    - 摘要前 8 字节 → sessionId（沿用既有 -{int} 形态）；
    - 摘要 8~24 字节 → 16 字节确定性 UUID 形态 trajectory_id。

    无锚点（如空对话）时返回 ("", "")，由调用方兜底。
    """
    digest = derive_conversation_anchor(model, gemini_request)
    if digest is None:
        return "", ""
    session_int = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    session_id = f"-{session_int}"
    trajectory_id = str(uuid.UUID(bytes=digest[8:24]))
    return session_id, trajectory_id


_last_request_millis = 0


def _generate_request_id(session_id: str, step: int, trajectory_id: str) -> str:
    """构造官方形态的 requestId：`agent/{conversationId}/{unixMs}/{trajectoryId}/{step}`。

    官方样本（3 份报文逐字核对），例如：
        agent/a89a2006-72b4-470d-8282-3f1e4c88dc29/1790410048596/d3a2e3d2-21f9-411b-967f-53811898c2cd/14

    - conversationId 段使用 sessionId 的单向哈希派生而非原文，同一会话稳定；
    - unixMs 段每次请求都不同 → 天然的幂等隔离，避免重试命中上一次的 429 / 旧缓存；
    - trajectoryId 段同一会话恒定（确定性派生自会话锚点），上游据此关联同一轨迹的
      连续 step，保证隐式 KV 缓存的路由亲和；不同会话天然隔离。
    """
    # Requests for the same session and step can start in the same millisecond.
    # This synchronous allocation keeps their request IDs distinct in each worker.
    global _last_request_millis
    ts_ms = _last_request_millis = max(time.time_ns() // 1_000_000, _last_request_millis + 1)
    if session_id:
        conversation = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:16]
    else:
        conversation = str(uuid.uuid4())
    if not trajectory_id:
        trajectory_id = str(uuid.uuid4())
    return f"agent/{conversation}/{ts_ms}/{trajectory_id}/{step}"


def _build_labels(model: str, trajectory_id: str, step: int) -> Dict[str, str]:
    """构建官方上下文标签元数据（键序按官方字母序排布）。

    官方样本（HAR 抓包）：
    {
      "last_step_index": "2",
      "model_enum": "MODEL_PLACEHOLDER_M320",
      "request_id": "0917011e-4959-4da9-8554-20499ed72869-2",
      "trajectory_id": "0917011e-4959-4da9-8554-20499ed72869",
      "used_claude": "false",
      "used_claude_conservative": "false",
      "used_non_gemini_model": "false"
    }

    step 传入「当前步索引」（模型轮数 + 1），与顶层 requestId 末段同步，
    request_id 同步使用该值。
    """
    model_lower = model.lower() if model else ""
    used_claude = "claude" in model_lower
    # used_non_gemini_model：Claude / gpt-oss 等非 Gemini 家族模型为 true
    used_non_gemini = used_claude or "gpt-oss" in model_lower or "grok" in model_lower
    return {
        "last_step_index": str(step),
        "model_enum": model,
        "request_id": f"{trajectory_id}-{step}",
        "trajectory_id": trajectory_id,
        "used_claude": str(used_claude).lower(),
        "used_claude_conservative": str(used_claude).lower(),
        "used_non_gemini_model": str(used_non_gemini).lower(),
    }


def _contents_have_tool_interactions(contents: Any) -> bool:
    """判断 contents 历史中是否存在工具调用上下文。"""
    if not isinstance(contents, list):
        return False
    for content in contents:
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict) and (
                "functionCall" in part
                or "function_call" in part
                or "functionResponse" in part
                or "function_response" in part
            ):
                return True
    return False


def _should_forward_antigravity_header(header_name: str) -> bool:
    """判断是否向 Antigravity 上游透传该请求头。

    对齐官方 Antigravity Hub 出站请求头（100% 同构）：
    - 严格过滤 x-session-id / x-client-name / x-client-version / x-machine-id /
      x-vscode-sessionid 等客户端或前端内部识别标头，规避 WAF 异常标记；
    - 仅放行官方链路使用的追踪类标头。
    """
    normalized = header_name.strip().lower()
    if not normalized:
        return False
    if normalized.startswith("x-b3-"):
        return True
    return normalized in {
        "traceparent",
        "tracestate",
        "x-cloud-trace-context",
        "x-goog-request-params",
        "x-goog-user-project",
        "x-request-id",
    }


def _sanitize_antigravity_headers(extra_headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not extra_headers:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in extra_headers.items():
        if _should_forward_antigravity_header(key):
            sanitized[key] = value
    return sanitized


async def wrap_cli_request(
    gemini_request: Dict[str, Any],
    model: str,
    project_id: str,
) -> Tuple[Dict[str, Any], str]:
    """
    将 Gemini 格式请求包装成 Antigravity CLI 格式（对齐官方 v1internal 信封）。
    返回 (payload, request_id)。

    顶层信封严格遵循官方键序: project -> requestId -> request -> model -> userAgent -> requestType。
    """
    # Keep normalized aliases and isolate the caller's payload from retries.
    inner = copy.deepcopy(normalize_gemini_request_fields(gemini_request))

    # 注入 sessionId（联合锚点 = model + systemInstruction + 前两条消息特征，
    # 与 derive_session_key 同口径）：同会话跨轮次恒定，共 system 的不同会话不撞车，
    # 上游据此做隐式 KV 缓存的路由亲和；客户端显式携带 sessionId 时原样透传
    session_id = str(inner.get("sessionId") or "").strip()
    derived_session_id, derived_trajectory_id = _derive_upstream_session_ids(inner, model)
    if not session_id:
        session_id = derived_session_id or f"-{uuid.uuid4().int % 9_000_000_000_000_000_000}"
        inner["sessionId"] = session_id

    # step：以 contents 中已完成的模型轮数为基准（当前步 = 模型轮数 + 1），
    # 同步写入 requestId 末段与 labels（last_step_index / request_id 后缀），
    # 保证轨迹元数据随对话轮次连续递增
    model_turns = 0
    contents = inner.get("contents")
    if isinstance(contents, list):
        model_turns = sum(
            1
            for content in contents
            if isinstance(content, dict) and content.get("role") == "model"
        )
    step = model_turns + 1

    # requestId：官方 5 段形态 agent/{conversationId}/{unixMs}/{trajectoryId}/{step}
    # trajectoryId 与 sessionId 同源确定性派生（无锚点时各自随机兜底），
    # 与顶层 requestId 严格同步一致
    trajectory_id = derived_trajectory_id or str(uuid.uuid4())
    request_id = _generate_request_id(session_id, step, trajectory_id)

    # 注入 labels（step 即当前步索引，与 requestId 末段同步）
    inner["labels"] = _build_labels(model, trajectory_id, step)

    # requestType 与 toolConfig 对齐官方 agent 状态机：
    # - 常规智能体交互（有 tools 或含工具调用上下文）→ agent；
    # - 纯文本问答 / 补全 → chat，且绝不携带 tools/toolConfig（避免 Agent 资源池限流）。
    tools = inner.get("tools")
    has_tools = isinstance(tools, list) and any(isinstance(tool, dict) and tool for tool in tools)
    has_tool_interactions = _contents_have_tool_interactions(inner.get("contents"))
    is_agent_request = has_tools or has_tool_interactions
    is_image_request = "image" in (model or "").lower()

    if is_agent_request and not is_image_request:
        # toolConfig 纯透传：客户端有就传，没有就不传（对齐官方 Antigravity 报文，
        # 绝不主动注入任何默认 mode），连同 allowedFunctionNames 等字段完整保留。
        # tools 兜底拆分为官方单函数独立切片形态（上游 normalize 已拆，此为最终出口保证）
        if isinstance(inner.get("tools"), list):
            inner["tools"] = _split_tools_single_function(inner["tools"])
        # 孤立悬挂的 toolConfig（无对应 tools）会引发上游 400 INVALID_ARGUMENT
        if not has_tools:
            inner.pop("toolConfig", None)
    else:
        # 无工具交互 / 图片生成：剥离客户端可能携带的 tools/toolConfig，对齐官方 chat 报文
        inner.pop("tools", None)
        inner.pop("toolConfig", None)

    # 内部 request 拓扑对齐官方键序:
    # contents -> systemInstruction -> tools -> toolConfig -> labels -> generationConfig -> sessionId -> 其余
    inner = _align_inner_request_topology(inner)

    payload = {
        "project": project_id,
        "requestId": request_id,
        "request": inner,
        "model": model,
        "userAgent": "antigravity",
        "requestType": "image_gen" if is_image_request else ("agent" if is_agent_request else "chat"),
    }
    return payload, request_id


def _renew_attempt_request_id(payload: Dict[str, Any]) -> None:
    request = payload["request"]
    labels = request["labels"]
    payload["requestId"] = _generate_request_id(
        str(request["sessionId"]), int(labels["last_step_index"]), labels["trajectory_id"]
    )


def _align_inner_request_topology(inner: Dict[str, Any]) -> Dict[str, Any]:
    """内部 request 严格按官方键序重组:
    contents -> systemInstruction -> tools -> toolConfig -> labels -> generationConfig -> sessionId -> 其余
    """
    key_order = [
        "contents",
        "systemInstruction",
        "tools",
        "toolConfig",
        "labels",
        "generationConfig",
        "sessionId",
    ]
    reordered: Dict[str, Any] = {}
    for key in key_order:
        if key in inner:
            reordered[key] = inner.pop(key)
    # 其余未知/扩展字段保留在末尾
    for key, value in inner.items():
        if key not in reordered:
            reordered[key] = value
    return reordered


# ==================== 辅助函数 ====================

def build_antigravity_headers(
    access_token: str,
    extra_headers: Optional[Dict[str, str]] = None,
    model_name: str = "",
) -> Dict[str, str]:
    """构建 Antigravity CLI API 请求头（对齐官方 Hub 出站指纹）。"""
    headers = {
        "User-Agent": ANTIGRAVITY_USER_AGENT,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
    }

    for key, value in _sanitize_antigravity_headers(extra_headers).items():
        headers.setdefault(key, value)

    return headers


def _is_retryable_status(status_code: int, disable_error_codes: List[int]) -> bool:
    """统一判断是否属于可重试状态码。"""
    return status_code in (429, 500, 503) or status_code in disable_error_codes


async def _record_model_not_found(email: str, model: str, access_token: str, error: str, *, expected_revision=None):
    try:
        await mark_model_unavailable(email, model, access_token, expected_revision=expected_revision)
    except Exception as exc:
        log.warning(f"无法更新模型可用状态: {type(exc).__name__}")
    await record_api_call_error(email, 404, model_name=model, error_message=error, expected_revision=expected_revision)


async def _dispatch_attempt(candidate, model, session_key, request_id, selector, previously_failed):
    if not previously_failed:
        return await acquire_dispatch(candidate, model, session_key, request_id, selector)

    async def select_preferred(model_name, session_key, exclude_emails=(), *, fallback=None):
        if previously_failed.difference(exclude_emails):
            try:
                fresh = await selector(model_name=model_name, session_key=session_key,
                                       exclude_emails=previously_failed.union(exclude_emails))
            except RateLimited:
                # Only the untried accounts were checked; the ordinary admission
                # check still applies to any previously failed credential.
                fresh = None
            if fresh:
                return fresh
            if fallback is not None:
                return fallback
        return await selector(model_name=model_name, session_key=session_key, exclude_emails=exclude_emails)

    if candidate[0] in previously_failed:
        candidate = await select_preferred(model, session_key, fallback=candidate)
    return await acquire_dispatch(candidate, model, session_key, request_id, select_preferred)


async def _switch_credential_for_retry(
    *,
    next_cred_task: Optional[asyncio.Task],
    retry_interval: float,
    refresh_credential_fast: Callable[[], Any],
    apply_cred_result: Callable[[Tuple[str, Dict[str, Any]]], bool],
    log_prefix: str,
) -> Tuple[bool, Optional[asyncio.Task]]:
    """优先使用预热凭证，失败后退回同步刷新。"""
    if next_cred_task is not None:
        try:
            cred_result = await next_cred_task
            next_cred_task = None
            if cred_result and apply_cred_result(cred_result):
                await asyncio.sleep(retry_interval)
                return True, next_cred_task
        except Exception as e:
            log.warning(f"{log_prefix} 预热凭证任务失败: {e}")
            next_cred_task = None

    await asyncio.sleep(retry_interval)
    if await refresh_credential_fast():
        return True, next_cred_task

    return False, next_cred_task


async def _cancel_credential_prefetch(task):
    """Join owned background work even if its parent is being cancelled."""
    if task is not None:
        with CancelScope(shield=True):
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def _close_upstream_stream(stream, delivery=None):
    """Release an attempt before retrying, including on cancellation or errors."""
    if stream is None:
        return
    close = stream.aclose
    with CancelScope(shield=True):
        try:
            await close()
        except Exception as exc:
            log.warning(f"Upstream stream cleanup failed: {type(exc).__name__}")
    # finalize() may already be iterating its closers. In that case leave its
    # list alone; closing an exhausted async generator again is harmless.
    if delivery and not delivery.closed and close in delivery.closers:
        delivery.closers.remove(close)


def _prepend_signature_parts(chunk, signatures):
    """Attach deferred metadata to confirmed output without inventing text/STOP."""
    was_bytes = isinstance(chunk, bytes)
    event = json.loads(chunk[5:])
    response = event.get("response", event)
    candidates = response.get("candidates", [])
    by_index = {}
    for position, candidate in enumerate(candidates):
        # Inserting a metadata-only candidate must not change the implicit
        # indexes of the original candidates in this event.
        candidate.setdefault("index", position)
        by_index[candidate["index"]] = candidate
    prefixes = {}
    for index, part in signatures:
        prefixes.setdefault(index, []).append(part)
    for index, parts in prefixes.items():
        candidate = by_index.get(index)
        if candidate is None:
            candidates.insert(0, {"index": index, "content": {"role": "model", "parts": parts}})
        else:
            content = candidate.get("content") or {}
            candidate["content"] = {**content, "parts": parts + content.get("parts", [])}
    response["candidates"] = candidates
    result = "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"
    return result.encode() if was_bytes else result


# ==================== 新的流式和非流式请求函数 ====================

def _derive_session_key(model_name: str, body: Dict[str, Any]) -> Optional[str]:
    """从 API 请求体派生会话 key（凭证亲和 cache 模式使用）

    body 格式: {"model": ..., "request": <Gemini 规范化请求>}
    hash(模型 + systemInstruction 或 第一条 user 消息) —— 后者必须是经过所有格式处理后的内容
    """
    inner = body.get("request") if isinstance(body.get("request"), dict) else body
    return derive_session_key(model_name, inner)


def _base_log_entry(
    model_name: str,
    email: str,
    is_stream: bool,
    log_ctx: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """构建请求日志条目的公共字段

    model_name: 后端实际使用的真实模型名（映射后）
    log_ctx.client_model: 客户端请求的原始模型名（可能带功能前缀）
    """
    ctx = log_ctx or {}
    return {
        "request_id": ctx.get("request_id") or (current_delivery.get().request_id if current_delivery.get() else new_request_id()),
        "api_format": ctx.get("api_format", ""),
        "client_ip": ctx.get("client_ip", ""),
        "model_name": ctx.get("client_model") or model_name,
        "upstream_model_name": model_name,
        "credential_email": email,
        "is_stream": is_stream,
    }


async def _record_request_body_detail(
    status_code: int,
    error_message: str,
    log_entry: Dict[str, Any],
    *,
    url: str,
    headers: Optional[Dict[str, str]],
    payload: Any,
    response_body: str,
    is_stream: bool,
    diagnostics: Optional[Dict[str, Any]] = None,
) -> str:
    """
    请求体详细日志：开关开启后按全部请求或指定状态码写盘（JSON 文件），
    数据库只保存文件索引路径。任何异常不影响主流程。
    """
    try:
        if not await get_request_log_enabled():
            return ""
        if not await usage_log.config.get_usage_log_enabled():
            return ""
        if not await get_request_log_all_requests() and status_code not in await get_request_log_error_codes():
            return ""

        rel_path = await record_request_body(
            request_id=str(log_entry.get("request_id", "")),
            response_id=str(log_entry.get("response_id", "")),
            model_name=str(log_entry.get("model_name", "")),
            upstream_model_name=str(log_entry.get("upstream_model_name", "")),
            credential_email=str(log_entry.get("credential_email", "")),
            is_stream=is_stream,
            status_code=status_code,
            url=url,
            headers=headers,
            payload=payload,
            response_body=response_body,
            error_message=error_message,
            diagnostics=diagnostics,
        )
        return rel_path or ""
    except Exception as e:
        log.debug(f"请求体详细日志写入失败（不影响主流程）: {e}")
        return ""


async def stream_request(
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
    log_ctx: Optional[Dict[str, Any]] = None,
):
    """
    流式请求函数

    Args:
        body: 请求体
        native: 是否返回原生bytes流，False则返回str流
        headers: 额外的请求头

    Yields:
        Response对象（错误时）或 bytes流/str流（成功时）
    """
    model_name = body.get("model", "")

    # 请求日志：开始时间与基础条目（成功/失败均会记录一次）
    log_entry = _base_log_entry(model_name, "", True, log_ctx)
    log_start_time = time.time()
    delivery = current_delivery.get()
    if delivery:
        await delivery.begin_segment()

    preparation_logged = False

    async def finalize_preparation(continuing=False):
        nonlocal preparation_logged
        if preparation_logged:
            return
        disconnected = delivery and delivery.disconnect_source
        status = 499 if disconnected else 500
        message = ("CLIENT_DISCONNECTED_BEFORE_UPSTREAM: 获取凭证或准备请求时客户端断开，尚未调用上游"
                   if disconnected else "REQUEST_PREPARATION_INTERRUPTED: 上游调用前请求准备被中断")
        with CancelScope(shield=True):
            detail = await _record_request_body_detail(
                status, message, log_entry, url="", headers=None, payload=body,
                response_body="", is_stream=True,
                diagnostics={"upstream_request_started": False},
            )
            await usage_log.record_usage_log({**log_entry, "status_code": status, "success": False,
                "error_message": message, "use_time": time.time() - log_start_time,
                "request_log_file": detail})
            preparation_logged = True

    if delivery:
        delivery.callbacks.append(finalize_preparation)

    # 凭证亲和: 从请求内容派生会话 key
    session_key = _derive_session_key(model_name, body)

    # 1. 获取有效凭证
    selection_error = None
    try:
        cred_result = await get_valid_credential(model_name=model_name, session_key=session_key, report_missing_model=True)
    except (RateLimited, PoolModelUnavailable) as exc:
        cred_result, selection_error = None, exc

    if not cred_result:
        if selection_error:
            status, message = selection_error.status_code, str(selection_error)
        else:
            status, message = 500, "当前无可用凭证"
        log_entry.update({
            "status_code": status, "success": False,
            "error_message": message, "use_time": time.time() - log_start_time,
        })
        with CancelScope(shield=True):
            log_entry["request_log_file"] = await _record_request_body_detail(
                status, message, log_entry,
                url="", headers=None, payload=body, response_body="", is_stream=True,
            )
            await usage_log.record_usage_log(log_entry)
            preparation_logged = True
        yield Response(
            content=json.dumps({"error": message}),
            status_code=status,
            headers={"Retry-After": str(selection_error.retry_after)} if isinstance(selection_error, RateLimited) else None,
            media_type="application/json"
        )
        return

    current_file, credential_data = cred_result
    log_entry["credential_email"] = current_file
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = generation_project_id(credential_data)

    if not access_token:
        log.error(f"[ANTIGRAVITY STREAM] No access token in credential: {current_file}")
        # 记录失败日志（含涉及的凭证）
        log_entry.update({
            "status_code": 500, "success": False,
            "error_message": "凭证中没有访问令牌", "use_time": time.time() - log_start_time,
        })
        with CancelScope(shield=True):
            log_entry["request_log_file"] = await _record_request_body_detail(
                500, log_entry["error_message"], log_entry,
                url="", headers=None, payload=body, response_body="", is_stream=True,
            )
            await usage_log.record_usage_log(log_entry)
            preparation_logged = True
        yield Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )
        return

    # 2. 构建URL和请求头（端点来自配置，支持面板热更新）
    target_url = f"{await get_antigravity_api_url()}/v1internal:streamGenerateContent?alt=sse"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    inner_request = normalize_gemini_request_fields(inner_request)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id)

    # 3. 调用stream_post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    last_error_body = ""  # 详细日志：最后一次的错误响应体
    next_cred_task = None  # 预热的下一个凭证任务
    empty_retries = 0
    previously_failed: set[str] = set()
    attempted_emails: set[str] = set()
    excluded_emails: set[str] = set()

    async def select_credential(model_name=None, session_key=None, exclude_emails=None):
        excluded = excluded_emails | set(exclude_emails or ())
        if excluded:
            return await get_valid_credential(model_name=model_name, session_key=session_key,
                                              exclude_emails=excluded)
        return await get_valid_credential(model_name=model_name, session_key=session_key)
    previous_finalizer = finalize_preparation if delivery else None

    async def close_prefetch():
        await _cancel_credential_prefetch(next_cred_task)

    if delivery:
        delivery.closers.append(close_prefetch)

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, credential_data, access_token, auth_headers, project_id, final_payload
        cred_result = await select_credential(model_name=model_name, session_key=session_key)
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = generation_project_id(credential_data)
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, credential_data, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = generation_project_id(credential_data)
        if not access_token:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    try:
        for attempt in range(max_retries + 1):
            lease = None
            last_error_body = ""
            last_error_response = None
            result_logged = False
            selected_result = None
            success_recorded = False  # 标记是否已记录成功
            content_delivered = False
            meaningful_content = False
            event_counts = {}
            upstream_event_seen = False
            outcome = "pending"
            need_retry = False  # 标记是否需要重试
            retry_model_not_found = False
            stream_usage = StreamUsage()
            deferred_signatures = []
            deferred_frames = []
            prefix_identity = {}
            prefix_bytes = 0
            deferred_metadata_bytes = 0
            attempt_start_time = time.time()  # 本次上游调用开始时间
            stream_metadata = StreamMetadata()
            completion = StreamCompletion((inner_request.get("generationConfig") or {}).get("candidateCount", 1))
            candidate_blocks = CandidateBlocks(completion.expected_candidates) if completion.expected_candidates > 1 else None
            if delivery:
                delivery.gemini_completion.expected_candidates = completion.expected_candidates

            upstream_stream = None
            transport_closed = False

            async def close_attempt_transport():
                nonlocal transport_closed
                if transport_closed:
                    return
                with CancelScope(shield=True):
                    await _close_upstream_stream(upstream_stream, delivery)
                    if lease:
                        await lease.close()
                    transport_closed = True

            async def _write_stream_result(status_code: int, success: bool, error_message: str = "") -> None:
                """
                写入一次上游调用的请求日志（调用一次谷歌 API 产生一条日志；
                重试会在后续 attempt 各自再记一条，属于正常行为）。
                """
                nonlocal result_logged
                if result_logged:
                    return
                if lease:
                    await lease.result(success=success, cancelled_empty=bool(
                        status_code == 499 and delivery and delivery.disconnect_source
                        and not delivery.conversion_error and not upstream_event_seen
                        and not content_delivered and delivery.bytes_sent == 0))

                log.info("[UPSTREAM_STREAM_METADATA] " + json.dumps({
                    "request_id": log_entry.get("request_id", ""),
                    "model": model_name, "status": status_code,
                    "attempt": attempt + 1, "outcome": outcome,
                    "event_counts": event_counts, "content_delivered": content_delivered,
                    **stream_metadata.as_dict(), **completion.as_dict(),
                    **(delivery.metadata() if delivery else {}),
                }, ensure_ascii=True))

                attempt_entry = {
                    **log_entry, "credential_email": current_file,
                    "response_id": stream_metadata.response_id or extract_response_id(last_error_body),
                }
                # 请求体详细日志与数据库条目关联本次尝试。
                request_log_file = await _record_request_body_detail(
                    status_code,
                    error_message,
                    attempt_entry,
                    url=target_url,
                    headers=auth_headers,
                    payload=final_payload,
                    response_body=last_error_body,
                    is_stream=True,
                    diagnostics={
                        "outcome": outcome, "event_counts": event_counts,
                        **stream_metadata.as_dict(), **completion.as_dict(),
                        **(delivery.metadata() if delivery else {}),
                    },
                )

                usage_source = last_error_response.body if last_error_response is not None else None
                if stream_usage.counts:
                    usage_source = {"usageMetadata": stream_usage.counts}
                # 每条日志记录本次调用实际使用的凭证
                await usage_log.record_usage_log({
                    **attempt_entry,
                    "credential_email": current_file,
                    "status_code": status_code,
                    "success": success,
                    "use_time": time.time() - attempt_start_time,
                    "error_message": error_message[:500],
                    "request_log_file": request_log_file,
                    **extract_usage_tokens(usage_source),
                })
                result_logged = True

            async def _log_stream_result(status_code: int, success: bool, error_message: str = "") -> None:
                nonlocal selected_result
                # Preserve an already decided outcome if cancellation races log I/O.
                if selected_result is None:
                    selected_result = (status_code, success, error_message)
                if delivery and success:
                    # Settle only after the adapter has delivered a terminal event.
                    delivery.completed_lease = lease
                    return
                with CancelScope(shield=True):
                    if not selected_result[1]:
                        # A converter can stop iterating immediately after receiving an error.
                        # Release the failed attempt before exposing that error to the client.
                        await close_attempt_transport()
                    await _write_stream_result(*selected_result)

            async def _log_interrupted() -> None:
                nonlocal outcome
                if delivery:
                    # The response owner finalizes after receive/send has settled.
                    return
                if completion.complete and meaningful_content:
                    outcome = "complete"
                    await _log_stream_result(200, True)
                else:
                    await _log_stream_result(500, False, "STREAM_INTERRUPTED_UNKNOWN: 任务被取消或生成器关闭，未观察到客户端断开")

            async def finalize_delivery(continuing=False):
                nonlocal outcome
                if result_logged:
                    return
                if selected_result and not selected_result[1]:
                    await _write_stream_result(*selected_result)
                elif delivery.conversion_error:
                    outcome = "conversion_error"
                    await _write_stream_result(delivery.conversion_error.status_code, False,
                        delivery.conversion_error.body.decode("utf-8"))
                elif completion.complete and meaningful_content and (
                    delivery.complete or (continuing and not delivery.disconnect_source and not delivery.error_sent)
                ):
                    outcome = "segment_complete" if continuing else "complete"
                    await _write_stream_result(200, True)
                elif delivery.disconnect_source:
                    outcome = "downstream_interrupted"
                    await _write_stream_result(499, False, "CLIENT_DISCONNECTED: 下游连接中断，响应未完整发送")
                else:
                    outcome = "internal_interrupted"
                    await _write_stream_result(500, False, "STREAM_INTERRUPTED_UNKNOWN: 内部取消或关闭，响应未完整发送")

            if delivery:
                if previous_finalizer is not None:
                    delivery.callbacks.remove(previous_finalizer)
                delivery.callbacks.append(finalize_delivery)
                previous_finalizer = finalize_delivery

            def output_chunks(chunk, has_content):
                nonlocal content_delivered, meaningful_content, deferred_metadata_bytes
                if native or has_content:
                    content_delivered = meaningful_content = True
                if not meaningful_content:
                    return
                for frame in deferred_frames:
                    completion.observe(frame)
                    yield frame
                deferred_frames.clear()
                if deferred_signatures:
                    chunk = _prepend_signature_parts(chunk, deferred_signatures)
                    deferred_signatures.clear()
                deferred_metadata_bytes = 0
                completion.observe(chunk)
                yield chunk

            try:
                try:
                    picked, lease = await _dispatch_attempt(
                        (current_file, credential_data),
                        model_name, session_key, log_entry["request_id"], select_credential, previously_failed)
                except (PoolCooling, PoolModelUnavailable) as exc:
                    outcome = {404: "model_unavailable", 429: "credential_rate_limited"}.get(exc.status_code, "credential_cooling")
                    await _log_stream_result(exc.status_code, False, str(exc))
                    yield Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                                   headers={"Retry-After": str(exc.retry_after)} if isinstance(exc, PoolCooling) else None,
                                   media_type="application/json")
                    return
                if not apply_cred_result(picked):
                    raise ValueError("所选凭证缺少访问令牌")
                revision = credential_data.get("credential_revision")
                attempted_emails.add(current_file)
                if attempt:
                    _renew_attempt_request_id(final_payload)
                attempt_start_time = time.time()
                retry_headers = {}
                upstream_stream = stream_post_async(
                    credential_email=current_file, credential_revision=revision,
                    url=target_url,
                    body=final_payload,
                    native=native,
                    retry_headers=retry_headers,
                    headers=auth_headers
                )
                if delivery:
                    delivery.closers.append(upstream_stream.aclose)
                upstream_chunks = lease.iterate(upstream_stream)
                record_has_data = False
                async for chunk in upstream_chunks:
                    upstream_event_seen = True
                    # Validate protocol events without reclassifying response text.
                    # Comments keep long connections alive but are never content.
                    event_kind = "http_error"
                    if not isinstance(chunk, Response) and not native:
                        event_kind, classified = inspect_line(
                            chunk, allow_text_notice=not success_recorded, allow_fragment=True, usage=stream_usage,
                            candidate_blocks=candidate_blocks
                        )
                        if event_kind == "blank_data" and record_has_data:
                            continue
                        if event_kind in ("fragment", "blank_data"):
                            chunk = await finish_data_record(chunk, upstream_chunks)
                            event_kind, classified = inspect_line(chunk, allow_text_notice=not success_recorded,
                                                                  usage=stream_usage, candidate_blocks=candidate_blocks)
                            record_has_data = False
                        elif chunk == "":
                            record_has_data = False
                        elif event_kind not in ("heartbeat", "empty"):
                            record_has_data = True
                        if event_kind == "error" and classified.status_code in (429, 503):
                            classified.headers.update(retry_headers)
                        stream_metadata.observe(chunk)
                        chunk = classified
                        event_counts[event_kind] = event_counts.get(event_kind, 0) + 1
                        if event_kind == "empty":
                            continue
                        if event_kind == "heartbeat":
                            yield chunk
                            continue
                        if event_kind == "done" and candidate_blocks and candidate_blocks.unanswered_error is not None:
                            event_kind, chunk = "blocked", candidate_blocks.unanswered_error
                        if event_kind == "done":
                            # Clients stop reading at [DONE], so validate the candidate
                            # count before forwarding it. An error after [DONE] is lost.
                            completion.observe(chunk)
                            if not success_recorded:
                                # The attempt is empty at its protocol boundary, even if the socket stays open.
                                break
                            if not completion.complete:
                                event_kind = "incomplete_stream"
                                chunk = stream_error(500, "UPSTREAM_INCOMPLETE_STREAM",
                                    f"上游已结束，仅返回 {len(completion.candidates)}/{completion.expected_candidates} 个候选答案")
                            elif delivery:
                                # Some adapters stop iterating as soon as they see [DONE].
                                # Close the completed transport before handing them that terminal.
                                outcome = "complete"
                                await close_attempt_transport()
                                await _log_stream_result(200, True)
                                yield chunk
                                return
                        if event_kind == "metadata" and not success_recorded:
                            # Hold attempt-specific metadata until the upstream actually
                            # produces output. An empty candidate can finish before a sibling.
                            value = json.loads(chunk[5:])
                            value = value.get("response", value)
                            candidates = value.get("candidates") or []
                            if completion.expected_candidates > 1 and any(
                                isinstance(reason := c.get("finishReason"), str)
                                and reason and reason != "FINISH_REASON_UNSPECIFIED"
                                for c in candidates
                            ):
                                deferred_metadata_bytes += len(chunk.encode("utf-8"))
                                if deferred_signatures:
                                    chunk = _prepend_signature_parts(chunk, deferred_signatures)
                                    deferred_signatures.clear()
                                deferred_frames.append(chunk)
                            else:
                                if log_entry["api_format"] == "gemini":
                                    for prefix in gemini_metadata_frames(value, prefix_identity):
                                        size = len(prefix.encode("utf-8"))
                                        # Optional source metadata must not exhaust the signature
                                        # buffer or turn an otherwise valid answer into HTTP 500.
                                        if prefix_bytes + size <= 1024 * 1024:
                                            deferred_frames.append(prefix)
                                            prefix_bytes += size
                                for index, candidate in enumerate(candidates):
                                    for part in (candidate.get("content") or {}).get("parts", []):
                                        if not part.get("text") and part.get("thoughtSignature"):
                                            deferred_signatures.append((candidate.get("index", index), copy.deepcopy(part)))
                                            deferred_metadata_bytes += len(json.dumps(part, ensure_ascii=True))
                            if deferred_metadata_bytes <= 1024 * 1024:
                                continue
                            event_kind = "invalid"
                            chunk = stream_error(500, "UPSTREAM_INVALID_EVENT", "上游首段元数据超过缓冲限制")
                    if event_kind in ("blocked", "error") and getattr(chunk, "upstream_content", None):
                        for partial in output_chunks(chunk.upstream_content, True):
                            yield partial
                    if event_kind in ("blocked", "invalid", "incomplete_stream"):
                        outcome = event_kind
                        completion.error = True
                        last_error_body = chunk.body.decode("utf-8")
                        await _log_stream_result(chunk.status_code, False, last_error_body)
                        yield chunk
                        return
                    # 判断是否是Response对象
                    if isinstance(chunk, Response):
                        if chunk.status_code in (204, 205) and not chunk.body:
                            # A bodyless generation ends the attempt just like empty SSE EOF.
                            break
                        outcome = "upstream_error"
                        completion.error = True
                        status_code = chunk.status_code
                        last_error_response = chunk  # 记录最后一次错误

                        # 缓存错误解析结果,避免重复decode
                        error_body = None
                        try:
                            error_body = chunk.body.decode('utf-8') if isinstance(chunk.body, bytes) else str(chunk.body)
                        except Exception:
                            error_body = ""
                        last_error_body = error_body or ""

                        if status_code == 404:
                            model_missing = is_model_not_found(
                                last_error_body, generated_content=event_kind == "error" and content_delivered
                            )
                            if model_missing:
                                await _record_model_not_found(current_file, model_name, access_token, last_error_body, expected_revision=revision)
                            await _log_stream_result(404, False, last_error_body or "HTTP 404")
                            if model_missing and not content_delivered and retry_config["retry_enabled"] and attempt < max_retries:
                                excluded_emails.add(current_file)
                                need_retry = retry_model_not_found = True
                                break
                            yield chunk
                            return

                        # 如果错误码是429、503或者在禁用码当中，做好记录后进行重试
                        if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                            log.warning(f"[ANTIGRAVITY STREAM] 流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")

                            # 解析冷却时间
                            cooldown_until = None
                            if status_code in (429, 503):
                                cooldown_until = await parse_and_log_cooldown(error_body, chunk.headers.get("retry-after"))

                            # 预热下一个凭证
                            if (not content_delivered and retry_config["retry_enabled"]
                                    and next_cred_task is None and attempt < max_retries):
                                next_cred_task = asyncio.create_task(
                                    select_credential(
                                        model_name=model_name, session_key=session_key, exclude_emails={current_file}
                                    )
                                )

                            # 记录错误并切换凭证
                            await record_api_call_error(current_file, status_code, cooldown_until, model_name=model_name,
                                error_message=error_body, expected_revision=revision
                            )
                            # 本次上游调用产生一条日志（重试会再各记一条）
                            await _log_stream_result(status_code, False, error_body[:500] or f"HTTP {status_code}")

                            # 检查是否应该重试
                            should_retry = await handle_error_with_retry(
                                status_code, current_file,
                                retry_config["retry_enabled"] and not content_delivered,
                                attempt, max_retries, expected_revision=revision)

                            if should_retry and not content_delivered and attempt < max_retries:
                                need_retry = True
                                break  # 跳出内层循环，准备重试
                            else:
                                # 不重试，直接返回原始错误
                                log.error(f"[ANTIGRAVITY STREAM] 达到最大重试次数或不应重试，返回原始错误")
                                yield chunk
                                return
                        else:
                            # 错误码不在禁用码当中，直接返回，无需重试
                            log.error(f"[ANTIGRAVITY STREAM] 流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")
                            await record_api_call_error(current_file, status_code, None, model_name=model_name,
                                error_message=error_body, expected_revision=revision
                            )
                            await _log_stream_result(status_code, False, error_body[:500] or f"HTTP {status_code}")
                            yield chunk
                            return
                    else:
                        # Only actual model content marks the credential responsive.
                        if not success_recorded and (native or event_kind == "content"):
                            await record_api_call_success(current_file, model_name=model_name, expected_revision=revision)
                            success_recorded = True
                            log.debug(f"[ANTIGRAVITY STREAM] 开始接收流式响应，模型: {model_name}")

                        # 记录原始chunk内容（用于调试）
                        if isinstance(chunk, bytes):
                            log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(bytes): {chunk}")
                        else:
                            log.debug(f"[ANTIGRAVITY STREAM RAW] chunk(str): {chunk}")

                        for output in output_chunks(chunk, event_kind == "content"):
                            yield output

                else:
                    completion.eof = True
                if not need_retry and candidate_blocks and candidate_blocks.unanswered_error is not None:
                    outcome = "blocked"
                    completion.error = True
                    error = candidate_blocks.unanswered_error
                    await _log_stream_result(error.status_code, False, error.body.decode("utf-8"))
                    yield error
                    return
                # A clean socket EOF is not a protocol completion marker.
                if meaningful_content and not need_retry:
                    if not completion.complete and not native:
                        outcome = "incomplete_stream"
                        error = stream_error(500, "UPSTREAM_INCOMPLETE_STREAM", "上游响应未完整结束，缺少结束事件")
                        last_error_body = error.body.decode("utf-8")
                        await _log_stream_result(500, False, last_error_body)
                        yield error
                        return
                    outcome = "complete"
                    await _log_stream_result(200, True)
                    return
                elif not need_retry:
                    # 没有收到任何数据（空回复），需要重试
                    log.warning(f"[ANTIGRAVITY STREAM] 收到空回复，无任何内容，凭证: {current_file}")
                    outcome = "empty_response"
                    completion.error = True
                    last_error_response = stream_error(502, "UPSTREAM_EMPTY_RESPONSE", "上游未返回有效内容")
                    last_error_body = last_error_response.body.decode("utf-8")
                    # 本次上游调用（空回复）产生一条日志
                    await _log_stream_result(502, False, "UPSTREAM_EMPTY_RESPONSE")

                    # At most one empty-response recovery; respect the retry switch.
                    if retry_config["retry_enabled"] and attempt < max_retries and empty_retries < 1:
                        empty_retries += 1
                        need_retry = True
                    else:
                        log.error(f"[ANTIGRAVITY STREAM] 空回复达到最大重试次数")
                        yield last_error_response
                        return

                # 统一处理重试
                if need_retry:
                    previously_failed.update(attempted_emails - excluded_emails)
                    log.info(f"[ANTIGRAVITY STREAM] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                    switched, next_cred_task = await _switch_credential_for_retry(
                        next_cred_task=next_cred_task,
                        retry_interval=0 if retry_model_not_found else retry_interval,
                        refresh_credential_fast=refresh_credential_fast,
                        apply_cred_result=apply_cred_result,
                        log_prefix="[ANTIGRAVITY STREAM]",
                    )
                    if not switched:
                        yield last_error_response
                        return
                    continue  # 重试

            except CredentialReplaced as exc:
                outcome = "credential_replaced"
                await _log_stream_result(exc.status_code, False, str(exc))
                if retry_config["retry_enabled"] and attempt < max_retries:
                    continue
                yield Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                               media_type="application/json")
                return
            except RateLimited as exc:
                if isinstance(exc, QuotaCooling) and last_error_response is not None and last_error_response.status_code in (429, 503):
                    yield last_error_response
                    return
                yield Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                               headers={"Retry-After": str(exc.retry_after)}, media_type="application/json")
                return
            except (asyncio.CancelledError, GeneratorExit):
                # A client may close immediately after the final protocol event.
                # Preserve completed/error outcomes; only unfinished streams are 499.
                with CancelScope(shield=True):
                    if outcome == "pending":
                        outcome = "interrupted"
                    await _log_interrupted()
                raise

            except Exception as e:
                # A transport teardown failure cannot invalidate an already
                # complete protocol answer. Explicit upstream error events still
                # take their normal error path above.
                if isinstance(e, TransportError) and completion.complete and meaningful_content:
                    outcome = "complete"
                    log.warning(f"[ANTIGRAVITY STREAM] 完整结束事件之后连接关闭异常: {type(e).__name__}, request_id={log_entry.get('request_id')}")
                    await _log_stream_result(200, True)
                    return
                outcome = "transport_error"
                completion.error = True
                error_status = 500
                error_message = f"异常: {type(e).__name__}: {e}"
                log.error(f"[ANTIGRAVITY STREAM] 流式请求异常: {type(e).__name__}: {e}, 凭证: {current_file}")
                if not content_delivered and retry_config["retry_enabled"] and attempt < max_retries:
                    log.info(f"[ANTIGRAVITY STREAM] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                    if isinstance(e, TransportError):
                        previously_failed.add(current_file)
                    # 本次上游调用（异常）产生一条日志
                    await _log_stream_result(error_status, False, error_message)
                else:
                    # 所有重试都失败，返回最后一次的错误（如果有）
                    log.error(f"[ANTIGRAVITY STREAM] 所有重试均失败，最后异常: {e}")
                    await _log_stream_result(error_status, False, error_message)
                    if last_error_response:
                        yield last_error_response
                    else:
                        # 与上游一致：没有先前错误响应时，传输异常返回 500。
                        yield Response(
                            content=json.dumps({"error": error_message}),
                            status_code=error_status,
                            media_type="application/json"
                        )
                    return

            finally:
                await close_attempt_transport()

            # Only the exception-retry path falls through. Release the slot before backoff.
            await asyncio.sleep(retry_interval)

        # 所有重试均已耗尽（for循环正常结束）——每次上游调用均已各自记录日志，
        # 这里不额外记录，直接返回最后一次的错误响应
        log.error("[ANTIGRAVITY STREAM] 所有重试均失败")
        if last_error_response:
            yield last_error_response
        else:
            yield Response(
                content=json.dumps({"error": "请求失败，所有重试均已耗尽"}),
                status_code=429,
                media_type="application/json"
            )
    finally:
        await _cancel_credential_prefetch(next_cred_task)



async def non_stream_request(
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    log_ctx: Optional[Dict[str, Any]] = None,
) -> Response:
    """
    非流式请求函数

    Args:
        body: 请求体
        headers: 额外的请求头
        log_ctx: 请求日志上下文（api_format / client_ip / request_id）

    Returns:
        Response对象
    """
    model_name = body.get("model", "")

    # 请求日志：开始时间与基础条目（成功/失败均会记录一次）
    log_entry = _base_log_entry(model_name, "", False, log_ctx)
    log_start_time = time.time()

    # 详细日志上下文（发往上游的 URL/请求头/请求体；请求派发前为空）
    target_url = ""
    auth_headers: Optional[Dict[str, str]] = None
    final_payload: Dict[str, Any] = {}
    last_error_body = ""  # 详细日志：最后一次的错误响应体
    upstream_response_content = None
    lease = None
    current_file = ""  # 当前使用的凭证（未获取前为空）
    attempt_start_time = time.time()  # 本次上游调用开始时间（循环内每次重置）

    async def _log_result(status_code: int, success: bool, response_content: Any = None, error_message: str = "") -> None:
        """
        写入一次上游调用的请求日志（调用一次谷歌 API 产生一条日志；
        重试会在后续 attempt 各自再记一条，属于正常行为）。
        """
        if lease:
            await lease.result(success=success)

        if response_content is None:
            response_content = upstream_response_content
        attempt_entry = {
            **log_entry, "credential_email": current_file,
            "response_id": extract_response_id(response_content) or extract_response_id(last_error_body),
        }
        # 请求体详细日志与数据库条目关联本次尝试。
        request_log_file = await _record_request_body_detail(
            status_code,
            error_message,
            attempt_entry,
            url=target_url,
            headers=auth_headers,
            payload=final_payload or body,
            response_body=last_error_body,
            is_stream=False,
        )

        # Rejections and partial/empty answers can still report real usage.
        # Keep it independent of success, using the original upstream body.
        tokens = extract_usage_tokens(response_content)
        # 每条日志记录本次调用实际使用的凭证
        await usage_log.record_usage_log({
            **attempt_entry,
            "credential_email": current_file,
            "status_code": status_code,
            "success": success,
            "use_time": time.time() - attempt_start_time,
            "error_message": error_message[:500],
            "request_log_file": request_log_file,
            **tokens,
        })

    # 凭证亲和: 从请求内容派生会话 key
    session_key = _derive_session_key(model_name, body)

    # 1. 获取有效凭证
    try:
        cred_result = await get_valid_credential(model_name=model_name, session_key=session_key, report_missing_model=True)
    except (RateLimited, PoolModelUnavailable) as exc:
        await _log_result(exc.status_code, False, error_message=str(exc))
        return Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                        headers={"Retry-After": str(exc.retry_after)} if isinstance(exc, RateLimited) else None,
                        media_type="application/json")

    if not cred_result:
        status, message = 500, "当前无可用凭证"
        log.error(f"[ANTIGRAVITY] {message}")
        await _log_result(status, False, error_message=message)
        return Response(
            content=json.dumps({"error": message}),
            status_code=status,
            media_type="application/json"
        )

    current_file, credential_data = cred_result
    log_entry["credential_email"] = current_file
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = generation_project_id(credential_data)

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        # 记录失败日志（含涉及的凭证）
        await _log_result(500, False, error_message="凭证中没有访问令牌")
        return Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )

    # 2. 构建URL和请求头（端点来自配置，支持面板热更新）
    target_url = f"{await get_antigravity_api_url()}/v1internal:generateContent"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id)

    # 3. 调用post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务
    empty_retries = 0
    previously_failed: set[str] = set()
    attempted_emails: set[str] = set()
    excluded_emails: set[str] = set()

    async def select_credential(model_name=None, session_key=None, exclude_emails=None):
        excluded = excluded_emails | set(exclude_emails or ())
        if excluded:
            return await get_valid_credential(model_name=model_name, session_key=session_key,
                                              exclude_emails=excluded)
        return await get_valid_credential(model_name=model_name, session_key=session_key)

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, credential_data, access_token, auth_headers, project_id, final_payload
        cred_result = await select_credential(model_name=model_name, session_key=session_key)
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = generation_project_id(credential_data)
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, credential_data, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = generation_project_id(credential_data)
        if not access_token:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    try:
        for attempt in range(max_retries + 1):
            lease = None
            last_error_body = ""
            upstream_response_content = None
            # A later connection failure must not return the previous attempt's
            # 429/503 while its usage row records the current 500.
            last_error_response = None
            need_retry = False  # 标记是否需要重试
            retry_model_not_found = False
            attempt_start_time = time.time()  # 本次上游调用开始时间

            try:
                try:
                    picked, lease = await _dispatch_attempt(
                        (current_file, credential_data),
                        model_name, session_key, log_entry["request_id"], select_credential, previously_failed)
                except (PoolCooling, PoolModelUnavailable) as exc:
                    await _log_result(exc.status_code, False, error_message=str(exc))
                    return Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                                    headers={"Retry-After": str(exc.retry_after)} if isinstance(exc, PoolCooling) else None,
                                    media_type="application/json")
                if not apply_cred_result(picked):
                    raise ValueError("所选凭证缺少访问令牌")
                revision = credential_data.get("credential_revision")
                attempted_emails.add(current_file)
                if attempt:
                    _renew_attempt_request_id(final_payload)
                attempt_start_time = time.time()
                response = await post_async(
                    credential_email=current_file, credential_revision=revision,
                    url=target_url,
                    json=final_payload,
                    headers=auth_headers
                )
                upstream_response_content = response.content

                status_code = response.status_code

                # HTTP 200 may still contain a structured rejection or no answer.
                partial_answer = None
                if status_code == 200 or (status_code in (204, 205) and not response.content):
                    # Use the same UTF-8 JSON bytes for validation, usage and the
                    # downstream adapters. Some upstreams prefix JSON with a BOM.
                    response = strip_json_bom(response)
                    if not response.content:
                        kind, classified = "metadata", None
                    else:
                        expected = (final_payload.get("request", {}).get("generationConfig") or {}).get("candidateCount", 1)
                        candidate_blocks = CandidateBlocks(expected, streaming=False) if type(expected) is int and expected > 1 else None
                        kind, classified = inspect_line("data: " + response.text, allow_text_notice=True,
                                                       candidate_blocks=candidate_blocks)
                    if kind in ("blocked", "error", "invalid"):
                        error_body = classified.body
                        partial_answer = getattr(classified, "upstream_content", None)
                        if partial_answer:
                            payload = json.loads(error_body)
                            partial = json.loads(partial_answer[5:])
                            payload["partialResponse"] = partial.get("response", partial)
                            error_body = json.dumps(payload, ensure_ascii=False).encode()
                        last_error_body = error_body.decode()
                        if kind == "blocked":
                            await _log_result(classified.status_code, False,
                                response_content=response.content, error_message=last_error_body)
                            return Response(content=error_body, status_code=classified.status_code,
                                            media_type="application/json")
                        # Explicit embedded HTTP errors use the same credential and
                        # retry policy as transport-level errors; do not fake 200.
                        error_headers = {"content-type": "application/json"}
                        if classified.status_code in (429, 503):
                            error_headers.update(retry_after_headers(response.headers))
                        response = httpx.Response(classified.status_code, content=error_body, headers=error_headers)
                        status_code = classified.status_code
                    elif kind != "content":
                        last_error_response = stream_error(502, "UPSTREAM_EMPTY_RESPONSE", "上游未返回有效内容")
                        last_error_body = last_error_response.body.decode()
                        await _log_result(502, False, response_content=response.content,
                                          error_message=last_error_body)
                        if retry_config["retry_enabled"] and attempt < max_retries and empty_retries < 1:
                            empty_retries += 1
                            need_retry = True
                        else:
                            return last_error_response
                    else:
                        await record_api_call_success(current_file, model_name=model_name, expected_revision=revision)
                        await _log_result(200, True, response_content=response.content)
                        return Response(content=response.content, status_code=200,
                                        headers=decoded_response_headers(response.headers))

                # 失败 - 记录最后一次错误
                if status_code != 200 and not need_retry:
                    last_error_response = Response(
                        content=response.content,
                        status_code=status_code,
                        headers=decoded_response_headers(response.headers)
                    )

                    # 判断是否需要重试
                    # 缓存错误文本,避免重复解析
                    error_text = ""
                    try:
                        error_text = response.text
                    except Exception:
                        pass
                    last_error_body = error_text

                    if status_code == 404:
                        model_missing = is_model_not_found(error_text, generated_content=bool(partial_answer))
                        if model_missing:
                            await _record_model_not_found(current_file, model_name, access_token, error_text, expected_revision=revision)
                        await _log_result(404, False, error_message=error_text or "HTTP 404")
                        if partial_answer or not model_missing or not retry_config["retry_enabled"] or attempt >= max_retries:
                            return last_error_response
                        excluded_emails.add(current_file)
                        need_retry = retry_model_not_found = True
                    elif _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                        log.warning(f"[ANTIGRAVITY] 非流式请求失败 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")

                        # 解析冷却时间
                        cooldown_until = None
                        if status_code in (429, 503):
                            cooldown_until = await parse_and_log_cooldown(error_text, response.headers.get("retry-after"))

                        # 并行预热下一个凭证,不阻塞当前处理
                        if retry_config["retry_enabled"] and next_cred_task is None and attempt < max_retries:
                            next_cred_task = asyncio.create_task(
                                    select_credential(
                                        model_name=model_name, session_key=session_key, exclude_emails={current_file}
                                    )
                                )

                        # 记录错误并切换凭证
                        await record_api_call_error(current_file, status_code, cooldown_until, model_name=model_name,
                            error_message=error_text, expected_revision=revision
                        )
                        # 本次上游调用产生一条日志（重试会再各记一条）
                        await _log_result(status_code, False, error_message=error_text[:500] or f"HTTP {status_code}")

                        # 检查是否应该重试
                        should_retry = await handle_error_with_retry(status_code, current_file, retry_config["retry_enabled"], attempt, max_retries, expected_revision=revision)

                        if should_retry and attempt < max_retries:
                            need_retry = True
                        else:
                            # 不重试，直接返回原始错误
                            log.error(f"[ANTIGRAVITY] 达到最大重试次数或不应重试，返回原始错误")
                            return last_error_response
                    else:
                        # 错误码不在禁用码当中，直接返回，无需重试
                        log.error(f"[ANTIGRAVITY] 非流式请求失败，非重试错误码 (status={status_code}), 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")
                        await record_api_call_error(current_file, status_code, None, model_name=model_name,
                            error_message=error_text, expected_revision=revision
                        )
                        await _log_result(status_code, False, error_message=error_text[:500] or f"HTTP {status_code}")
                        return last_error_response

                # 统一处理重试
                if need_retry:
                    previously_failed.update(attempted_emails - excluded_emails)
                    log.info(f"[ANTIGRAVITY] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")
                    if lease:
                        await lease.close()

                    switched, next_cred_task = await _switch_credential_for_retry(
                        next_cred_task=next_cred_task,
                        retry_interval=0 if retry_model_not_found else retry_interval,
                        refresh_credential_fast=refresh_credential_fast,
                        apply_cred_result=apply_cred_result,
                        log_prefix="[ANTIGRAVITY]",
                    )
                    if not switched:
                        return last_error_response
                    continue  # 重试

            except CredentialReplaced as exc:
                await _log_result(exc.status_code, False, error_message=str(exc))
                if retry_config["retry_enabled"] and attempt < max_retries:
                    continue
                return Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                                media_type="application/json")
            except RateLimited as exc:
                if isinstance(exc, QuotaCooling) and last_error_response is not None and last_error_response.status_code in (429, 503):
                    return last_error_response
                return Response(content=json.dumps({"error": str(exc)}), status_code=exc.status_code,
                                headers={"Retry-After": str(exc.retry_after)}, media_type="application/json")
            except Exception as e:
                log.error(f"[ANTIGRAVITY] 非流式请求异常: {e}, 凭证: {current_file}")
                if retry_config["retry_enabled"] and attempt < max_retries:
                    log.info(f"[ANTIGRAVITY] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                    if isinstance(e, TransportError):
                        previously_failed.add(current_file)
                    # 本次上游调用（异常）产生一条日志
                    await _log_result(500, False, error_message=f"异常: {type(e).__name__}: {e}")
                else:
                    # 所有重试都失败，返回最后一次的错误（如果有）或500错误
                    log.error(f"[ANTIGRAVITY] 所有重试均失败，最后异常: {e}")
                    await _log_result(500, False, error_message=f"异常: {type(e).__name__}: {e}")
                    if last_error_response:
                        return last_error_response
                    else:
                        return Response(
                            content=json.dumps({"error": f"非流式请求异常: {str(e)}"}),
                            status_code=500,
                            media_type="application/json"
                        )
            finally:
                if lease:
                    with CancelScope(shield=True):
                        await lease.result()
                        await lease.close()

            # Only the exception-retry path falls through. Release the slot before backoff.
            await asyncio.sleep(retry_interval)

        # 所有重试都失败（for循环正常结束）——每次上游调用均已各自记录日志，
        # 这里不额外记录，直接返回最后一次的原始错误
        log.error("[ANTIGRAVITY] 所有重试均失败")
        if last_error_response:
            return last_error_response
        else:
            return Response(
                content=json.dumps({"error": "所有重试均失败"}),
                status_code=500,
                media_type="application/json"
            )
    finally:
        await _cancel_credential_prefetch(next_cred_task)


# ==================== 模型和配额查询 ====================

# 模型列表白名单家族（名字包含即放行）
_MODEL_FAMILY_WHITELIST = ("claude", "gemini", "gpt", "argon")


def _is_listable_model(model_id: str) -> bool:
    """模型列表过滤：剔除 disabled 模型与白名单家族之外的杂牌模型"""
    lowered = (model_id or "").lower()
    if "disabled" in lowered:
        return False
    return any(family in lowered for family in _MODEL_FAMILY_WHITELIST)


def _make_model_list(model_names) -> List[Dict[str, Any]]:
    models = {name for name in model_names if _is_listable_model(name)}
    if "claude-sonnet-4-6" in models:
        models.add("claude-sonnet-4-6-thinking")
    if "claude-opus-4-6-thinking" in models:
        models.add("claude-opus-4-6")
    timestamp = int(time.time())
    return [
        model_to_dict(Model(id=name, object="model", created=timestamp, owned_by="google"))
        for name in sorted(models)
    ]


async def fetch_available_models() -> List[Dict[str, Any]]:
    """
    汇总启用凭证的模型清单，返回符合 OpenAI API 规范的格式。
    权益后台刷新准备清单；尚无任何快照时，用一个可用凭证实时查询。

    Returns:
        模型列表，格式为字典列表

    Raises:
        返回空列表如果获取失败
    """
    # Known catalogs are filled by the bounded entitlement refresh workers.
    database = await get_database()
    known_models = await database.get_enabled_model_catalog()
    if known_models is not None:
        return _make_model_list(known_models)
    cred_result = await get_valid_credential()
    if not cred_result:
        log.error("[ANTIGRAVITY] No valid credentials available for fetching models")
        return []

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return []

    # 构建请求头
    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        # 使用 POST 请求获取模型列表
        response = await post_async(
            credential_email=current_file, credential_revision=credential_data.get("credential_revision"),
            url=f"{await get_antigravity_api_url()}/v1internal:fetchAvailableModels",
            json={},  # 空的请求体
            headers=headers
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY] Raw models response: {json.dumps(data, ensure_ascii=False)[:500]}")

            model_list = _make_model_list(
                name for name, entry in data.get("models", {}).items()
                if isinstance(entry, dict) and not entry.get("disabled")
            )

            log.info(f"[ANTIGRAVITY] Fetched {len(model_list)} available models")
            return model_list
        else:
            log.error(f"[ANTIGRAVITY] Failed to fetch models ({response.status_code}): {response.text[:500]}")
            return []

    except Exception as e:
        log.error(f"[ANTIGRAVITY] Failed to fetch models: {e}")
        log.error(f"[ANTIGRAVITY] Traceback: {traceback.format_exc()}")
        return []


async def fetch_quota_info(access_token: str, project_id: str = "", credential_email: str = None,
                           credential_revision: str = None, *, account_tier: str = "unknown") -> Dict[str, Any]:
    """
    获取指定凭证的额度信息（使用 retrieveUserQuotaSummary 获取 weekly+5h 分组配额）

    Args:
        access_token: Antigravity 访问令牌
        project_id: 凭证对应的项目 ID

    Returns:
        包含额度信息的字典，格式为：
        {
            "success": True/False,
            "models": {
                "bucket_id": {
                    "remaining": 0.95,
                    "resetTime": "12-20 10:30",
                    "resetTimeRaw": "2025-12-20T02:30:00Z",
                    "resetDays": 5,
                    "window": "weekly",
                    "group": "gemini 3 pro"
                }
            },
            "minResetDays": 5,
            "accountTier": 凭证记录的订阅等级,
            "isPro": 是否为付费等级（未知时为 null）,
            "quotaGroups": [...],
            "source": "retrieveUserQuotaSummary",
            "error": "错误信息" (仅在失败时)
        }
    """

    headers = build_antigravity_headers(access_token, model_name="agent")

    url = f"{await get_antigravity_api_url()}/v1internal:retrieveUserQuotaSummary"
    payload: Dict[str, Any] = {}
    if project_id:
        payload["project"] = project_id

    log.info(f"[ANTIGRAVITY QUOTA] fetch_quota_info: project={project_id}, url={url}")

    try:
        response = await post_async(
            credential_email=credential_email, credential_revision=credential_revision,
            url=url,
            json=payload,
            headers=headers,
            timeout=30.0
        )

        if response.status_code != 200:
            error_text = response.text[:500]
            log.error(f"[ANTIGRAVITY QUOTA] fetch_quota_info 失败: {response.status_code} - {error_text}")
            return {"success": False, "status_code": response.status_code if response.is_error else 502,
                    "retry_after": response.headers.get("Retry-After"),
                    "error": f"API返回错误: {response.status_code} - {error_text}"}

        data = response.json()
        if isinstance(data, dict) and data.get("error"):
            error = data["error"]
            code = error.get("code") if isinstance(error, dict) else None
            message = error.get("message") if isinstance(error, dict) else str(error)
            return {"success": False, "status_code": code if isinstance(code, int) and 400 <= code <= 599 else 502,
                    "retry_after": response.headers.get("Retry-After"), "error": str(message or "上游额度查询失败")[:500]}
        log.debug(f"[ANTIGRAVITY QUOTA] Raw response: {json.dumps(data, ensure_ascii=False)[:500]}")

        # 解析分组配额（weekly + 5h 双窗口）
        quota_groups: List[Dict[str, Any]] = []
        for g in data.get("groups", []):
            buckets = []
            for b in g.get("buckets", []):
                buckets.append({
                    "bucketId": b.get("bucketId", ""),
                    "displayName": b.get("displayName", ""),
                    "window": b.get("window", ""),
                    "resetTime": b.get("resetTime", ""),
                    "remainingFraction": b.get("remainingFraction", 0),
                    "description": b.get("description"),
                })
            quota_groups.append({
                "displayName": g.get("displayName", ""),
                "description": g.get("description"),
                "buckets": buckets,
            })

        min_reset_days = None
        now = datetime.now(timezone.utc)
        beijing = timezone(timedelta(hours=8))

        # 构建配额映射（从分组数据展开，兼容旧格式）
        quota_info: Dict[str, Dict[str, Any]] = {}
        for group in quota_groups:
            group_name = group.get("displayName", "").lower()
            for bucket in group.get("buckets", []):
                bucket_id = bucket.get("bucketId", "")
                remaining = bucket.get("remainingFraction", 0)
                reset_time = bucket.get("resetTime", "")
                window = bucket.get("window", "")

                # 转换为北京时间
                reset_time_beijing = 'N/A'
                reset_days = None
                if reset_time:
                    try:
                        reset_at = datetime.fromisoformat(reset_time.replace('Z', '+00:00'))
                        if reset_at.tzinfo is None:
                            reset_at = reset_at.replace(tzinfo=timezone.utc)
                        reset_time_beijing = reset_at.astimezone(beijing).strftime('%m-%d %H:%M')
                        reset_days = max(0, (reset_at - now).days)
                        if min_reset_days is None or reset_days < min_reset_days:
                            min_reset_days = reset_days
                    except Exception as e:
                        log.warning(f"[ANTIGRAVITY QUOTA] Failed to parse reset time: {e}")

                quota_info[bucket_id] = {
                    "remaining": remaining,
                    "resetTime": reset_time_beijing,
                    "resetTimeRaw": reset_time,
                    "resetDays": reset_days,
                    "window": window,
                    "group": group_name,
                }

        log.info(
            f"[ANTIGRAVITY QUOTA] 解析到 {len(quota_groups)} 个分组, "
            f"{len(quota_info)} 个配额桶, tier={account_tier}"
        )
        return {
            "success": True,
            "models": quota_info,
            "minResetDays": min_reset_days,
            "accountTier": account_tier,
            "isPro": {"ultra": True, "ultra_lite": True, "pro": True, "free": False}.get(account_tier),
            "quotaGroups": quota_groups,
            "source": "retrieveUserQuotaSummary",
        }
    except CredentialReplaced as e:
        return {"success": False, "status_code": e.status_code, "error": str(e)}
    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "error": "上游额度查询超时"}
    except (httpx.RequestError, ValueError, TypeError, AttributeError) as e:
        log.warning(f"[ANTIGRAVITY QUOTA] 额度查询失败: {type(e).__name__}")
        return {"success": False, "status_code": 502, "error": "无法获取有效的上游额度响应"}
