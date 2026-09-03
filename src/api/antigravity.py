"""
Antigravity API Client - Handles communication with Google's Antigravity API
处理与 Google Antigravity API 的通信
"""

import asyncio
import copy
import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Callable, Tuple

from fastapi import Response
from config import (
    get_antigravity_api_url,
    get_antigravity_stream2nostream,
    get_auto_ban_error_codes,
)
from log import log

from src.credential_manager import credential_manager
from src.stats_collector import stats_collector
from src.httpx_client import stream_post_async, post_async
from src.models import Model, model_to_dict
from src.utils import ANTIGRAVITY_USER_AGENT

# 导入共同的基础功能
from src.api.utils import (
    handle_error_with_retry,
    get_retry_config,
    record_api_call_success,
    record_api_call_error,
    collect_streaming_response,
)
from src.api.cooldown_policy import resolve_antigravity_cooldown_until
from src.api.capacity_fallback import (
    CapacityFallbackState,
    post_with_capacity_fallback,
    stream_with_capacity_fallback,
)

# ==================== 全局凭证管理器 ====================

def _has_generated_content(data_json: dict) -> bool:
    """检查 response JSON 中是否包含生成的文本内容。"""
    resp = data_json.get("response") if "response" in data_json else data_json
    if not isinstance(resp, dict):
        return False
    candidates = resp.get("candidates")
    if isinstance(candidates, list) and len(candidates) > 0:
        for cand in candidates:
            if not isinstance(cand, dict):
                continue
            content = cand.get("content")
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if isinstance(part, dict) and part.get("text"):
                            return True
    return False


# 使用全局单例 credential_manager，自动初始化


def _extract_first_user_text(request_payload: Dict[str, Any]) -> str:
    contents = request_payload.get("contents", [])
    if not isinstance(contents, list):
        return ""
    for content in contents:
        if not isinstance(content, dict) or content.get("role") != "user":
            continue
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict) and part.get("text"):
                return str(part["text"])
    return ""


def _generate_request_id() -> str:
    """生成完整格式的 requestId，对齐参考实现:
    agent/{uuid}/{毫秒时间戳}/{trajectory_id}/{step}
    """
    trajectory_id = str(uuid.uuid4())
    step = 1
    ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"agent/{uuid.uuid4()}/{ms}/{trajectory_id}/{step}"


def _build_labels(model: str, trajectory_id: str, step: int) -> Dict[str, str]:
    used_claude = "claude" in model.lower()
    return {
        "last_step_index": str(step),
        "model_enum": model,
        "trajectory_id": trajectory_id,
        "used_claude": str(used_claude).lower(),
        "used_claude_conservative": str(used_claude).lower(),
    }


def _should_forward_antigravity_header(header_name: str) -> bool:
    normalized = header_name.strip().lower()
    if not normalized:
        return False
    if normalized.startswith("x-b3-"):
        return True
    return normalized in {
        "accept-language",
        "traceparent",
        "tracestate",
        "x-cloud-trace-context",
        "x-goog-api-client",
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
    enable_credit: bool = False,
) -> Tuple[Dict[str, Any], str]:
    """
    将 Gemini 格式请求包装成 Antigravity CLI 格式。
    返回 (payload, request_id)。
    """
    inner = copy.deepcopy(gemini_request)
    first_user_text = _extract_first_user_text(inner)

    # 移除 safetySettings（CLI 不发送）
    inner.pop("safetySettings", None)

    # 注入 sessionId
    session_id = str(inner.get("sessionId") or "").strip()
    if not session_id:
        if first_user_text:
            digest = hashlib.sha256(first_user_text.encode("utf-8")).digest()
            session_id_val = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
            session_id = f"-{session_id_val}"
        else:
            session_id = f"-{uuid.uuid4().int % 9_000_000_000_000_000_000}"
        inner["sessionId"] = session_id

    # 注入 labels
    inner["labels"] = _build_labels(model, session_id, 1)

    # toolConfig 默认 VALIDATED
    tool_config = inner.get("toolConfig") or {}
    func_config = tool_config.get("functionCallingConfig") or {}
    func_config["mode"] = "VALIDATED"
    tool_config["functionCallingConfig"] = func_config
    inner["toolConfig"] = tool_config

    request_id = _generate_request_id()

    payload = {
        "project": project_id,
        "requestId": request_id,
        "request": inner,
        "model": model,
        "userAgent": "antigravity",
        "requestType": "agent",
    }
    if enable_credit:
        payload["enabledCreditTypes"] = ["GOOGLE_ONE_AI"]
    return payload, request_id


# ==================== 辅助函数 ====================

def build_antigravity_headers(
    access_token: str,
    extra_headers: Optional[Dict[str, str]] = None,
    model_name: str = "",
) -> Dict[str, str]:
    """构建 Antigravity CLI API 请求头。"""
    headers = {
        "User-Agent": ANTIGRAVITY_USER_AGENT,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "requestId": f"req-{uuid.uuid4()}",
    }

    for key, value in _sanitize_antigravity_headers(extra_headers).items():
        headers.setdefault(key, value)

    # 根据模型名称判断 request_type
    if model_name:
        if "image" in model_name.lower():
            headers["requestType"] = "image_gen"
        else:
            headers["requestType"] = "agent"

    return headers


def _is_retryable_status(status_code: int, disable_error_codes: List[int]) -> bool:
    """统一判断是否属于可重试状态码。"""
    return status_code in (429, 503) or status_code in disable_error_codes


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


# ==================== 新的流式和非流式请求函数 ====================

async def stream_request(
    body: Dict[str, Any],
    native: bool = False,
    headers: Optional[Dict[str, str]] = None,
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

    # 1. 获取有效凭证
    cred_result = await credential_manager.get_valid_credential(
        mode="antigravity", model_name=model_name
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY STREAM] 当前无可用凭证")
        stats_collector.record_request(model_name, "antigravity", False)
        stats_collector.record_error_code(model_name, "antigravity", 0)
        yield Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )
        return

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY STREAM] No access token in credential: {current_file}")
        stats_collector.record_request(model_name, "antigravity", False)
        stats_collector.record_error_code(model_name, "antigravity", 0)
        yield Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )
        return

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:streamGenerateContent?alt=sse"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, enable_credit)

    # 3. 调用stream_post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务
    capacity_fallback_state = CapacityFallbackState()

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, access_token, auth_headers, project_id, final_payload
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity", model_name=model_name
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    for attempt in range(max_retries + 1):
        success_recorded = False  # 标记是否已记录成功
        need_retry = False  # 标记是否需要重试

        sse_data_count = 0
        has_any_text = False
        is_empty_candidate = False
        empty_candidate_json = None

        try:
            async for chunk in stream_with_capacity_fallback(
                url=target_url,
                body=final_payload,
                model_name=model_name,
                state=capacity_fallback_state,
                native=native,
                headers=auth_headers,
                sender=stream_post_async,
            ):
                # 判断是否是Response对象
                if isinstance(chunk, Response):
                    status_code = chunk.status_code
                    last_error_response = chunk  # 记录最后一次错误

                    try:
                        error_body = chunk.body.decode('utf-8') if isinstance(chunk.body, bytes) else str(chunk.body)
                    except Exception:
                        error_body = ""

                    # 如果错误码是429、503或者在禁用码当中，做好记录后进行重试
                    if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                        log.warning(f"[ANTIGRAVITY STREAM] 流式请求失败 (status={status_code}), 模型: {model_name}, 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")

                        # 解析冷却时间。429 即使没有合法 JSON 响应体，也按后台配置处理。
                        cooldown_until = None
                        if status_code in (429, 503):
                            try:
                                cooldown_until = await resolve_antigravity_cooldown_until(
                                    status_code=status_code,
                                    error_text=error_body,
                                )
                            except Exception as cooldown_error:
                                log.warning(
                                    f"[ANTIGRAVITY STREAM] 计算冷却时间失败: {cooldown_error}"
                                )

                        # 先持久化冷却，再预热下一个凭证，避免并发任务重新选中刚刚429的凭证。
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            cooldown_until, mode="antigravity", model_name=model_name,
                            error_message=error_body
                        )
                        stats_collector.record_error_code(model_name, "antigravity", status_code, error_body)

                        if next_cred_task is None and attempt < max_retries:
                            next_cred_task = asyncio.create_task(
                                credential_manager.get_valid_credential(
                                    mode="antigravity", model_name=model_name
                                )
                            )

                        # 检查是否应该重试
                        should_retry = await handle_error_with_retry(
                            credential_manager, status_code, current_file,
                            retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                            mode="antigravity"
                        )

                        if should_retry and attempt < max_retries:
                            need_retry = True
                            break  # 跳出内层循环，准备重试
                        else:
                            # 不重试，直接返回原始错误
                            log.error(f"[ANTIGRAVITY STREAM] 达到最大重试次数或不应重试，返回原始错误")
                            stats_collector.record_request(model_name, "antigravity", False)
                            stats_collector.record_error_code(model_name, "antigravity", status_code, error_body)
                            yield chunk
                            return
                    else:
                        # 错误码不在禁用码当中，直接返回，无需重试
                        log.error(f"[ANTIGRAVITY STREAM] 流式请求失败，非重试错误码 (status={status_code}), 模型: {model_name}, 凭证: {current_file}, 响应: {error_body[:500] if error_body else '无'}")
                        await record_api_call_error(
                            credential_manager, current_file, status_code,
                            None, mode="antigravity", model_name=model_name,
                            error_message=error_body
                        )
                        stats_collector.record_request(model_name, "antigravity", False)
                        stats_collector.record_error_code(model_name, "antigravity", status_code, error_body)
                        yield chunk
                        return
                else:
                    # 不是Response，说明是真流，直接yield返回
                    # 只在第一个chunk时记录成功
                    if not success_recorded:
                        await record_api_call_success(
                            credential_manager, current_file, mode="antigravity", model_name=model_name
                        )
                        success_recorded = True
                        cred_label = credential_data.get('client_email') or current_file
                        log.info(f"[ANTIGRAVITY STREAM] 开始接收流式响应，模型: {model_name}, 凭证: {cred_label}")
                        stats_collector.record_request(model_name, "antigravity", True)

                    # 检测特殊的0输出token响应并收集数据
                    try:
                        chunk_str = chunk.decode('utf-8', errors='ignore') if isinstance(chunk, bytes) else str(chunk)
                        for line in chunk_str.split("\n"):
                            line = line.strip()
                            if line.startswith("data: "):
                                data_str = line[6:].strip()
                                if data_str and data_str != "[DONE]":
                                    sse_data_count += 1
                                    try:
                                        data_json = json.loads(data_str)
                                        if _has_generated_content(data_json):
                                            has_any_text = True
                                        
                                        usage_meta = None
                                        if "response" in data_json and isinstance(data_json["response"], dict):
                                            usage_meta = data_json["response"].get("usageMetadata")
                                        elif "usageMetadata" in data_json:
                                            usage_meta = data_json["usageMetadata"]

                                        if isinstance(usage_meta, dict):
                                            p_tokens = usage_meta.get("promptTokenCount")
                                            t_tokens = usage_meta.get("totalTokenCount")
                                            if p_tokens is not None and t_tokens is not None and p_tokens == t_tokens and t_tokens > 0:
                                                is_empty_candidate = True
                                                empty_candidate_json = data_json
                                    except Exception:
                                        pass
                    except Exception as e:
                        log.debug(f"Special logger parse error: {e}")

                    yield chunk

            # 流式请求完成，检查结果
            if success_recorded:
                cred_label = credential_data.get('client_email') or current_file
                log.info(f"[ANTIGRAVITY STREAM] 流式响应完成，模型: {model_name}, 凭证: {cred_label}")
                
                # 检测特殊的0输出token响应并且该请求只返回了一个有效的 JSON 流且不包含任何生成文本
                if sse_data_count == 1 and is_empty_candidate and not has_any_text:
                    log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 检测到目标空响应！")
                    log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 对应请求体 Request Body: {json.dumps(final_payload, ensure_ascii=False)}")
                    log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 原始响应 Response: {json.dumps(empty_candidate_json, ensure_ascii=False)}")

                # record_request 已在首个成功chunk处记录
                return
            elif not need_retry:
                # 没有收到任何数据（空回复），需要重试
                log.warning(f"[ANTIGRAVITY STREAM] 收到空回复，无任何内容，凭证: {current_file}")
                await record_api_call_error(
                    credential_manager, current_file, 200,
                    None, mode="antigravity", model_name=model_name,
                    error_message="Empty response from API"
                )
                
                if attempt < max_retries:
                    need_retry = True
                else:
                    log.error(f"[ANTIGRAVITY STREAM] 空回复达到最大重试次数")
                    stats_collector.record_request(model_name, "antigravity", False)
                    stats_collector.record_error_code(model_name, "antigravity", 0)
                    yield Response(
                        content=json.dumps({"error": "服务返回空回复"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    return
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY STREAM] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY STREAM]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY STREAM] 重试时无可用凭证或令牌")
                    yield Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                    stats_collector.record_request(model_name, "antigravity", False)
                    stats_collector.record_error_code(model_name, "antigravity", 0)
                    return
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY STREAM] 流式请求异常: {e}, 凭证: {current_file}")
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY STREAM] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）
                log.error(f"[ANTIGRAVITY STREAM] 所有重试均失败，最后异常: {e}")
                if last_error_response:
                    yield last_error_response
                else:
                    # 如果没有记录到错误响应，返回500错误
                    yield Response(
                        content=json.dumps({"error": f"流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )
                stats_collector.record_request(model_name, "antigravity", False)
                stats_collector.record_error_code(model_name, "antigravity", status_code, error_body if 'error_body' in dir() else None)
                return

    # 所有重试均已耗尽（for循环正常结束），返回最后记录的错误
    log.error("[ANTIGRAVITY STREAM] 所有重试均失败")
    stats_collector.record_request(model_name, "antigravity", False)
    stats_collector.record_error_code(model_name, "antigravity", status_code, error_body if 'error_body' in dir() else None)
    if last_error_response:
        yield last_error_response
    else:
        yield Response(
            content=json.dumps({"error": "请求失败，所有重试均已耗尽"}),
            status_code=429,
            media_type="application/json"
        )


async def non_stream_request(
    body: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Response:
    """
    非流式请求函数

    Args:
        body: 请求体
        headers: 额外的请求头

    Returns:
        Response对象
    """
    # 检查是否启用流式收集模式
    if await get_antigravity_stream2nostream():
        log.debug("[ANTIGRAVITY] 使用流式收集模式实现非流式请求")

        # 调用stream_request获取流
        stream = stream_request(body=body, native=False, headers=headers)

        # 收集流式响应
        # stream_request是一个异步生成器，可能yield Response（错误）或流数据
        # collect_streaming_response会自动处理这两种情况
        return await collect_streaming_response(stream)

    # 否则使用传统非流式模式
    log.debug("[ANTIGRAVITY] 使用传统非流式模式")

    model_name = body.get("model", "")

    # 1. 获取有效凭证
    cred_result = await credential_manager.get_valid_credential(
        mode="antigravity", model_name=model_name
    )

    if not cred_result:
        # 如果返回值是None，直接返回错误500
        log.error("[ANTIGRAVITY] 当前无可用凭证")
        stats_collector.record_request(model_name, "antigravity", False)
        stats_collector.record_error_code(model_name, "antigravity", 0)
        return Response(
            content=json.dumps({"error": "当前无可用凭证"}),
            status_code=500,
            media_type="application/json"
        )

    current_file, credential_data = cred_result
    access_token = credential_data.get("access_token") or credential_data.get("token")
    project_id = credential_data.get("project_id", "")
    enable_credit = bool(credential_data.get("enable_credit", False))

    if not access_token:
        log.error(f"[ANTIGRAVITY] No access token in credential: {current_file}")
        return Response(
            content=json.dumps({"error": "凭证中没有访问令牌"}),
            status_code=500,
            media_type="application/json"
        )

    # 2. 构建URL和请求头
    antigravity_url = await get_antigravity_api_url()
    target_url = f"{antigravity_url}/v1internal:generateContent"

    auth_headers = build_antigravity_headers(access_token, headers, model_name)

    # 构建 CLI 格式请求体
    inner_request = body.get("request", body)
    final_payload, _ = await wrap_cli_request(inner_request, model_name, project_id, enable_credit)

    # 3. 调用post_async进行请求
    retry_config = await get_retry_config()
    max_retries = retry_config["max_retries"]
    retry_interval = retry_config["retry_interval"]

    DISABLE_ERROR_CODES = await get_auto_ban_error_codes()  # 禁用凭证的错误码
    last_error_response = None  # 记录最后一次的错误响应
    next_cred_task = None  # 预热的下一个凭证任务
    capacity_fallback_state = CapacityFallbackState()

    # 内部函数：快速更新凭证(只更新token和project_id,避免重建整个请求)
    async def refresh_credential_fast():
        nonlocal current_file, access_token, auth_headers, project_id, final_payload
        cred_result = await credential_manager.get_valid_credential(
            mode="antigravity", model_name=model_name
        )
        if not cred_result:
            return None
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token:
            return None
        # 只更新token和project_id,不重建整个headers和payload
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    def apply_cred_result(cred_result: Tuple[str, Dict[str, Any]]) -> bool:
        nonlocal current_file, access_token, project_id, auth_headers, final_payload
        current_file, credential_data = cred_result
        access_token = credential_data.get("access_token") or credential_data.get("token")
        project_id = credential_data.get("project_id", "")
        if not access_token or not project_id:
            return False
        auth_headers["Authorization"] = f"Bearer {access_token}"
        final_payload["project"] = project_id
        return True

    for attempt in range(max_retries + 1):
        need_retry = False  # 标记是否需要重试
        
        try:
            response = await post_with_capacity_fallback(
                url=target_url,
                json_body=final_payload,
                model_name=model_name,
                state=capacity_fallback_state,
                headers=auth_headers,
                sender=post_async,
            )

            status_code = response.status_code
            # 检测特殊的0输出token响应并打印
            try:
                data_json = json.loads(response.text)
                usage_meta = None
                if "response" in data_json and isinstance(data_json["response"], dict):
                    usage_meta = data_json["response"].get("usageMetadata")
                elif "usageMetadata" in data_json:
                    usage_meta = data_json["usageMetadata"]

                if isinstance(usage_meta, dict):
                    p_tokens = usage_meta.get("promptTokenCount")
                    t_tokens = usage_meta.get("totalTokenCount")
                    if p_tokens is not None and t_tokens is not None and p_tokens == t_tokens and t_tokens > 0:
                        if not _has_generated_content(data_json):
                            log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 检测到目标空响应（非流式）！")
                            log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 对应请求体 Request Body: {json.dumps(final_payload, ensure_ascii=False)}")
                            log.info(f"[ANTIGRAVITY SPECIAL LOGGER] 原始响应 Response: {response.text}")
            except Exception as e:
                log.debug(f"Special logger non-stream parse error: {e}")

            # 成功
            if status_code == 200:
                # 检查是否为空回复
                if not response.content or len(response.content) == 0:
                    log.warning(f"[ANTIGRAVITY] 收到200响应但内容为空，凭证: {current_file}")
                    
                    # 记录错误
                    await record_api_call_error(
                        credential_manager, current_file, 200,
                        None, mode="antigravity", model_name=model_name,
                        error_message="Empty response from API"
                    )
                    
                    if attempt < max_retries:
                        need_retry = True
                    else:
                        log.error(f"[ANTIGRAVITY] 空回复达到最大重试次数")
                        stats_collector.record_request(model_name, "antigravity", False)
                        stats_collector.record_error_code(model_name, "antigravity", 0)
                        return Response(
                            content=json.dumps({"error": "服务返回空回复"}),
                            status_code=500,
                            media_type="application/json"
                        )
                else:
                    # 正常响应
                    await record_api_call_success(
                        credential_manager, current_file, mode="antigravity", model_name=model_name
                    )
                    stats_collector.record_request(model_name, "antigravity", True)
                    return Response(
                        content=response.content,
                        status_code=200,
                        headers=dict(response.headers)
                    )

            # 失败 - 记录最后一次错误
            if status_code != 200:
                last_error_response = Response(
                    content=response.content,
                    status_code=status_code,
                    headers=dict(response.headers)
                )

                # 判断是否需要重试
                # 缓存错误文本,避免重复解析
                error_text = ""
                try:
                    error_text = response.text
                except Exception:
                    pass

                if _is_retryable_status(status_code, DISABLE_ERROR_CODES):
                    log.warning(f"[ANTIGRAVITY] 非流式请求失败 (status={status_code}), 模型: {model_name}, 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")

                    # 解析冷却时间。429 即使没有合法 JSON 响应体，也按后台配置处理。
                    cooldown_until = None
                    if status_code in (429, 503):
                        try:
                            cooldown_until = await resolve_antigravity_cooldown_until(
                                status_code=status_code,
                                error_text=error_text,
                            )
                        except Exception as cooldown_error:
                            log.warning(
                                f"[ANTIGRAVITY] 计算冷却时间失败: {cooldown_error}"
                            )

                    # 先持久化冷却，再预热下一个凭证，避免并发任务重新选中刚刚429的凭证。
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        cooldown_until, mode="antigravity", model_name=model_name,
                        error_message=error_text
                    )
                    stats_collector.record_error_code(model_name, "antigravity", status_code, error_text)

                    if next_cred_task is None and attempt < max_retries:
                        next_cred_task = asyncio.create_task(
                            credential_manager.get_valid_credential(
                                mode="antigravity", model_name=model_name
                            )
                        )

                    # 检查是否应该重试
                    should_retry = await handle_error_with_retry(
                        credential_manager, status_code, current_file,
                        retry_config["retry_enabled"], attempt, max_retries, retry_interval,
                        mode="antigravity"
                    )

                    if should_retry and attempt < max_retries:
                        need_retry = True
                    else:
                        # 不重试，直接返回原始错误
                        log.error(f"[ANTIGRAVITY] 达到最大重试次数或不应重试，返回原始错误")
                        stats_collector.record_request(model_name, "antigravity", False)
                        stats_collector.record_error_code(model_name, "antigravity", status_code, error_text)
                        return last_error_response
                else:
                    # 错误码不在禁用码当中，直接返回，无需重试
                    log.error(f"[ANTIGRAVITY] 非流式请求失败，非重试错误码 (status={status_code}), 模型: {model_name}, 凭证: {current_file}, 响应: {error_text[:500] if error_text else '无'}")
                    await record_api_call_error(
                        credential_manager, current_file, status_code,
                        None, mode="antigravity", model_name=model_name,
                        error_message=error_text
                    )
                    stats_collector.record_request(model_name, "antigravity", False)
                    stats_collector.record_error_code(model_name, "antigravity", status_code, error_text)
                    return last_error_response
            
            # 统一处理重试
            if need_retry:
                log.info(f"[ANTIGRAVITY] 重试请求 (attempt {attempt + 2}/{max_retries + 1})...")

                switched, next_cred_task = await _switch_credential_for_retry(
                    next_cred_task=next_cred_task,
                    retry_interval=retry_interval,
                    refresh_credential_fast=refresh_credential_fast,
                    apply_cred_result=apply_cred_result,
                    log_prefix="[ANTIGRAVITY]",
                )
                if not switched:
                    log.error("[ANTIGRAVITY] 重试时无可用凭证或令牌")
                    stats_collector.record_request(model_name, "antigravity", False)
                    stats_collector.record_error_code(model_name, "antigravity", 0)
                    return Response(
                        content=json.dumps({"error": "当前无可用凭证"}),
                        status_code=500,
                        media_type="application/json"
                    )
                continue  # 重试

        except Exception as e:
            log.error(f"[ANTIGRAVITY] 非流式请求异常: {e}, 凭证: {current_file}")
            if attempt < max_retries:
                log.info(f"[ANTIGRAVITY] 异常后重试 (attempt {attempt + 2}/{max_retries + 1})...")
                await asyncio.sleep(retry_interval)
                continue
            else:
                # 所有重试都失败，返回最后一次的错误（如果有）或500错误
                log.error(f"[ANTIGRAVITY] 所有重试均失败，最后异常: {e}")
                stats_collector.record_request(model_name, "antigravity", False)
                stats_collector.record_error_code(model_name, "antigravity", 0)
                if last_error_response:
                    return last_error_response
                else:
                    return Response(
                        content=json.dumps({"error": f"非流式请求异常: {str(e)}"}),
                        status_code=500,
                        media_type="application/json"
                    )

    # 所有重试都失败，返回最后一次的原始错误（如果有）或500错误
    log.error("[ANTIGRAVITY] 所有重试均失败")
    stats_collector.record_request(model_name, "antigravity", False)
    stats_collector.record_error_code(model_name, "antigravity", status_code, error_text if 'error_text' in dir() else None)
    if last_error_response:
        return last_error_response
    else:
        return Response(
            content=json.dumps({"error": "所有重试均失败"}),
            status_code=500,
            media_type="application/json"
        )


# ==================== 模型和配额查询 ====================

async def fetch_available_models() -> List[Dict[str, Any]]:
    """
    获取可用模型列表，返回符合 OpenAI API 规范的格式
    
    Returns:
        模型列表，格式为字典列表（用于兼容现有代码）
        
    Raises:
        返回空列表如果获取失败
    """
    # 获取凭证管理器和可用凭证
    cred_result = await credential_manager.get_valid_credential(mode="antigravity")
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
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},  # 空的请求体
            headers=headers
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY] Raw models response: {json.dumps(data, ensure_ascii=False)[:500]}")

            # 转换为 OpenAI 格式的模型列表，使用 Model 类
            model_list = []
            current_timestamp = int(datetime.now(timezone.utc).timestamp())

            if 'models' in data and isinstance(data['models'], dict):
                # 遍历模型字典
                for model_id in data['models'].keys():
                    model = Model(
                        id=model_id,
                        object='model',
                        created=current_timestamp,
                        owned_by='google'
                    )
                    model_list.append(model_to_dict(model))
            # 添加额外的 claude-sonnet-4-6-thinking 模型
            if "claude-sonnet-4-6" in data.get('models', {}):
                model = Model(
                    id='claude-sonnet-4-6-thinking',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(model))
            # 添加额外的 claude-opus-4-6 模型
            if "claude-opus-4-6-thinking" in data.get('models', {}):
                claude_opus_model = Model(
                    id='claude-opus-4-6',
                    object='model',
                    created=current_timestamp,
                    owned_by='google'
                )
                model_list.append(model_to_dict(claude_opus_model))

            log.info(f"[ANTIGRAVITY] Fetched {len(model_list)} available models")
            return model_list
        else:
            log.error(f"[ANTIGRAVITY] Failed to fetch models ({response.status_code}): {response.text[:500]}")
            return []

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY] Failed to fetch models: {e}")
        log.error(f"[ANTIGRAVITY] Traceback: {traceback.format_exc()}")
        return []


async def fetch_quota_info(access_token: str) -> Dict[str, Any]:
    """
    获取指定凭证的额度信息
    
    Args:
        access_token: Antigravity 访问令牌
        
    Returns:
        包含额度信息的字典，格式为：
        {
            "success": True/False,
            "models": {
                "model_name": {
                    "remaining": 0.95,
                    "resetTime": "12-20 10:30",
                    "resetTimeRaw": "2025-12-20T02:30:00Z"
                }
            },
            "error": "错误信息" (仅在失败时)
        }
    """

    headers = build_antigravity_headers(access_token, model_name="agent")

    try:
        antigravity_url = await get_antigravity_api_url()

        response = await post_async(
            url=f"{antigravity_url}/v1internal:fetchAvailableModels",
            json={},
            headers=headers,
            timeout=30.0
        )

        if response.status_code == 200:
            data = response.json()
            log.debug(f"[ANTIGRAVITY QUOTA] Raw response: {json.dumps(data, ensure_ascii=False)[:500]}")

            quota_info = {}

            if 'models' in data and isinstance(data['models'], dict):
                for model_id, model_data in data['models'].items():
                    if isinstance(model_data, dict) and 'quotaInfo' in model_data:
                        quota = model_data['quotaInfo']
                        remaining = quota.get('remainingFraction', 0)
                        reset_time_raw = quota.get('resetTime', '')

                        # 转换为北京时间
                        reset_time_beijing = 'N/A'
                        if reset_time_raw:
                            try:
                                utc_date = datetime.fromisoformat(reset_time_raw.replace('Z', '+00:00'))
                                # 转换为北京时间 (UTC+8)
                                from datetime import timedelta
                                beijing_date = utc_date + timedelta(hours=8)
                                reset_time_beijing = beijing_date.strftime('%m-%d %H:%M')
                            except Exception as e:
                                log.warning(f"[ANTIGRAVITY QUOTA] Failed to parse reset time: {e}")

                        quota_info[model_id] = {
                            "remaining": remaining,
                            "resetTime": reset_time_beijing,
                            "resetTimeRaw": reset_time_raw
                        }

            return {
                "success": True,
                "models": quota_info
            }
        else:
            log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota ({response.status_code}): {response.text[:500]}")
            return {
                "success": False,
                "error": f"API返回错误: {response.status_code}"
            }

    except Exception as e:
        import traceback
        log.error(f"[ANTIGRAVITY QUOTA] Failed to fetch quota: {e}")
        log.error(f"[ANTIGRAVITY QUOTA] Traceback: {traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e)
        }
