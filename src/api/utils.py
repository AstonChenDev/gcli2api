"""
API 客户端共用基础功能：自动封禁、错误重试、调用结果记录、冷却解析
"""

import json
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

from config import (
    get_auto_ban_enabled,
    get_auto_ban_error_codes,
    get_retry_429_enabled,
    get_retry_429_interval,
    get_retry_429_max_retries,
)
from log import log
from src.credential_manager import record_api_call_result, set_cred_disabled


# ==================== 错误检查与处理 ====================

def is_model_not_found(error_body, *, generated_content=False):
    """Classify a generation 404 before changing credential or retry state."""
    try:
        payload = json.loads(error_body.removeprefix("\ufeff") if isinstance(error_body, str) else error_body)
    except (TypeError, ValueError):
        return not generated_content
    error = payload.get("error") if isinstance(payload, dict) else None
    details = error.get("details") if isinstance(error, dict) else None
    if not isinstance(details, list):
        return not generated_content
    resource_types = [item.get("resourceType") for item in details if isinstance(item, dict)
                      and item.get("@type") == "type.googleapis.com/google.rpc.ResourceInfo"]
    input_types = ("generativelanguage.googleapis.com/File", "generativelanguage.googleapis.com/CachedContent",
                   "storage.googleapis.com/Object")
    if resource_types and all(kind in input_types for kind in resource_types):
        return False
    return not generated_content or "generativelanguage.googleapis.com/Model" in resource_types


async def check_should_auto_ban(status_code: int) -> bool:
    """检查是否应该触发自动封禁"""
    return (
        await get_auto_ban_enabled()
        and status_code in await get_auto_ban_error_codes()
    )


async def handle_auto_ban(status_code: int, email: str, *, expected_revision=None) -> None:
    """处理自动封禁：直接禁用凭证"""
    if email:
        if await set_cred_disabled(email, True, expected_revision=expected_revision):
            log.warning(f"[AUTO_BAN] Status {status_code} triggers auto-ban for credential: {email}")


async def handle_error_with_retry(
    status_code: int,
    email: str,
    retry_enabled: bool,
    attempt: int,
    max_retries: int,
    *, expected_revision=None,
) -> bool:
    """
    统一处理错误和重试逻辑

    仅在以下情况下进行自动重试:
    1. 429错误(速率限制)
    2. 503错误(服务不可用)
    3. 500错误(服务临时不可用)
    4. 导致凭证封禁的错误(AUTO_BAN_ERROR_CODES配置)

    Returns:
        bool: True表示需要继续重试，False表示不需要重试
    """
    # 优先检查自动封禁
    if await check_should_auto_ban(status_code):
        await handle_auto_ban(status_code, email, expected_revision=expected_revision)

        # 自动封禁后，仍然尝试重试（会在下次循环中自动获取新凭证）
        if retry_enabled and attempt < max_retries:
            log.info(
                f"[RETRY] Retrying with next credential after auto-ban "
                f"(status {status_code}, attempt {attempt + 1}/{max_retries})"
            )
            return True
        return False

    # 如果不触发自动封禁，仅对429、503和500错误进行重试
    if status_code in (429, 500, 503) and retry_enabled and attempt < max_retries:
        log.info(
            f"[RETRY] {status_code} error encountered, retrying "
            f"(attempt {attempt + 1}/{max_retries})"
        )
        return True

    return False


# ==================== 重试配置获取 ====================

async def get_retry_config() -> Dict[str, Any]:
    """获取重试配置"""
    return {
        "retry_enabled": await get_retry_429_enabled(),
        "max_retries": await get_retry_429_max_retries(),
        "retry_interval": await get_retry_429_interval(),
    }


# ==================== API调用结果记录 ====================

async def record_api_call_success(
    email: str,
    model_name: Optional[str] = None,
    *, expected_revision=None,
) -> None:
    """记录API调用成功"""
    if email:
        await record_api_call_result(
            email, True, model_name=model_name, expected_revision=expected_revision
        )


async def record_api_call_error(
    email: str,
    status_code: int,
    cooldown_until: Optional[float] = None,
    model_name: Optional[str] = None,
    error_message: Optional[str] = None,
    *, expected_revision=None,
) -> None:
    """记录API调用错误"""
    if email:
        await record_api_call_result(
            email,
            False,
            status_code,
            cooldown_until=cooldown_until,
            model_name=model_name,
            error_message=error_message, expected_revision=expected_revision,
        )


# ==================== 429错误处理 ====================

async def parse_and_log_cooldown(error_text: str, retry_after: Optional[str] = None) -> Optional[float]:
    """Prefer Google quota details and fall back to the HTTP Retry-After header."""
    try:
        # json.loads accepts a BOM on bytes, but rejects it on decoded text.
        if isinstance(error_text, str):
            error_text = error_text.removeprefix("\ufeff")
        error_data = json.loads(error_text)
    except (ValueError, TypeError):
        error_data = None
    deadline = parse_quota_reset_timestamp(error_data)
    if deadline is None and retry_after:
        value = retry_after.strip()
        now = time.time()
        try:
            if re.fullmatch(r"[0-9]+", value):
                deadline = now + int(value)
            else:
                deadline = parsedate_to_datetime(value).timestamp()
            if deadline <= now:
                return None
        except (TypeError, ValueError, OverflowError, OSError):
            return None
    if deadline is not None:
        try:
            reset = datetime.fromtimestamp(deadline, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
        log.info(f"检测到quota冷却时间: {reset.isoformat()}")
    return deadline


def _delay_seconds(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9]+(?:\.[0-9]+)?[smhd])+", value):
        return None
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return sum(float(n) * units[u] for n, u in re.findall(r"([0-9]+(?:\.[0-9]+)?)([smhd])", value))


def parse_quota_reset_timestamp(error_response: dict) -> Optional[float]:
    error = error_response.get("error") if isinstance(error_response, dict) else None
    details = error.get("details") if isinstance(error, dict) else None
    if not isinstance(details, list):
        return None
    now = time.time()
    quota_delays, retry_delays = [], []
    for detail in details:
        if not isinstance(detail, dict):
            continue
        kind = detail.get("@type")
        if kind == "type.googleapis.com/google.rpc.RetryInfo":
            retry_delays.append(detail.get("retryDelay"))
        elif kind == "type.googleapis.com/google.rpc.ErrorInfo":
            metadata = detail.get("metadata")
            if not isinstance(metadata, dict):
                continue
            quota_delays.append(metadata.get("quotaResetDelay"))
            absolute = metadata.get("quotaResetTimeStamp")
            if isinstance(absolute, str):
                try:
                    parsed = datetime.fromisoformat(absolute)
                    deadline = (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()
                    if deadline > now:
                        return deadline
                except (ValueError, OverflowError, OSError):
                    pass
    for value in quota_delays + retry_delays:
        seconds = _delay_seconds(value)
        if seconds and seconds > 0:
            try:
                datetime.fromtimestamp(now + seconds, timezone.utc)
            except (OverflowError, OSError, ValueError):
                continue
            return now + seconds
    return None
