"""
上游限额冷却策略。

Antigravity 的 RESOURCE_EXHAUSTED 策略放在独立模块中，避免把项目定制逻辑
继续写进上游经常修改的通用错误解析函数。以后同步上游时，只需保留调用点即可。
"""

import json
import time
from typing import Any, Optional

from config import get_antigravity_resource_exhausted_cooldown_minutes
from log import log
from src.api.utils import parse_and_log_cooldown

_RESOURCE_EXHAUSTED_STATUS = "RESOURCE_EXHAUSTED"


def _extract_google_error_status(error_text: str) -> Optional[str]:
    """从 Google 风格错误响应中提取状态名；格式异常时返回 None。"""
    if not error_text:
        return None

    try:
        payload: Any = json.loads(error_text)
    except (TypeError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    error_obj = payload.get("error", payload)
    if not isinstance(error_obj, dict):
        return None

    status = error_obj.get("status")
    return status.strip().upper() if isinstance(status, str) else None


def is_antigravity_resource_exhausted(status_code: int, error_text: str) -> bool:
    """
    判断 Antigravity 错误是否应采用可配置的限额冷却策略。

    HTTP 429 本身已经足以表明限额；同时兼容少数使用 503 HTTP 状态、但响应体
    标记为 RESOURCE_EXHAUSTED 的上游返回。即使响应体不是合法 JSON，429 仍会冷却。
    """
    return (
        status_code == 429 or _extract_google_error_status(error_text) == _RESOURCE_EXHAUSTED_STATUS
    )


async def resolve_antigravity_cooldown_until(
    *,
    status_code: int,
    error_text: str,
    now: Optional[float] = None,
) -> Optional[float]:
    """
    计算 Antigravity 错误对应的模型级冷却截止时间。

    - 429 / RESOURCE_EXHAUSTED：完全服从后台固定时长配置；配置为 None 时不冷却。
    - 其他错误：继续使用项目原有的通用解析器，保持既有兼容行为。

    ``now`` 仅用于确定性测试；生产调用不传时使用当前 Unix 时间。
    """
    if not is_antigravity_resource_exhausted(status_code, error_text):
        return await parse_and_log_cooldown(error_text, mode="antigravity")

    cooldown_minutes = await get_antigravity_resource_exhausted_cooldown_minutes()
    if cooldown_minutes is None:
        log.info("[ANTIGRAVITY] RESOURCE_EXHAUSTED 模型冷却已通过配置关闭")
        return None

    current_time = time.time() if now is None else now
    cooldown_until = current_time + cooldown_minutes * 60
    log.info(f"[ANTIGRAVITY] RESOURCE_EXHAUSTED 使用固定冷却配置: {cooldown_minutes:g}分钟")
    return cooldown_until
