"""
请求体详细日志服务（磁盘 JSON 文件）
- request_log_enabled 控制；可记录全部请求或仅指定状态码
- 文件保存在 data/request_logs/<YYYY-MM-DD>/<毫秒时间戳>_<随机ID>.json
- usage_log 数据库行只保存相对路径索引（request_log_file 字段）
- 前端查看详情时按索引从磁盘读取；清理/清空日志时同步删除对应文件
"""

import json
import os
import time
import uuid
from typing import Any, Dict, Optional

from log import log

# 详细日志根目录（相对工作目录）
REQUEST_LOG_DIR = os.path.join("data", "request_logs")

# 记录时需要脱敏的请求头（小写）
_SENSITIVE_HEADERS = {"authorization", "cookie", "set-cookie", "x-goog-api-key", "x-api-key"}


def _sanitize_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    safe: Dict[str, str] = {}
    for key, value in (headers or {}).items():
        if str(key).lower() in _SENSITIVE_HEADERS:
            safe[key] = "(已脱敏)"
        else:
            safe[str(key)] = str(value)
    return safe


async def record_request_body(
    *,
    request_id: str,
    model_name: str,
    upstream_model_name: str,
    credential_email: str,
    is_stream: bool,
    status_code: int,
    url: str,
    headers: Optional[Dict[str, str]],
    payload: Any,
    response_id: str = "",
    response_body: str = "",
    error_message: str = "",
    diagnostics: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    写入一条请求体详细日志（磁盘 JSON）。
    返回相对路径索引（写入失败返回 None）。任何异常不影响主流程。
    """
    import aiofiles

    ts = time.time()
    date_dir = time.strftime("%Y-%m-%d", time.localtime(ts))
    rel_dir = os.path.join(REQUEST_LOG_DIR, date_dir)
    file_name = f"{int(ts * 1000)}_{uuid.uuid4().hex}.json"
    # 数据库索引：相对 request_logs 根目录的路径（正斜杠）
    rel_path = f"{date_dir}/{file_name}"
    full_path = os.path.join(rel_dir, file_name)

    entry = {
        "created_at": ts,
        "request_id": request_id,
        "response_id": response_id,
        "model_name": model_name,
        "upstream_model_name": upstream_model_name,
        "credential_email": credential_email,
        "is_stream": is_stream,
        "status_code": status_code,
        "url": url,
        "request_headers": _sanitize_headers(headers),
        "request_body": payload,
        "response_body": response_body,
        "error_message": error_message,
        "diagnostics": diagnostics or {},
    }

    try:
        os.makedirs(rel_dir, exist_ok=True)
        async with aiofiles.open(full_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(entry, ensure_ascii=False, indent=2))
        return rel_path
    except Exception as e:
        log.warning(f"请求体详细日志写入失败（不影响主流程）: {e}")
        return None


def _resolve_path(rel_path: str) -> Optional[str]:
    """把数据库中的相对路径索引解析为磁盘绝对路径（防目录穿越）"""
    if not rel_path:
        return None
    normalized = rel_path.replace("\\", "/")
    root = os.path.normpath(REQUEST_LOG_DIR)
    full = os.path.normpath(os.path.join(root, normalized))
    if full != root and not full.startswith(root + os.sep):
        return None
    return full


def read_request_body_log(rel_path: str) -> Optional[Dict[str, Any]]:
    """按索引读取详细日志文件；不存在或损坏时返回 None（文件可能已被清理或手动删除）"""
    full = _resolve_path(rel_path)
    if not full or not os.path.isfile(full):
        return None
    try:
        with open(full, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        # 文件损坏（可能被手动编辑/删除到一半）也不报错，视为不可读
        return None


def delete_request_body_logs(paths) -> int:
    """仅删除已提交删除的数据库记录所引用的文件，保留并发写入。"""
    deleted = 0
    for path in set(paths):
        full = _resolve_path(path)
        if not full:
            continue
        try:
            os.remove(full)
            deleted += 1
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning(f"删除请求体详细日志失败: {type(exc).__name__}")
    return deleted
