"""
请求体详细日志服务（磁盘 JSON 文件）
- request_log_enabled 控制；可记录全部请求或仅指定状态码
- 文件保存在 data/request_logs/<YYYY-MM-DD>/<毫秒时间戳>_<随机ID>.json
- usage_log 数据库行只保存相对路径索引（request_log_file 字段）
- 前端查看详情时按索引从磁盘读取；清理/清空日志时同步删除对应文件
"""

import asyncio
import importlib
import json
import os
import re
import time
import uuid
from typing import Any, Dict, Optional

from log import log

# 详细日志根目录（相对工作目录）
REQUEST_LOG_DIR = os.path.join("data", "request_logs")

# 记录时需要脱敏的请求头（小写）
_SENSITIVE_HEADERS = {"authorization", "cookie", "set-cookie", "x-goog-api-key", "x-api-key"}

# Keep ownership until the usage queue commits (or abandons) the file's index.
# OS locks also protect files queued in another worker sharing this directory.
_locking = importlib.import_module("msvcrt" if os.name == "nt" else "fcntl")
_active_files = {}
_DETAIL_NAME = re.compile(r"^\d+_[A-Za-z0-9_-]+\.json$")


def _lock_file(handle):
    if os.name == "nt":
        handle.seek(0)
        _locking.locking(handle.fileno(), _locking.LK_NBLCK, 1)
    else:
        _locking.flock(handle.fileno(), _locking.LOCK_EX | _locking.LOCK_NB)


def retain_request_body_log(path):
    full = _resolve_path(path)
    lease = _active_files.get(os.path.abspath(full)) if full else None
    if lease:
        lease["queued"] = True


def release_request_body_log(path):
    full = _resolve_path(path)
    lease = _active_files.get(os.path.abspath(full)) if full else None
    if lease:
        _release_lease(lease)


def _release_lease(lease):
    if _active_files.get(lease["key"]) is lease:
        _active_files.pop(lease["key"])
        lease["owner"].remove_done_callback(lease["callback"])
        lease["handle"].close()


def _release_unqueued(path, lease):
    if lease["queued"]:
        return
    writer = lease.get("writer")
    if writer and not writer.done():
        def finished(task):
            if not task.cancelled():
                task.exception()
            _release_lease(lease)
        writer.add_done_callback(finished)
    else:
        _release_lease(lease)


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

    lease = None
    try:
        os.makedirs(rel_dir, exist_ok=True)
        handle = open(full_path, "x+b")
        try:
            _lock_file(handle)
        except BaseException:
            handle.close()
            raise
        lease = {"handle": handle, "queued": False, "key": os.path.abspath(full_path),
                 "owner": asyncio.current_task()}
        _active_files[os.path.abspath(full_path)] = lease
        lease["callback"] = lambda _: _release_unqueued(rel_path, lease)
        lease["owner"].add_done_callback(lease["callback"])

        def write():
            # Use the locking descriptor itself (required by Windows byte locks).
            handle.write(json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8"))
            handle.flush()

        writer = lease["writer"] = asyncio.create_task(asyncio.to_thread(write))
        # Cancellation cannot release the lock while a worker thread still writes.
        await asyncio.shield(writer)
        return rel_path
    except BaseException as e:
        if lease:
            _release_unqueued(rel_path, lease)
        if not isinstance(e, Exception):
            raise
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


async def delete_orphan_request_body_logs(database, before_timestamp: float) -> int:
    """Reclaim completed, unindexed files; never guess ownership from age alone."""
    deleted = 0
    root = os.path.abspath(REQUEST_LOG_DIR)
    for directory, _, names in os.walk(root, followlinks=False):
        if os.path.islink(directory):
            continue
        for name in names:
            if not _DETAIL_NAME.fullmatch(name):
                continue
            full = os.path.join(directory, name)
            if os.path.islink(full):
                continue
            handle = None
            try:
                handle = open(full, "r+b")
                _lock_file(handle)
                stat = os.fstat(handle.fileno())
                # Check both creation-name time and last write: partial writes
                # and files created during clearing must remain untouched.
                if int(name.split("_", 1)[0]) / 1000 >= before_timestamp or stat.st_mtime >= before_timestamp:
                    continue
                path = os.path.relpath(full, root).replace(os.sep, "/")
                # Read AFTER acquiring ownership; a writer may just have committed.
                if await database.has_request_body_log(path):
                    continue
                if os.name == "nt":
                    # Windows disallows unlinking an open file. No producer can
                    # reuse this UUID path after publication.
                    handle.close()
                    handle = None
                os.remove(full)
                deleted += 1
            except FileNotFoundError:
                pass
            except OSError:
                # Locked by a writer/another cleaner, or not safely accessible.
                pass
            finally:
                if handle:
                    handle.close()
    return deleted
