"""
凭证管理路由模块 - 处理 /creds/* 相关的HTTP请求（仅 Antigravity 模式）
凭证以 email 为唯一键；导入/导出使用规范 JSON 格式：
  { "refresh_token": "...", "email": "...", "tier": "...", "project_id": "..." }
"""

import asyncio
import io
import json
import time
import zipfile

import httpx
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Response, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from log import log
from src.credential_manager import (
    add_credential,
    remove_credential,
    set_cred_disabled,
)
from src.models import (
    CredFileActionRequest,
    CredFileBatchActionRequest,
    CredentialSelectionRequest,
)
from src.antigravity_import import (
    AntigravityImportInputError,
    MAX_IMPORT_BODY_BYTES,
    import_antigravity_refresh_tokens,
    parse_import_request_payload,
)
from src.storage.database import get_database
from src.utils import (
    verify_panel_token,
    ANTIGRAVITY_USER_AGENT,
    ANTIGRAVITY_CLIENT_ID,
    ANTIGRAVITY_CLIENT_SECRET,
)
from src.api.antigravity import fetch_quota_info, build_antigravity_headers
from src.google_oauth_api import (
    Credentials,
    fetch_project_id_and_tier,
    get_user_email,
)
from config import get_antigravity_api_url


# 创建路由器
router = APIRouter(prefix="/creds", tags=["credentials"])


class CredentialNoteRequest(BaseModel):
    note: str = Field(max_length=1000, strict=True)


@router.patch("/note/{email}")
async def update_credential_note(email: str, request: CredentialNoteRequest, token: str = Depends(verify_panel_token)):
    database = await get_database()
    note = request.note.strip()
    if not await database.update_credential_state(email, {"note": note}):
        raise HTTPException(404, "凭证不存在")
    return {"email": email, "note": note}


# =============================================================================
# 凭证导入核心
# =============================================================================

async def _import_single_credential(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    导入单个凭证（规范格式）。
    必须字段: refresh_token
    可选字段: email / tier / project_id（缺失时自动获取）

    Returns:
        {"status": "success"|"error", "email": ..., "message": ...}
    """
    try:
        refresh_token = (data.get("refresh_token") or "").strip()
        if not refresh_token:
            return {"status": "error", "message": "缺少 refresh_token"}

        email = (data.get("email") or "").strip().lower() or None
        tier = (data.get("tier") or "").strip().lower() or None
        project_id = (data.get("project_id") or "").strip() or None

        # 用 refresh_token 换 access_token，同时验证凭证有效性
        creds = Credentials(
            access_token="",
            refresh_token=refresh_token,
            client_id=ANTIGRAVITY_CLIENT_ID,
            client_secret=ANTIGRAVITY_CLIENT_SECRET,
        )
        for attempt in range(2):
            try:
                await asyncio.wait_for(creds.refresh(), timeout=IMPORT_REFRESH_TIMEOUT)
                break
            except Exception as exc:
                # Credentials.refresh wraps transport errors in TokenError; keep the cause.
                network_error = isinstance(exc, (TimeoutError, httpx.RequestError)) or isinstance(exc.__context__, httpx.RequestError)
                status = getattr(exc, "status_code", None)
                retryable = network_error or status in (429, 500, 502, 503, 504)
                if retryable and attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                if network_error:
                    return {"status": "error", "code": "network_error", "message": "连接 Google 验证服务超时或网络异常（已重试），请检查代理后重试；不代表凭证无效"}
                return {"status": "error", "code": "oauth_rejected", "message": f"Google 令牌验证失败（HTTP {status}）" if status else "令牌验证失败，请检查凭证格式后重试"}

        if not creds.access_token:
            return {"status": "error", "message": "refresh_token 无效: 未获取到 access_token"}

        # 自动获取缺失的 email
        if not email:
            try:
                email = await asyncio.wait_for(get_user_email(creds), timeout=15.0)
            except Exception as e:
                log.warning(f"自动获取邮箱失败: {type(e).__name__}")
        if not email:
            return {"status": "error", "message": "无法获取邮箱（JSON 中未提供且自动获取失败）"}

        # 自动获取缺失的 project_id / tier
        if not project_id or not tier:
            try:
                api_base_url = await get_antigravity_api_url()
                fetched_project, fetched_tier = await asyncio.wait_for(fetch_project_id_and_tier(
                    access_token=creds.access_token,
                    user_agent=ANTIGRAVITY_USER_AGENT,
                    api_base_url=api_base_url,
                ), timeout=20.0)
                if not project_id:
                    project_id = fetched_project
                if not tier:
                    tier = fetched_tier
            except Exception as e:
                log.warning(f"自动获取 project_id/tier 失败: {e}")

        # project_id 获取失败时回退为 aicode-consumers，避免空值入库
        if not project_id:
            log.warning(f"导入凭证 {email}: project_id 获取失败，使用回退值 aicode-consumers")
            project_id = "aicode-consumers"

        credential_data = {
            "access_token": creds.access_token,
            "refresh_token": refresh_token,
            "client_id": ANTIGRAVITY_CLIENT_ID,
            "client_secret": ANTIGRAVITY_CLIENT_SECRET,
            "project_id": project_id,
            "expiry": creds.expires_at.isoformat() if creds.expires_at else "",
            "tier": tier,
        }

        if "note" in data:
            credential_data["note"] = data["note"].strip()
        await add_credential(email, credential_data)
        return {
            "status": "success",
            "email": email,
            "tier": tier,
            "message": "导入成功",
        }

    except Exception as e:
        return {"status": "error", "message": f"凭证处理失败（{type(e).__name__}），请稍后重试"}


# =============================================================================
# 工具函数
# =============================================================================


# File imports use a separate short validation budget, not the model POST timeout.
IMPORT_CONCURRENCY = 3
IMPORT_REFRESH_TIMEOUT = 20.0
IMPORT_ITEM_TIMEOUT = 90.0
IMPORT_HEARTBEAT_SECONDS = 10.0
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_EXPANDED_BYTES = 64 * 1024 * 1024
MAX_IMPORT_FILES = 1000
_import_slots = asyncio.Semaphore(IMPORT_CONCURRENCY)


def _extract_zip_content(content: bytes) -> List[dict]:
    """Decode off the event loop; preserve invalid entries as visible failures."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = [entry for entry in archive.infolist()
                       if not entry.is_dir() and entry.filename.lower().endswith(".json")
                       and not entry.filename.startswith("__MACOSX/")]
            if not entries:
                raise HTTPException(400, "ZIP 文件中没有找到 JSON 文件")
            if len(entries) > MAX_IMPORT_FILES or sum(entry.file_size for entry in entries) > MAX_EXPANDED_BYTES:
                raise HTTPException(400, "ZIP 最多包含 1000 个 JSON，解压后总大小不能超过 64 MB")
            result = []
            for entry in entries:
                try:
                    value = archive.read(entry).decode("utf-8-sig")
                    result.append({"filename": entry.filename, "content": value})
                except Exception:
                    result.append({"filename": entry.filename, "error": "文件无法读取：请检查 ZIP 完整性、加密状态和 UTF-8 编码"})
            return result
    except zipfile.BadZipFile:
        raise HTTPException(400, "无效或损坏的 ZIP 文件")


async def extract_json_files_from_zip(zip_file: UploadFile) -> List[dict]:
    content = await zip_file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(400, "单次上传不能超过 20 MB")
    return await asyncio.to_thread(_extract_zip_content, content)


async def _read_credential_files(files: List[UploadFile]) -> List[dict]:
    if not files or len(files) > 100:
        raise HTTPException(400, "请选择 1 至 100 个 JSON 或 ZIP 文件")
    entries = []
    total_bytes = expanded_bytes = 0
    for file in files:
        content = await file.read(MAX_UPLOAD_BYTES + 1)
        total_bytes += len(content)
        if total_bytes > MAX_UPLOAD_BYTES:
            raise HTTPException(400, "单次上传不能超过 20 MB")
        filename = file.filename or "unknown"
        if filename.lower().endswith(".zip"):
            extracted = await asyncio.to_thread(_extract_zip_content, content)
            for entry in extracted:
                entry["filename"] = f"{filename}/{entry['filename']}"
            entries.extend(extracted)
            expanded_bytes += sum(len(entry.get("content", "").encode("utf-8")) for entry in extracted)
        elif filename.lower().endswith(".json"):
            try:
                entries.append({"filename": filename, "content": content.decode("utf-8-sig")})
            except UnicodeDecodeError:
                entries.append({"filename": filename, "error": "JSON 文件必须使用 UTF-8 编码"})
            expanded_bytes += len(content)
        else:
            raise HTTPException(400, "仅支持 JSON 和 ZIP 文件")
        if len(entries) > MAX_IMPORT_FILES or expanded_bytes > MAX_EXPANDED_BYTES:
            raise HTTPException(400, "单次最多导入 1000 个凭证，JSON 总大小不能超过 64 MB")
    return entries


async def _process_credential_file(entry: dict) -> dict:
    if entry.get("error"):
        return {"status": "error", "message": entry["error"]}
    try:
        data = json.loads(entry["content"])
    except RecursionError:
        return {"status": "error", "message": "JSON 嵌套层级过深，请检查凭证文件内容"}
    except (ValueError, TypeError):
        return {"status": "error", "message": "JSON 格式错误"}
    if not isinstance(data, dict):
        return {"status": "error", "message": "JSON 顶层必须是对象"}
    for key in ("refresh_token", "email", "tier", "project_id", "note"):
        if data.get(key) is not None and not isinstance(data[key], str):
            return {"status": "error", "message": f"{key} 必须是字符串"}
    if "note" in data and (not isinstance(data["note"], str) or len(data["note"]) > 1000):
        return {"status": "error", "message": "备注必须是最多 1000 字的字符串"}
    # Include queueing outside the per-item timeout so a large valid ZIP is not discarded.
    async with _import_slots:
        try:
            async with asyncio.timeout(IMPORT_ITEM_TIMEOUT):
                return await _import_single_credential(data)
        except TimeoutError:
            return {"status": "error", "code": "import_timeout", "message": "单个凭证验证超过 90 秒，已跳过；其他凭证继续导入，可稍后重试此项"}
        except Exception as exc:
            return {"status": "error", "message": f"导入失败（{type(exc).__name__}），请稍后重试"}


async def _credential_import_events(entries: List[dict]):
    """Bounded workers report each committed item; disconnect cancels unfinished work."""
    total = len(entries)
    queue = asyncio.Queue()
    pending = iter(enumerate(entries))
    results = []
    success = 0

    async def worker():
        for index, entry in pending:
            try:
                result = await _process_credential_file(entry)
                if not isinstance(result, dict) or result.get("status") not in ("success", "error"):
                    raise ValueError("Invalid credential import result")
            except Exception as exc:
                # Every scheduled file must produce a result; otherwise the
                # progress reader waits forever for a worker that has exited.
                # CancelledError still propagates to stop disconnected imports.
                log.warning(f"文件导入异常: item={index}, type={type(exc).__name__}")
                result = {"status": "error", "code": "processing_failed",
                          "message": "此文件处理异常，已跳过；其他凭证继续导入，请检查后重试此项"}
            await queue.put({**result, "filename": entry["filename"], "item_index": index})

    workers = [asyncio.create_task(worker()) for _ in range(min(IMPORT_CONCURRENCY, total))]
    try:
        yield {"type": "start", "total_count": total, "completed_count": 0, "uploaded_count": 0}
        while len(results) < total:
            try:
                result = await asyncio.wait_for(queue.get(), timeout=IMPORT_HEARTBEAT_SECONDS)
            except TimeoutError:
                yield {"type": "heartbeat"}
                continue
            results.append(result)
            success += result["status"] == "success"
            yield {"type": "progress", "result": result, "total_count": total,
                   "completed_count": len(results), "uploaded_count": success}
        yield {"type": "complete", "uploaded_count": success, "total_count": total,
               "completed_count": total, "results": sorted(results, key=lambda row: row["item_index"]),
               "message": f"导入完成：成功 {success}，失败 {total - success}，共 {total} 个凭证"}
        log.info(f"文件导入完成: 成功 {success}/{total} 个凭证")
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def upload_credentials_common(files: List[UploadFile]) -> JSONResponse:
    """Backward-compatible JSON endpoint using the same bounded importer."""
    entries = await _read_credential_files(files)
    async for event in _credential_import_events(entries):
        if event["type"] == "complete":
            return JSONResponse(content={key: value for key, value in event.items() if key != "type"})


@router.post("/upload-stream")
async def upload_credentials_stream(files: List[UploadFile] = File(...), token: str = Depends(verify_panel_token)):
    entries = await _read_credential_files(files)

    async def stream():
        async for event in _credential_import_events(entries):
            yield json.dumps(event, ensure_ascii=False) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson", headers={
        "Cache-Control": "no-store", "X-Accel-Buffering": "no",
    })


async def get_creds_status_common(
    offset: int, limit: int, status_filter: str,
    error_code_filter: str = None, cooldown_filter: str = None, tier_filter: str = None
) -> JSONResponse:
    """获取凭证状态"""
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset 必须大于等于 0")
    if limit not in [20, 50, 100, 200, 500, 1000]:
        raise HTTPException(status_code=400, detail="limit 只能是 20、50、100、200、500 或 1000")
    if status_filter not in ["all", "enabled", "disabled"]:
        raise HTTPException(status_code=400, detail="status_filter 只能是 all、enabled 或 disabled")
    if cooldown_filter and cooldown_filter not in ["all", "in_cooldown", "no_cooldown"]:
        raise HTTPException(status_code=400, detail="cooldown_filter 只能是 all、in_cooldown 或 no_cooldown")
    if tier_filter and tier_filter not in ["all", "free", "pro", "ultra", "ultra_lite"]:
        raise HTTPException(status_code=400, detail="tier_filter 只能是 all、free、pro、ultra 或 ultra_lite")

    database = await get_database()
    result = await database.get_credentials_summary(
        offset=offset,
        limit=limit,
        status_filter=status_filter,
        error_code_filter=error_code_filter if error_code_filter and error_code_filter != "all" else None,
        cooldown_filter=cooldown_filter if cooldown_filter and cooldown_filter != "all" else None,
        tier_filter=tier_filter if tier_filter and tier_filter != "all" else None
    )

    return JSONResponse(content={
        "items": result["items"],
        "total": result["total"],
        "offset": offset,
        "limit": limit,
        "has_more": (offset + limit) < result["total"],
        "stats": result.get("stats", {"total": 0, "normal": 0, "disabled": 0}),
    })


async def download_all_creds_common() -> Response:
    """打包导出所有凭证（规范格式，文件名=邮箱.json）"""
    zip_filename = "antigravity_credentials.zip"

    database = await get_database()
    emails = await database.list_credentials()

    if not emails:
        raise HTTPException(status_code=404, detail="没有找到凭证")

    log.info(f"开始打包 {len(emails)} 个凭证...")

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        success_count = 0
        for email in emails:
            try:
                state = await database.get_credential_state(email)
                export_data = {
                    "refresh_token": "",
                    "email": email,
                    "tier": state.get("tier", "pro"),
                    "project_id": state.get("project_id", ""),
                }
                # 从凭证数据中取 refresh_token
                cred = await database.get_credential(email)
                if cred:
                    export_data["refresh_token"] = cred.get("refresh_token", "")
                else:
                    continue

                if cred.get("note"):
                    export_data["note"] = cred["note"]
                content = json.dumps(export_data, ensure_ascii=False, indent=2)
                zip_file.writestr(f"{email}.json", content)
                success_count += 1
            except Exception as e:
                log.warning(f"导出凭证 {email} 时出错: {e}")
                continue

    log.info(f"打包完成: 成功 {success_count}/{len(emails)} 个凭证")

    zip_buffer.seek(0)
    return Response(
        content=zip_buffer.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={zip_filename}"},
    )


async def verify_credential_project_common(email: str) -> JSONResponse:
    """验证并重新获取凭证的 project id / tier"""

    database = await get_database()

    credential_data = await database.get_credential(email)
    if not credential_data:
        raise HTTPException(status_code=404, detail="凭证不存在")

    credentials = Credentials.from_dict(credential_data)

    token_refreshed = await credentials.refresh_if_needed()

    if token_refreshed:
        log.info(f"Token已自动刷新: {email}")
        credential_data = credentials.to_dict()
        await database.store_credential(email, credential_data)

    # 重新获取 project id / tier
    api_base_url = await get_antigravity_api_url()
    project_id, subscription_tier = await fetch_project_id_and_tier(
        access_token=credentials.access_token,
        user_agent=ANTIGRAVITY_USER_AGENT,
        api_base_url=api_base_url,
    )

    # project_id 获取失败时回退，避免空值入库
    if not project_id:
        log.warning(f"校验凭证 {email}: project_id 获取失败，使用回退值 aicode-consumers")
        project_id = "aicode-consumers"

    # tier 获取失败时保留数据库原值，不覆盖为 None
    if not subscription_tier:
        old_state = await database.get_credential_state(email)
        subscription_tier = old_state.get("tier") or "pro"
        log.warning(f"校验凭证 {email}: tier 获取失败，保留原值 {subscription_tier}")

    credential_data["project_id"] = project_id

    await database.store_credential(email, credential_data)

    state_update = {
        "disabled": False,
        "error_codes": [],
        "tier": subscription_tier,
        "project_id": project_id,
    }

    await database.update_credential_state(email, state_update)

    log.info(f"检验凭证成功: {email} - Project ID: {project_id}, Tier: {subscription_tier}")

    return JSONResponse(content={
        "success": True,
        "email": email,
        "project_id": project_id,
        "subscription_tier": subscription_tier,
        "message": "检验成功！已解除禁用状态并清除错误码",
    })


# =============================================================================
# 路由处理函数
# =============================================================================


@router.post("/import-antigravity-refresh-tokens")
async def import_antigravity_refresh_token_credentials(
    request: Request,
    token: str = Depends(verify_panel_token),
) -> JSONResponse:
    """Import AG credentials after OAuth and Project ID validation.

    The endpoint never returns submitted refresh tokens or generated access
    tokens.  ``token`` is intentionally unused beyond the auth dependency.
    """

    del token
    try:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > MAX_IMPORT_BODY_BYTES:
                    raise HTTPException(status_code=413, detail="导入文件不能超过 1 MiB")
            except ValueError:
                raise HTTPException(status_code=400, detail="Content-Length 无效")

        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > MAX_IMPORT_BODY_BYTES:
                raise HTTPException(status_code=413, detail="导入文件不能超过 1 MiB")
        try:
            payload = json.loads(bytes(body))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(status_code=400, detail="请求内容不是有效的 JSON")

        candidates = parse_import_request_payload(payload)
        result = await import_antigravity_refresh_tokens(candidates)
        return JSONResponse(content=result)
    except AntigravityImportInputError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except HTTPException:
        raise
    except Exception as error:
        log.error(
            "Antigravity Refresh Token import aborted: "
            f"exception={type(error).__name__}"
        )
        raise HTTPException(
            status_code=500, detail="导入过程中发生内部错误，请检查服务器日志"
        ) from error


@router.post("/upload")
async def upload_credentials( files: List[UploadFile] = File(...),
    token: str = Depends(verify_panel_token),
):
    """批量导入凭证文件（规范 JSON / ZIP）"""
    try:
        return await upload_credentials_common(files)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量导入失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_creds_status(
    token: str = Depends(verify_panel_token),
    offset: int = 0,
    limit: int = 50,
    status_filter: str = "all",
    error_code_filter: str = "all",
    cooldown_filter: str = "all",
    tier_filter: str = "all"):
    """获取凭证状态（轻量级摘要，支持分页和状态筛选）"""
    try:
        return await get_creds_status_common(
            offset, limit, status_filter,
            error_code_filter=error_code_filter,
            cooldown_filter=cooldown_filter,
            tier_filter=tier_filter
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/action")
async def creds_action(
    request: CredFileActionRequest,
    token: str = Depends(verify_panel_token)):
    """对凭证执行操作（启用/禁用/删除）"""
    try:

        email = request.email
        action = request.action

        log.info(f"Performing action '{action}' on credential: {email}")

        database = await get_database()

        if action == "enable":
            result = await set_cred_disabled(email, False)
            if result:
                return JSONResponse(content={"message": f"已启用凭证 {email}"})
            raise HTTPException(status_code=500, detail="启用凭证失败，可能凭证不存在")

        elif action == "disable":
            result = await set_cred_disabled(email, True)
            if result:
                return JSONResponse(content={"message": f"已禁用凭证 {email}"})
            raise HTTPException(status_code=500, detail="禁用凭证失败，可能凭证不存在")

        elif action == "delete":
            success = await remove_credential(email)
            if success:
                return JSONResponse(content={"message": f"已删除凭证 {email}"})
            raise HTTPException(status_code=500, detail="删除凭证失败")

        else:
            raise HTTPException(status_code=400, detail="无效的操作类型")

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"凭证操作失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-action")
async def creds_batch_action(
    request: CredFileBatchActionRequest,
    token: str = Depends(verify_panel_token)):
    """批量对凭证执行操作（启用/禁用/删除）"""
    try:

        action = request.action
        emails = request.emails

        if not emails:
            raise HTTPException(status_code=400, detail="凭证列表不能为空")

        log.info(f"对 {len(emails)} 个凭证执行批量操作 '{action}'")

        success_count = 0
        errors = []

        for email in emails:
            try:
                if action == "enable":
                    await set_cred_disabled(email, False)
                    success_count += 1
                elif action == "disable":
                    await set_cred_disabled(email, True)
                    success_count += 1
                elif action == "delete":
                    if await remove_credential(email):
                        success_count += 1
                    else:
                        errors.append(f"{email}: 删除失败")
                        continue
                else:
                    errors.append(f"{email}: 无效的操作类型")
                    continue
            except Exception as e:
                log.error(f"处理 {email} 时出错: {e}")
                errors.append(f"{email}: 处理失败 - {str(e)}")
                continue

        result_message = f"批量操作完成：成功处理 {success_count}/{len(emails)} 个凭证"
        if errors:
            result_message += "\n错误详情:\n" + "\n".join(errors)

        return JSONResponse(content={
            "success_count": success_count,
            "total_count": len(emails),
            "errors": errors,
            "message": result_message,
        })

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"批量凭证操作失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export/{email}")
async def export_credential(
    email: str,
    token: str = Depends(verify_panel_token)):
    """导出单个凭证（规范格式，文件名=邮箱.json）"""
    try:
        database = await get_database()

        cred = await database.get_credential(email)
        if not cred:
            raise HTTPException(status_code=404, detail="凭证不存在")

        state = await database.get_credential_state(email)

        export_data = {
            "refresh_token": cred.get("refresh_token", ""),
            "email": email,
            "tier": state.get("tier", "pro"),
            "project_id": cred.get("project_id", ""),
        }

        if cred.get("note"):
            export_data["note"] = cred["note"]
        content = json.dumps(export_data, ensure_ascii=False, indent=2)

        return Response(
            content=content,
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{email}.json"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"导出凭证失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download-all")
async def download_all_creds(
    token: str = Depends(verify_panel_token)):
    """打包导出所有凭证（每个文件为 邮箱.json）"""
    try:
        return await download_all_creds_common()
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"打包导出失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download-selected")
async def download_selected_credentials(
    request: CredentialSelectionRequest,
    token: str = Depends(verify_panel_token),
):
    """Export exactly the selected credentials; fail instead of silently omitting missing rows."""
    database = await get_database()
    selected = list(dict.fromkeys(request.emails))
    entries = []
    for email in selected:
        cred = await database.get_credential(email)
        if not cred:
            raise HTTPException(status_code=404, detail=f"凭证不存在，请刷新列表后重试：{email}")
        entries.append((email, {
            "email": email,
            "refresh_token": cred.get("refresh_token", ""),
            "project_id": cred.get("project_id", ""),
            "tier": cred.get("tier", "pro"),
            **({"note": cred["note"]} if cred.get("note") else {}),
        }))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for email, data in entries:
            archive.writestr(f"{quote(email, safe='@._+-')}.json", json.dumps(data, ensure_ascii=False, indent=2))
    return Response(
        content=buffer.getvalue(), media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="antigravity_selected_credentials.zip"',
            "Cache-Control": "no-store",
            "X-Credential-Count": str(len(entries)),
        },
    )


@router.post("/verify-project/{email}")
async def verify_credential_project(
    email: str,
    token: str = Depends(verify_panel_token)):
    """检验凭证（重新获取 Project ID / Tier，可修复 403）"""
    try:
        return await verify_credential_project_common(email)
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"检验凭证失败 {email}: {e}")
        raise HTTPException(status_code=500, detail=f"检验失败: {str(e)}")


@router.get("/errors/{email}")
async def get_credential_errors(
    email: str,
    token: str = Depends(verify_panel_token)):
    """获取凭证的错误信息"""
    try:
        database = await get_database()
        error_info = await database.get_credential_errors(email)
        return JSONResponse(content=error_info)

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证错误信息失败 {email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quota/{email}")
async def get_credential_quota(
    email: str,
    token: str = Depends(verify_panel_token)):
    """获取凭证的额度信息"""
    try:
        database = await get_database()

        credential_data = await database.get_credential(email)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        creds = Credentials.from_dict(credential_data)

        if creds.is_expired():
            await asyncio.wait_for(creds.refresh(), timeout=30.0)
            updated_data = creds.to_dict()
            log.info(f"Token已自动刷新: {email}")
            await database.store_credential(email, updated_data)
            credential_data = updated_data

        access_token = credential_data.get("access_token") or credential_data.get("token")
        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")

        quota_info = await fetch_quota_info(access_token, credential_data.get("project_id", ""))

        if quota_info.get("success"):
            result = {
                "success": True,
                "email": email,
                "models": quota_info.get("models", {})
            }
            for extra_key in ("minResetDays", "accountTier", "isPro", "quotaGroups", "source"):
                if extra_key in quota_info:
                    result[extra_key] = quota_info[extra_key]
            return JSONResponse(content=result)
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "email": email,
                    "error": quota_info.get("error", "未知错误")
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"获取凭证额度失败 {email}: {e}")
        raise HTTPException(status_code=500, detail=f"获取额度失败: {str(e)}")


@router.post("/test/{email}")
async def test_credential(
    email: str,
    model: str = Query("gemini-4-argon-medium", min_length=1, max_length=128),
    _token: str = Depends(verify_panel_token)
):
    """
    测试凭证是否可用

    Returns:
        - 200: 凭证可用
        - 429: 凭证被限流但有效
        - 其他: 凭证失败（返回实际错误码）
    """
    try:
        database = await get_database()

        credential_data = await database.get_credential(email)
        if not credential_data:
            raise HTTPException(status_code=404, detail="凭证不存在")

        credentials = Credentials.from_dict(credential_data)
        token_refreshed = credentials.is_expired()
        if token_refreshed:
            await asyncio.wait_for(credentials.refresh(), timeout=30.0)

        if token_refreshed:
            log.info(f"Token已自动刷新: {email}")
            credential_data = credentials.to_dict()
            await database.store_credential(email, credential_data)

        access_token = credential_data.get("access_token") or credential_data.get("token")
        if not access_token:
            raise HTTPException(status_code=400, detail="凭证中没有访问令牌")

        from src.httpx_client import post_async

        project_id = credential_data.get("project_id", "")
        if not project_id:
            raise HTTPException(status_code=400, detail="凭证中没有项目ID")

        test_model = model.strip()
        if not test_model:
            raise HTTPException(status_code=422, detail="测试模型不能为空")

        api_base_url = await get_antigravity_api_url()
        headers = build_antigravity_headers(access_token)

        response = await post_async(
            url=f"{api_base_url}/v1internal:generateContent",
            json={
                "model": test_model,
                "project": project_id,
                "request": {
                    "contents": [{"role": "user", "parts": [{"text": "Reply OK."}]}],
                    "generationConfig": {"maxOutputTokens": 8},
                }
            },
            headers=headers,
            timeout=30.0
        )

        status_code = response.status_code
        if status_code == 200:
            # The batch tester must apply the same response semantics as the
            # model API, rather than clearing errors on an HTTP-200 rejection.
            from src.stream_events import inspect_line, stream_error
            kind, classified = inspect_line("data: " + response.text, allow_text_notice=True) if response.content else ("metadata", None)
            if kind != "content":
                if kind not in ("blocked", "error", "invalid"):
                    classified = stream_error(502, "UPSTREAM_EMPTY_RESPONSE", "上游未返回有效内容")
                status_code = classified.status_code
                response = httpx.Response(status_code, content=classified.body)

        if status_code == 200 or status_code == 429:
            log.info(f"凭证测试成功: {email} (status={status_code})")
            if status_code == 200:
                await database.update_credential_state(email, {
                    "error_codes": [],
                    "error_messages": {}
                })

            return JSONResponse(
                status_code=status_code,
                content={
                    "success": True,
                    "status_code": status_code,
                    "message": "测试成功" if status_code == 200 else "凭证有效，但模型请求被限流",
                    "email": email
                }
            )
        else:
            log.warning(f"凭证测试失败: {email} (status={status_code})")
            try:
                error_text = response.text if hasattr(response, "text") else ""
                error_codes = [status_code]
                error_messages = {str(status_code): error_text if error_text else f"HTTP {status_code}"}

                await database.update_credential_state(email, {
                    "error_codes": error_codes,
                    "error_messages": error_messages
                })
            except Exception as e:
                log.error(f"保存测试错误信息失败: {e}")

        error_text = response.text if hasattr(response, "text") else ""

        return JSONResponse(
            status_code=status_code,
            content={
                "success": False,
                "status_code": status_code,
                "message": f"测试失败: HTTP {status_code}",
                "error": error_text,
                "email": email
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"测试凭证失败 {email}: {e}")
        raise HTTPException(status_code=500, detail=f"测试失败: {str(e)}")
