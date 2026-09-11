"""Import Antigravity credentials from refresh tokens or Manager backups.

This module deliberately keeps OAuth secrets and tokens out of API results and
application logs.  A credential is persisted only after Google accepts the
refresh token and the Antigravity project can be resolved.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from config import get_antigravity_api_url
from log import log
from src.google_oauth_api import Credentials, fetch_project_id_and_tier, get_user_email
from src.storage_adapter import StorageAdapter, get_storage_adapter
from src.utils import (
    ANTIGRAVITY_CLIENT_ID,
    ANTIGRAVITY_CLIENT_SECRET,
    ANTIGRAVITY_SCOPES,
    ANTIGRAVITY_USER_AGENT,
    TOKEN_URL,
)

MAX_IMPORT_ITEMS = 100
MAX_REFRESH_TOKEN_LENGTH = 4096
IMPORT_CONCURRENCY = 3
IMPORT_ITEM_TIMEOUT_SECONDS = 90
IMPORT_BATCH_TIMEOUT_SECONDS = 300
MAX_IMPORT_BODY_BYTES = 1024 * 1024


class AntigravityImportInputError(ValueError):
    """The submitted import document is structurally invalid."""


@dataclass(frozen=True)
class RefreshTokenCandidate:
    item: int
    refresh_token: Optional[str]
    error_code: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class PreparedCredential:
    candidate: RefreshTokenCandidate
    credential_data: Dict[str, Any]
    project_id: str
    email: Optional[str]
    tier: Optional[str]
    credit_amount: Optional[int]


def _normalize_email(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().casefold()
    if not normalized or len(normalized) > 254 or "@" not in normalized:
        return None
    return normalized


def _validate_refresh_token(value: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(value, str):
        return None, "missing_refresh_token", "缺少 Refresh Token"

    token = value.strip()
    if not token:
        return None, "missing_refresh_token", "缺少 Refresh Token"
    if len(token) > MAX_REFRESH_TOKEN_LENGTH:
        return None, "invalid_refresh_token", "Refresh Token 长度无效"
    if any(character.isspace() or ord(character) < 32 for character in token):
        return None, "invalid_refresh_token", "Refresh Token 不能包含空白或控制字符"
    return token, None, None


def _iter_direct_tokens(values: Sequence[Any]) -> Iterable[Any]:
    for value in values:
        # Make the JSON API friendly to text-area clients that submit one string
        # containing several newline-separated tokens.
        if isinstance(value, str) and ("\n" in value or "\r" in value):
            yield from value.splitlines()
        else:
            yield value


def _backup_accounts(manager_backup: Any) -> Sequence[Any]:
    if manager_backup is None:
        return []
    if isinstance(manager_backup, list):
        # Current Antigravity Manager downloads response.accounts directly, so
        # its real backup file is a top-level array.
        return manager_backup
    if isinstance(manager_backup, dict):
        accounts = manager_backup.get("accounts")
        if not isinstance(accounts, list):
            raise AntigravityImportInputError("Manager 备份必须包含 accounts 数组")
        return accounts
    raise AntigravityImportInputError("Manager 备份必须是账号数组或包含 accounts 的对象")


def extract_refresh_token_candidates(
    refresh_tokens: Sequence[Any], manager_backup: Any = None
) -> List[RefreshTokenCandidate]:
    """Normalize direct tokens and both Manager backup shapes.

    Invalid account entries are returned as per-item failures so that one bad
    record does not hide the valid records in a large backup.
    """

    raw_entries: List[Any] = list(_iter_direct_tokens(refresh_tokens))

    for account in _backup_accounts(manager_backup):
        if not isinstance(account, dict):
            raw_entries.append(None)
            continue

        token_value = account.get("refresh_token", account.get("refreshToken"))
        # Also accept Manager's internal account shape for manually recovered
        # backups, while keeping the documented export format as the default.
        nested_token = account.get("token")
        if token_value is None and isinstance(nested_token, dict):
            token_value = nested_token.get("refresh_token", nested_token.get("refreshToken"))
        # Backup email is intentionally ignored. Only Google userinfo can
        # provide an identity that is safe to return or persist.
        raw_entries.append(token_value)

    if not raw_entries:
        raise AntigravityImportInputError("请提供 Refresh Token 或 Manager 备份")
    if len(raw_entries) > MAX_IMPORT_ITEMS:
        raise AntigravityImportInputError(
            f"单次最多导入 {MAX_IMPORT_ITEMS} 个账号，当前为 {len(raw_entries)} 个"
        )

    candidates: List[RefreshTokenCandidate] = []
    for index, raw_token in enumerate(raw_entries, start=1):
        token, error_code, error = _validate_refresh_token(raw_token)
        candidates.append(
            RefreshTokenCandidate(
                item=index,
                refresh_token=token,
                error_code=error_code,
                error=error,
            )
        )
    return candidates


def parse_import_request_payload(payload: Any) -> List[RefreshTokenCandidate]:
    """Parse the JSON request without Pydantic echoing token values in 422s."""

    if isinstance(payload, list):
        # Also accept the Manager backup file itself as the request body for
        # command-line/API callers. The web UI wraps this under manager_backup.
        if all(isinstance(item, str) for item in payload):
            return extract_refresh_token_candidates(payload)
        return extract_refresh_token_candidates([], payload)
    if not isinstance(payload, dict):
        raise AntigravityImportInputError("请求内容必须是 JSON 对象")
    refresh_tokens = payload.get("refresh_tokens", [])
    if not isinstance(refresh_tokens, list):
        raise AntigravityImportInputError("refresh_tokens 必须是数组")
    manager_backup = payload.get("manager_backup")
    if manager_backup is None and "accounts" in payload:
        manager_backup = payload
    return extract_refresh_token_candidates(refresh_tokens, manager_backup)


def _token_digest(refresh_token: str) -> str:
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def _safe_import_error(error: Exception) -> Tuple[str, str]:
    """Map OAuth/network exceptions to stable messages without echoing secrets."""

    if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
        return "import_timeout", "凭证验证超时，请稍后重试"
    oauth_error_code = getattr(error, "oauth_error_code", None)
    status_code = getattr(error, "status_code", None)
    lowered = str(error).lower()
    if oauth_error_code == "invalid_grant" or "invalid_grant" in lowered:
        return "invalid_grant", "Refresh Token 已失效、被撤销或 OAuth Client 不匹配"
    if status_code == 429:
        return "oauth_rate_limited", "Google OAuth 请求过于频繁，请稍后重试"
    if isinstance(status_code, int) and status_code >= 500:
        return "oauth_upstream_error", "Google OAuth 暂时不可用，请稍后重试"
    if "timeout" in lowered or "timed out" in lowered:
        return "oauth_timeout", "连接 Google OAuth 超时，请稍后重试"
    if "connect" in lowered or "network" in lowered:
        return "oauth_network_error", "无法连接 Google OAuth，请检查网络或上游代理"
    return "oauth_exchange_failed", "Google OAuth 拒绝了该 Refresh Token"


def build_antigravity_credential_data(credentials: Credentials, project_id: str) -> Dict[str, Any]:
    """Build the same self-contained credential shape as the OAuth flow."""

    data: Dict[str, Any] = {
        "client_id": ANTIGRAVITY_CLIENT_ID,
        "client_secret": ANTIGRAVITY_CLIENT_SECRET,
        "token": credentials.access_token,
        "refresh_token": credentials.refresh_token,
        "scopes": ANTIGRAVITY_SCOPES,
        "token_uri": TOKEN_URL,
        "project_id": project_id,
    }
    if credentials.expires_at:
        data["expiry"] = credentials.expires_at.isoformat()
    return data


async def _load_existing_identity_index(
    storage_adapter: StorageAdapter,
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """Return token, verified-email, and project indexes for existing AG creds."""

    filenames = await storage_adapter.list_credentials(mode="antigravity")
    states = await storage_adapter.get_all_credential_states(mode="antigravity")
    token_index: Dict[str, str] = {}
    email_index: Dict[str, str] = {}
    project_index: Dict[str, str] = {}

    for filename, state in states.items():
        if not isinstance(state, dict):
            continue
        email = _normalize_email(state.get("user_email"))
        if email:
            email_index[email] = filename

    async def load_one(filename: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        return filename, await storage_adapter.get_credential(filename, mode="antigravity")

    # Avoid hundreds of sequential DB round trips without flooding a remote DB.
    for offset in range(0, len(filenames), 25):
        rows = await asyncio.gather(
            *(load_one(filename) for filename in filenames[offset : offset + 25])
        )
        for filename, credential_data in rows:
            if not credential_data:
                continue
            if not isinstance(credential_data, dict):
                # Legacy upload accepts any valid JSON. Ignore malformed pool
                # entries instead of letting one bad row block all imports.
                continue
            refresh_token = credential_data.get("refresh_token")
            if isinstance(refresh_token, str) and refresh_token:
                token_index[_token_digest(refresh_token)] = filename
            project_id = credential_data.get("project_id")
            if isinstance(project_id, str) and project_id.strip():
                project_index[project_id.strip()] = filename

    return token_index, email_index, project_index


async def _prepare_candidate(
    candidate: RefreshTokenCandidate,
    semaphore: asyncio.Semaphore,
    get_api_url: Callable[[], Awaitable[str]],
    fetch_project: Callable[..., Awaitable[Tuple[Optional[str], Optional[str], Optional[int]]]],
    fetch_email: Callable[[Credentials], Awaitable[Optional[str]]],
) -> Tuple[Optional[PreparedCredential], Optional[Dict[str, Any]]]:
    assert candidate.refresh_token is not None

    async with semaphore:
        credentials = Credentials(
            access_token="",
            refresh_token=candidate.refresh_token,
            client_id=ANTIGRAVITY_CLIENT_ID,
            client_secret=ANTIGRAVITY_CLIENT_SECRET,
        )
        try:
            async with asyncio.timeout(IMPORT_ITEM_TIMEOUT_SECONDS):
                # The general proxy keeps a long timeout for generation calls;
                # credential import must fail promptly instead of holding a
                # batch request for 15 minutes on an unreachable OAuth host.
                await credentials.refresh(timeout=30.0)
                api_base_url = await get_api_url()
                project_id, tier, credit_amount = await fetch_project(
                    access_token=credentials.access_token,
                    user_agent=ANTIGRAVITY_USER_AGENT,
                    api_base_url=api_base_url,
                    include_credits=True,
                )
                if not project_id:
                    return None, {
                        "item": candidate.item,
                        "status": "failed",
                        "error_code": "project_not_found",
                        "error": "Refresh Token 有效，但无法获取 Antigravity Project ID",
                    }

                # Only trust the Google userinfo response for persisted identity;
                # Manager backup email fields are user-controlled input.
                email = _normalize_email(await fetch_email(credentials))
                return (
                    PreparedCredential(
                        candidate=candidate,
                        credential_data=build_antigravity_credential_data(credentials, project_id),
                        project_id=project_id,
                        email=email,
                        tier=tier,
                        credit_amount=credit_amount,
                    ),
                    None,
                )
        except Exception as error:
            error_code, message = _safe_import_error(error)
            # Do not include the exception text: OAuth libraries can attach the
            # submitted form body to some errors.
            log.warning(
                "Antigravity Refresh Token import failed: "
                f"item={candidate.item}, code={error_code}, "
                f"exception={type(error).__name__}"
            )
            return None, {
                "item": candidate.item,
                "status": "failed",
                "error_code": error_code,
                "error": message,
            }


async def import_antigravity_refresh_tokens(
    candidates: Sequence[RefreshTokenCandidate],
    *,
    storage_adapter: Optional[StorageAdapter] = None,
    get_api_url: Callable[[], Awaitable[str]] = get_antigravity_api_url,
    fetch_project: Callable[..., Awaitable[Any]] = fetch_project_id_and_tier,
    fetch_email: Callable[[Credentials], Awaitable[Optional[str]]] = get_user_email,
) -> Dict[str, Any]:
    """Validate, enrich, de-duplicate, and persist AG refresh tokens."""

    adapter = storage_adapter or await get_storage_adapter()
    existing_tokens, existing_emails, existing_projects = await _load_existing_identity_index(
        adapter
    )
    results: List[Optional[Dict[str, Any]]] = [None] * len(candidates)
    seen_request_tokens: Dict[str, int] = {}
    to_prepare: List[RefreshTokenCandidate] = []

    for position, candidate in enumerate(candidates):
        if candidate.error_code or not candidate.refresh_token:
            results[position] = {
                "item": candidate.item,
                "status": "failed",
                "error_code": candidate.error_code or "missing_refresh_token",
                "error": candidate.error or "缺少 Refresh Token",
            }
            continue

        digest = _token_digest(candidate.refresh_token)
        if digest in seen_request_tokens:
            results[position] = {
                "item": candidate.item,
                "status": "skipped",
                "error_code": "duplicate_in_request",
                "error": f"与第 {seen_request_tokens[digest]} 项重复",
            }
            continue
        seen_request_tokens[digest] = candidate.item

        if digest in existing_tokens:
            results[position] = {
                "item": candidate.item,
                "status": "skipped",
                "error_code": "already_exists",
                "error": "该 Refresh Token 已存在",
                "filename": existing_tokens[digest],
            }
            continue
        to_prepare.append(candidate)

    semaphore = asyncio.Semaphore(IMPORT_CONCURRENCY)
    try:
        async with asyncio.timeout(IMPORT_BATCH_TIMEOUT_SECONDS):
            prepared_rows = await asyncio.gather(
                *(
                    _prepare_candidate(
                        candidate,
                        semaphore,
                        get_api_url,
                        fetch_project,
                        fetch_email,
                    )
                    for candidate in to_prepare
                )
            )
    except TimeoutError:
        # Preparation does not write storage, so cancellation leaves no partial
        # credentials. The caller can safely retry with a smaller batch.
        prepared_rows = [
            (
                None,
                {
                    "item": candidate.item,
                    "status": "failed",
                    "error_code": "batch_timeout",
                    "error": "本批验证超过 5 分钟，未完成的凭证均未保存，请缩小批次后重试",
                },
            )
            for candidate in to_prepare
        ]

    prepared_by_item: Dict[int, PreparedCredential] = {}
    errors_by_item: Dict[int, Dict[str, Any]] = {}
    for prepared, error_result in prepared_rows:
        if prepared:
            prepared_by_item[prepared.candidate.item] = prepared
        elif error_result:
            errors_by_item[error_result["item"]] = error_result

    seen_batch_emails: Dict[str, int] = {}
    seen_batch_projects: Dict[str, int] = {}
    for position, candidate in enumerate(candidates):
        if results[position] is not None:
            continue
        if candidate.item in errors_by_item:
            results[position] = errors_by_item[candidate.item]
            continue

        prepared = prepared_by_item[candidate.item]
        normalized_email = _normalize_email(prepared.email)
        if prepared.project_id in seen_batch_projects:
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "skipped",
                "error_code": "duplicate_project_in_request",
                "error": f"与第 {seen_batch_projects[prepared.project_id]} 项属于同一 Antigravity Project",
            }
            continue
        if prepared.project_id in existing_projects:
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "skipped",
                "error_code": "project_already_exists",
                "error": "该 Antigravity Project 已存在",
                "filename": existing_projects[prepared.project_id],
            }
            continue
        if normalized_email and normalized_email in seen_batch_emails:
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "skipped",
                "error_code": "duplicate_email_in_request",
                "error": f"与第 {seen_batch_emails[normalized_email]} 项属于同一 Google 账号",
            }
            continue
        if normalized_email and normalized_email in existing_emails:
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "skipped",
                "error_code": "email_already_exists",
                "error": "该 Google 账号已存在",
                "filename": existing_emails[normalized_email],
            }
            continue

        # Project ID is verified, stable, and available even when userinfo is
        # temporarily unavailable. It also makes concurrent imports of the same
        # account converge on one filename across production workers.
        identity_digest = hashlib.sha256(
            f"project:{prepared.project_id}".encode("utf-8")
        ).hexdigest()
        seen_batch_projects[prepared.project_id] = candidate.item
        if normalized_email:
            seen_batch_emails[normalized_email] = candidate.item
        filename = f"ag_import_{identity_digest[:20]}.json"

        # Deterministic filenames make concurrent imports across workers
        # converge on one row instead of creating duplicates.
        if await adapter.get_credential(filename, mode="antigravity"):
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "skipped",
                "error_code": "already_exists",
                "error": "该 Google 账号已存在",
                "filename": filename,
            }
            continue

        stored = await adapter.store_credential(
            filename, prepared.credential_data, mode="antigravity"
        )
        if not stored:
            # Another worker may have inserted the same deterministic identity
            # after our preflight check. Treat that unique-key race as an
            # idempotent skip, not a storage outage.
            if await adapter.get_credential(filename, mode="antigravity"):
                results[position] = {
                    "item": candidate.item,
                    "email": normalized_email,
                    "status": "skipped",
                    "error_code": "already_exists",
                    "error": "该 Google 账号已被另一并发请求导入",
                    "filename": filename,
                }
                continue
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "failed",
                "error_code": "storage_failed",
                "error": "凭证验证成功，但保存失败",
            }
            continue

        state_updates = {
            "disabled": False,
            "error_codes": [],
            "last_success": time.time(),
            "user_email": normalized_email,
            "tier": prepared.tier or "pro",
        }
        state_saved = False
        for retry in range(3):
            state_saved = await adapter.update_credential_state(
                filename, state_updates, mode="antigravity"
            )
            if state_saved:
                break
            if retry < 2:
                await asyncio.sleep(0.05 * (retry + 1))
        if not state_saved:
            # Do not delete here: with multiple production workers, another
            # request may now own the same deterministic row. The credential is
            # usable because all storage backends create safe default state;
            # report the metadata problem explicitly for follow-up verification.
            results[position] = {
                "item": candidate.item,
                "email": normalized_email,
                "status": "partial",
                "filename": filename,
                "project_id": prepared.project_id,
                "tier": prepared.tier,
                "error_code": "state_metadata_failed",
                "error": "凭证已保存，但邮箱或等级状态更新失败，请稍后执行凭证检验",
            }
            continue

        if normalized_email:
            existing_emails[normalized_email] = filename
        existing_projects[prepared.project_id] = filename
        existing_tokens[_token_digest(candidate.refresh_token or "")] = filename
        results[position] = {
            "item": candidate.item,
            "email": normalized_email,
            "status": "success",
            "filename": filename,
            "project_id": prepared.project_id,
            "tier": prepared.tier,
            "credit_amount": prepared.credit_amount,
        }

    final_results = [result for result in results if result is not None]
    succeeded = sum(result["status"] == "success" for result in final_results)
    skipped = sum(result["status"] == "skipped" for result in final_results)
    failed = sum(result["status"] == "failed" for result in final_results)
    partial = sum(result["status"] == "partial" for result in final_results)
    log.info(
        "Antigravity Refresh Token import completed: "
        f"total={len(final_results)}, success={succeeded}, "
        f"partial={partial}, skipped={skipped}, failed={failed}"
    )
    return {
        "total": len(final_results),
        "imported_count": succeeded,
        "partial_count": partial,
        "skipped_count": skipped,
        "failed_count": failed,
        "results": final_results,
    }
