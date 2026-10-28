"""RT/Manager import adapted from my-custom: validate first, skip duplicates, never echo tokens."""
import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from config import get_antigravity_api_url, get_googleapis_proxy_url, get_oauth_proxy_url
from log import log
from src.httpx_client import get_async, post_async
from src.storage.database import get_database
from src.utils import ANTIGRAVITY_CLIENT_ID, ANTIGRAVITY_CLIENT_SECRET, ANTIGRAVITY_USER_AGENT

MAX_IMPORT_ITEMS = 100
MAX_REFRESH_TOKEN_LENGTH = 4096
MAX_IMPORT_BODY_BYTES = 1024 * 1024
IMPORT_CONCURRENCY = 3
IMPORT_ITEM_TIMEOUT_SECONDS = 90
IMPORT_BATCH_TIMEOUT_SECONDS = 300


class AntigravityImportInputError(ValueError):
    """The submitted import document is structurally invalid."""


@dataclass(frozen=True)
class RefreshTokenCandidate:
    item: int
    refresh_token: Optional[str]
    error_code: Optional[str] = None
    error: Optional[str] = None


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


class ImportFailure(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(message)


async def prepare_credential(refresh_token):
    """Use existing configured OAuth/Google proxies, without logging upstream bodies."""
    oauth_url = await get_oauth_proxy_url()
    response = await post_async(
        f"{oauth_url.rstrip('/')}/token",
        data={"refresh_token": refresh_token, "grant_type": "refresh_token",
              "client_id": ANTIGRAVITY_CLIENT_ID, "client_secret": ANTIGRAVITY_CLIENT_SECRET},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30.0,
    )
    if response.status_code != 200:
        if response.status_code == 429:
            raise ImportFailure("oauth_rate_limited", "Google OAuth 请求过于频繁，请稍后重试")
        if response.status_code >= 500:
            raise ImportFailure("oauth_unavailable", "Google OAuth 暂时不可用，请稍后重试")
        raise ImportFailure("oauth_rejected", "RT 已失效、被撤销或 OAuth Client 不匹配")
    data = response.json()
    access_token = data.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise ImportFailure("oauth_invalid_response", "Google 未返回有效的访问令牌")
    headers = {"Authorization": f"Bearer {access_token}", "User-Agent": ANTIGRAVITY_USER_AGENT}
    google_url = await get_googleapis_proxy_url()
    response = await get_async(f"{google_url.rstrip('/')}/oauth2/v2/userinfo", headers=headers, timeout=30.0)
    if response.status_code != 200:
        raise ImportFailure("userinfo_failed", "RT 有效，但获取 Google 账号邮箱失败")
    user = response.json()
    email = _normalize_email(user.get("email"))
    if not email or user.get("verified_email") is False:
        raise ImportFailure("email_not_found", "无法确认 Google 账号邮箱，未保存凭证")

    api_url = await get_antigravity_api_url()
    response = await post_async(
        f"{api_url.rstrip('/')}/v1internal:loadCodeAssist", headers=headers,
        json={"metadata": {"ideType": "ANTIGRAVITY"}}, timeout=30.0,
    )
    if response.status_code != 200:
        raise ImportFailure("project_lookup_failed", f"获取 Antigravity 项目失败（HTTP {response.status_code}）")
    account = response.json()
    project = account.get("cloudaicompanionProject")
    if isinstance(project, dict):
        project = project.get("id")
    if not isinstance(project, str) or not project.strip():
        raise ImportFailure("project_not_found", "RT 有效，但没有获取到 Antigravity Project ID；请先在官方应用激活账号")
    tier_data = account.get("paidTier") or account.get("currentTier") or {}
    raw_tier = str(tier_data.get("id", "") if isinstance(tier_data, dict) else "").lower()
    tier = "ultra" if "ultra" in raw_tier else "pro" if "pro" in raw_tier else "free"
    credential = {
        "access_token": access_token, "refresh_token": data.get("refresh_token") or refresh_token,
        "client_id": ANTIGRAVITY_CLIENT_ID, "client_secret": ANTIGRAVITY_CLIENT_SECRET,
        "project_id": project.strip(), "tier": tier,
        "expiry": (datetime.now(timezone.utc) + timedelta(seconds=int(data.get("expires_in", 3600)))).isoformat(),
    }
    return email, credential


def _failure(item, code, message):
    return {"item": item, "status": "failed", "error_code": code, "error": message}


async def import_antigravity_refresh_tokens(candidates, *, database=None):
    database = database or await get_database()
    identities = await database.get_import_identities()
    existing_tokens = {_token_digest(rt) for _, rt in identities if rt}
    existing_emails = {email.casefold() for email, _ in identities}
    results = {}
    pending = []
    seen = {}
    for candidate in candidates:
        if candidate.error_code:
            results[candidate.item] = _failure(candidate.item, candidate.error_code, candidate.error)
            continue
        digest = _token_digest(candidate.refresh_token)
        if digest in seen or digest in existing_tokens:
            results[candidate.item] = {"item": candidate.item, "status": "skipped", "error_code": "duplicate_token",
                "error": f"与第 {seen[digest]} 项重复" if digest in seen else "该 RT 已存在"}
            continue
        seen[digest] = candidate.item
        pending.append(candidate)

    semaphore = asyncio.Semaphore(IMPORT_CONCURRENCY)

    async def prepare(candidate):
        async with semaphore:
            try:
                async with asyncio.timeout(IMPORT_ITEM_TIMEOUT_SECONDS):
                    return candidate, await prepare_credential(candidate.refresh_token), None
            except ImportFailure as error:
                return candidate, None, _failure(candidate.item, error.code, error.message)
            except TimeoutError:
                return candidate, None, _failure(candidate.item, "import_timeout", "验证超时，请稍后重试")
            except Exception as error:
                log.warning(f"RT import verification failed: item={candidate.item}, type={type(error).__name__}")
                return candidate, None, _failure(candidate.item, "validation_failed", "验证失败，请检查网络或稍后重试")

    try:
        async with asyncio.timeout(IMPORT_BATCH_TIMEOUT_SECONDS):
            prepared = await asyncio.gather(*(prepare(c) for c in pending))
    except TimeoutError:
        prepared = [(c, None, _failure(c.item, "batch_timeout", "本批验证超过 5 分钟，本批新凭证均未保存；请缩小批次重试")) for c in pending]

    for candidate, row, error in prepared:
        if error:
            results[candidate.item] = error
            continue
        email, credential = row
        if email in existing_emails:
            results[candidate.item] = {"item": candidate.item, "email": email, "status": "skipped",
                "error_code": "email_already_exists", "error": "该 Google 账号已存在，保留原凭证"}
            continue
        try:
            inserted = await database.insert_imported_credential(email, credential)
        except Exception as error:
            log.error(f"RT import storage failed: item={candidate.item}, type={type(error).__name__}")
            results[candidate.item] = _failure(candidate.item, "storage_failed", "保存凭证失败，请稍后重试")
            continue
        existing_emails.add(email)
        results[candidate.item] = {"item": candidate.item, "email": email,
            "status": "success" if inserted else "skipped", "project_id": credential["project_id"], "tier": credential["tier"],
            "error": None if inserted else "该 Google 账号已存在，保留原凭证"}

    ordered = [results[c.item] for c in candidates]
    summary = {"total": len(ordered), "results": ordered}
    for key, status in [("imported_count", "success"), ("skipped_count", "skipped"), ("failed_count", "failed")]:
        summary[key] = sum(r["status"] == status for r in ordered)
    log.info(f"RT import completed: total={summary['total']}, success={summary['imported_count']}, skipped={summary['skipped_count']}, failed={summary['failed_count']}")
    return summary
