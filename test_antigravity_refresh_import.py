import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi import FastAPI

from src import antigravity_import
from src import google_oauth_api
from src.antigravity_import import (
    AntigravityImportInputError,
    MAX_IMPORT_ITEMS,
    extract_refresh_token_candidates,
    import_antigravity_refresh_tokens,
    parse_import_request_payload,
)
from src.panel import creds
from src.storage.sqlite_manager import SQLiteManager
from src.utils import verify_panel_token


def _refresh_token(number: int) -> str:
    return f"1//test-refresh-token-{number:04d}-abcdefghijklmnopqrstuvwxyz"


class FakeStorageAdapter:
    def __init__(self):
        self.credentials = {}
        self.states = {}
        self.store_calls = []
        self.state_calls = []
        self.store_result = True
        self.state_failures = 0

    async def list_credentials(self, mode="geminicli"):
        assert mode == "antigravity"
        return list(self.credentials)

    async def get_all_credential_states(self, mode="geminicli"):
        assert mode == "antigravity"
        return {name: dict(state) for name, state in self.states.items()}

    async def get_credential(self, filename, mode="geminicli"):
        assert mode == "antigravity"
        value = self.credentials.get(filename)
        return dict(value) if value else None

    async def store_credential(self, filename, credential_data, mode="geminicli"):
        assert mode == "antigravity"
        self.store_calls.append((filename, dict(credential_data), mode))
        if not self.store_result:
            return False
        self.credentials[filename] = dict(credential_data)
        self.states.setdefault(filename, {})
        return True

    async def update_credential_state(self, filename, updates, mode="geminicli"):
        assert mode == "antigravity"
        self.state_calls.append((filename, dict(updates), mode))
        if self.state_failures > 0:
            self.state_failures -= 1
            return False
        if filename not in self.credentials:
            return False
        self.states.setdefault(filename, {}).update(updates)
        return True

    async def delete_credential(self, filename, mode="geminicli"):
        assert mode == "antigravity"
        existed = filename in self.credentials
        self.credentials.pop(filename, None)
        self.states.pop(filename, None)
        return existed


class FakeCredentials:
    active = 0
    peak_active = 0
    refresh_calls = []

    def __init__(
        self,
        access_token,
        refresh_token=None,
        client_id=None,
        client_secret=None,
        expires_at=None,
        project_id=None,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.expires_at = expires_at
        self.project_id = project_id

    async def refresh(self, timeout=900.0):
        type(self).refresh_calls.append(self.refresh_token)
        assert timeout == 30.0
        assert self.client_id == antigravity_import.ANTIGRAVITY_CLIENT_ID
        assert self.client_secret == antigravity_import.ANTIGRAVITY_CLIENT_SECRET
        type(self).active += 1
        type(self).peak_active = max(type(self).peak_active, type(self).active)
        try:
            await antigravity_import.asyncio.sleep(0)
            if self.refresh_token.endswith("invalid"):
                error = RuntimeError("safe wrapper")
                error.oauth_error_code = "invalid_grant"
                raise error
            number = self.refresh_token.split("-")[-2]
            self.access_token = f"generated-access-token-{number}"
            self.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        finally:
            type(self).active -= 1


@pytest.fixture(autouse=True)
def fake_credentials(monkeypatch):
    FakeCredentials.active = 0
    FakeCredentials.peak_active = 0
    FakeCredentials.refresh_calls = []
    monkeypatch.setattr(antigravity_import, "Credentials", FakeCredentials)


async def fake_api_url():
    return "https://antigravity.invalid"


async def fake_project(**kwargs):
    assert kwargs["access_token"].startswith("generated-access-token-")
    assert kwargs["user_agent"] == antigravity_import.ANTIGRAVITY_USER_AGENT
    assert kwargs["include_credits"] is True
    number = kwargs["access_token"].rsplit("-", 1)[-1]
    return f"project-{number}", "pro", 50


async def fake_email(credentials):
    number = credentials.refresh_token.split("-")[-2]
    return f"User{number}@Example.com"


def test_parser_accepts_real_manager_top_level_array_and_wrapper():
    top_level = [
        {"email": "first@example.com", "refresh_token": _refresh_token(1)},
        {"email": "second@example.com", "refresh_token": _refresh_token(2)},
    ]
    wrapped = {"accounts": top_level}

    direct = parse_import_request_payload({"refresh_tokens": [], "manager_backup": top_level})
    raw_document = parse_import_request_payload(top_level)
    envelope = parse_import_request_payload({"refresh_tokens": [], "manager_backup": wrapped})
    raw_wrapper = parse_import_request_payload(wrapped)

    assert [item.refresh_token for item in direct] == [
        _refresh_token(1),
        _refresh_token(2),
    ]
    assert direct == envelope
    assert direct == raw_document
    assert direct == raw_wrapper


def test_parser_accepts_multiline_and_internal_manager_shape():
    candidates = extract_refresh_token_candidates(
        [f"{_refresh_token(1)}\r\n{_refresh_token(2)}"],
        [{"token": {"refresh_token": _refresh_token(3)}}],
    )
    assert [item.refresh_token for item in candidates] == [
        _refresh_token(1),
        _refresh_token(2),
        _refresh_token(3),
    ]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"refresh_tokens": "not-a-list"},
        {"manager_backup": {}},
        {"manager_backup": "not-a-backup"},
    ],
)
def test_parser_rejects_invalid_documents_without_pydantic_echo(payload):
    with pytest.raises(AntigravityImportInputError):
        parse_import_request_payload(payload)


def test_parser_bounds_batch_and_marks_invalid_items():
    candidates = extract_refresh_token_candidates(["", 123, "bad token"])
    assert [item.error_code for item in candidates] == [
        "missing_refresh_token",
        "missing_refresh_token",
        "invalid_refresh_token",
    ]

    with pytest.raises(AntigravityImportInputError, match="最多导入"):
        extract_refresh_token_candidates(
            [_refresh_token(index) for index in range(MAX_IMPORT_ITEMS + 1)]
        )


@pytest.mark.asyncio
async def test_oauth_refresh_exposes_only_safe_google_error_code(monkeypatch):
    async def oauth_base_url():
        return "https://oauth.invalid"

    async def rejected_post(url, data=None, headers=None, timeout=None, **kwargs):
        assert timeout == 12.0
        request = httpx.Request("POST", url)
        return httpx.Response(
            400,
            json={"error": "invalid_grant", "error_description": "sensitive detail"},
            request=request,
        )

    monkeypatch.setattr(google_oauth_api, "get_oauth_proxy_url", oauth_base_url)
    monkeypatch.setattr(google_oauth_api, "post_async", rejected_post)
    credentials = google_oauth_api.Credentials(
        access_token="",
        refresh_token=_refresh_token(1),
        client_id="test-client",
        client_secret="test-secret",
    )

    with pytest.raises(google_oauth_api.TokenError) as exc_info:
        await credentials.refresh(timeout=12.0)

    assert exc_info.value.status_code == 400
    assert exc_info.value.oauth_error_code == "invalid_grant"
    assert "sensitive detail" not in str(exc_info.value)
    assert _refresh_token(1) not in str(exc_info.value)


@pytest.mark.asyncio
async def test_oauth_transport_exception_cannot_leak_submitted_form(monkeypatch):
    sensitive_refresh = _refresh_token(1)
    captured_logs = []

    async def oauth_base_url():
        return "https://oauth.invalid"

    async def leaky_post(*args, **kwargs):
        raise RuntimeError(
            f"request data refresh={sensitive_refresh} "
            f"secret={antigravity_import.ANTIGRAVITY_CLIENT_SECRET}"
        )

    monkeypatch.setattr(google_oauth_api, "get_oauth_proxy_url", oauth_base_url)
    monkeypatch.setattr(google_oauth_api, "post_async", leaky_post)
    monkeypatch.setattr(google_oauth_api.log, "error", captured_logs.append)
    credentials = google_oauth_api.Credentials(
        access_token="",
        refresh_token=sensitive_refresh,
        client_id="test-client",
        client_secret=antigravity_import.ANTIGRAVITY_CLIENT_SECRET,
    )

    with pytest.raises(google_oauth_api.TokenError) as exc_info:
        await credentials.refresh(timeout=12.0)

    combined = "\n".join(captured_logs) + str(exc_info.value)
    assert sensitive_refresh not in combined
    assert antigravity_import.ANTIGRAVITY_CLIENT_SECRET not in combined


@pytest.mark.asyncio
async def test_success_builds_complete_credential_and_verified_state():
    adapter = FakeStorageAdapter()
    candidate = extract_refresh_token_candidates(
        [],
        [
            {
                "email": "attacker-controlled@example.net",
                "refresh_token": _refresh_token(1),
                "client_id": "untrusted-client",
                "client_secret": "untrusted-secret",
            }
        ],
    )

    result = await import_antigravity_refresh_tokens(
        candidate,
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )

    assert result["imported_count"] == 1
    assert result["failed_count"] == 0
    stored = adapter.store_calls[0][1]
    assert stored["client_id"] == antigravity_import.ANTIGRAVITY_CLIENT_ID
    assert stored["client_secret"] == antigravity_import.ANTIGRAVITY_CLIENT_SECRET
    assert stored["refresh_token"] == _refresh_token(1)
    assert stored["token"] == "generated-access-token-0001"
    assert stored["project_id"] == "project-0001"
    assert stored["scopes"] == antigravity_import.ANTIGRAVITY_SCOPES
    assert stored["token_uri"] == antigravity_import.TOKEN_URL
    assert stored["expiry"]
    assert adapter.state_calls[0][1]["user_email"] == "user0001@example.com"
    assert adapter.state_calls[0][1]["tier"] == "pro"
    assert adapter.state_calls[0][1]["disabled"] is False
    assert adapter.state_calls[0][1]["error_codes"] == []

    serialized_result = json.dumps(result)
    assert _refresh_token(1) not in serialized_result
    assert "generated-access-token" not in serialized_result
    assert antigravity_import.ANTIGRAVITY_CLIENT_SECRET not in serialized_result
    assert "attacker-controlled" not in serialized_result


@pytest.mark.asyncio
async def test_duplicate_tokens_are_idempotent_and_not_refreshed():
    adapter = FakeStorageAdapter()
    existing_name = "existing.json"
    adapter.credentials[existing_name] = {"refresh_token": _refresh_token(1)}
    adapter.states[existing_name] = {"user_email": "existing@example.com"}
    candidates = extract_refresh_token_candidates(
        [_refresh_token(1), _refresh_token(2), _refresh_token(2)]
    )

    result = await import_antigravity_refresh_tokens(
        candidates,
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )

    assert result["imported_count"] == 1
    assert result["skipped_count"] == 2
    assert FakeCredentials.refresh_calls == [_refresh_token(2)]
    assert len(adapter.credentials) == 2


@pytest.mark.asyncio
async def test_malformed_legacy_json_credential_does_not_block_import():
    class LegacyAdapter(FakeStorageAdapter):
        async def get_credential(self, filename, mode="geminicli"):
            assert mode == "antigravity"
            if filename == "bad-array.json":
                return ["legacy", "invalid", "shape"]
            return await super().get_credential(filename, mode=mode)

    adapter = LegacyAdapter()
    adapter.credentials["bad-array.json"] = {"placeholder": True}
    adapter.states["bad-array.json"] = {}
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["imported_count"] == 1
    assert len(adapter.credentials) == 2


@pytest.mark.asyncio
async def test_different_tokens_for_same_verified_email_store_once():
    adapter = FakeStorageAdapter()

    async def same_email(credentials):
        return "same@example.com"

    candidates = extract_refresh_token_candidates([_refresh_token(1), _refresh_token(2)])
    result = await import_antigravity_refresh_tokens(
        candidates,
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=same_email,
    )

    assert result["imported_count"] == 1
    assert result["skipped_count"] == 1
    assert result["results"][1]["error_code"] == "duplicate_email_in_request"
    assert len(adapter.credentials) == 1


@pytest.mark.asyncio
async def test_invalid_grant_and_missing_project_never_store():
    adapter = FakeStorageAdapter()
    invalid = f"{_refresh_token(1)}invalid"

    invalid_result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([invalid]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert invalid_result["results"][0]["error_code"] == "invalid_grant"

    async def missing_project(**kwargs):
        return None, "pro", 50

    missing_result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(2)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=missing_project,
        fetch_email=fake_email,
    )
    assert missing_result["results"][0]["error_code"] == "project_not_found"
    assert not adapter.credentials


@pytest.mark.asyncio
async def test_missing_verified_email_never_stores_or_uses_backup_email():
    adapter = FakeStorageAdapter()

    async def missing_email(credentials):
        return None

    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates(
            [],
            [
                {
                    "email": "untrusted-backup@example.com",
                    "refresh_token": _refresh_token(1),
                }
            ],
        ),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=missing_email,
    )
    assert result["failed_count"] == 1
    assert result["results"][0]["error_code"] == "email_not_found"
    assert "email" not in result["results"][0]
    assert not adapter.credentials


@pytest.mark.asyncio
async def test_oauth_errors_and_timeouts_are_safely_redacted(monkeypatch):
    sensitive_refresh = _refresh_token(1)

    class LeakyFailureCredentials(FakeCredentials):
        async def refresh(self, timeout=900.0):
            error = RuntimeError(
                f"upstream included {sensitive_refresh} generated-access-token "
                f"{antigravity_import.ANTIGRAVITY_CLIENT_SECRET}"
            )
            raise error

    monkeypatch.setattr(antigravity_import, "Credentials", LeakyFailureCredentials)
    failed = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([sensitive_refresh]),
        storage_adapter=FakeStorageAdapter(),
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    serialized = json.dumps(failed)
    assert failed["results"][0]["error_code"] == "oauth_exchange_failed"
    assert sensitive_refresh not in serialized
    assert "generated-access-token" not in serialized
    assert antigravity_import.ANTIGRAVITY_CLIENT_SECRET not in serialized

    class SlowCredentials(FakeCredentials):
        async def refresh(self, timeout=900.0):
            await antigravity_import.asyncio.sleep(0.05)

    monkeypatch.setattr(antigravity_import, "Credentials", SlowCredentials)
    monkeypatch.setattr(antigravity_import, "IMPORT_ITEM_TIMEOUT_SECONDS", 0.001)
    timed_out = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(2)]),
        storage_adapter=FakeStorageAdapter(),
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert timed_out["results"][0]["error_code"] == "import_timeout"


@pytest.mark.asyncio
async def test_failure_never_reflects_untrusted_manager_email():
    sensitive_value = _refresh_token(9)
    invalid = f"{_refresh_token(1)}invalid"
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates(
            [], [{"email": f"{sensitive_value}@example.com", "refresh_token": invalid}]
        ),
        storage_adapter=FakeStorageAdapter(),
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    serialized = json.dumps(result)
    assert result["results"][0]["error_code"] == "invalid_grant"
    assert sensitive_value not in serialized
    assert "email" not in result["results"][0]


@pytest.mark.asyncio
async def test_batch_deadline_cancels_preparation_without_storage(monkeypatch):
    class SlowCredentials(FakeCredentials):
        async def refresh(self, timeout=900.0):
            await antigravity_import.asyncio.sleep(0.05)

    adapter = FakeStorageAdapter()
    monkeypatch.setattr(antigravity_import, "Credentials", SlowCredentials)
    monkeypatch.setattr(antigravity_import, "IMPORT_ITEM_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(antigravity_import, "IMPORT_BATCH_TIMEOUT_SECONDS", 0.001)
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1), _refresh_token(2)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["failed_count"] == 2
    assert {item["error_code"] for item in result["results"]} == {"batch_timeout"}
    assert not adapter.credentials


@pytest.mark.asyncio
async def test_storage_failures_and_state_retry_are_reported_truthfully():
    adapter = FakeStorageAdapter()
    adapter.store_result = False
    failed = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert failed["failed_count"] == 1
    assert failed["results"][0]["error_code"] == "storage_failed"

    adapter = FakeStorageAdapter()
    adapter.state_failures = 2
    recovered = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(2)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert recovered["imported_count"] == 1
    assert len(adapter.state_calls) == 3

    adapter = FakeStorageAdapter()
    adapter.state_failures = 3
    partial = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(3)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert partial["partial_count"] == 1
    assert partial["results"][0]["status"] == "partial"
    assert len(adapter.credentials) == 1


@pytest.mark.asyncio
async def test_existing_verified_email_is_skipped_without_overwrite():
    adapter = FakeStorageAdapter()
    adapter.credentials["existing-account.json"] = {"refresh_token": _refresh_token(99)}
    adapter.states["existing-account.json"] = {"user_email": "USER0001@example.com"}
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["imported_count"] == 0
    assert result["skipped_count"] == 1
    assert result["results"][0]["error_code"] == "email_already_exists"
    assert list(adapter.credentials) == ["existing-account.json"]


@pytest.mark.asyncio
async def test_shared_project_id_does_not_merge_different_accounts():
    adapter = FakeStorageAdapter()
    adapter.credentials["existing-account.json"] = {
        "refresh_token": _refresh_token(99),
        "project_id": "project-0001",
    }
    adapter.states["existing-account.json"] = {"user_email": "other@example.com"}
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["imported_count"] == 1
    assert result["skipped_count"] == 0
    assert len(adapter.credentials) == 2

    async def shared_project(**kwargs):
        return "aicode-consumers", "pro", None

    adapter = FakeStorageAdapter()
    batch = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1), _refresh_token(2)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=shared_project,
        fetch_email=fake_email,
    )
    assert batch["imported_count"] == 2
    assert batch["skipped_count"] == 0
    assert len(adapter.credentials) == 2
    assert len({item["filename"] for item in batch["results"]}) == 2


@pytest.mark.asyncio
async def test_concurrent_store_race_is_treated_as_idempotent_skip():
    class RacingAdapter(FakeStorageAdapter):
        async def store_credential(self, filename, credential_data, mode="geminicli"):
            assert mode == "antigravity"
            self.credentials[filename] = dict(credential_data)
            self.states.setdefault(filename, {})
            return False

    adapter = RacingAdapter()
    result = await import_antigravity_refresh_tokens(
        extract_refresh_token_candidates([_refresh_token(1)]),
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["skipped_count"] == 1
    assert result["results"][0]["error_code"] == "already_exists"
    assert len(adapter.credentials) == 1


@pytest.mark.asyncio
async def test_import_persists_through_real_sqlite_backend(tmp_path, monkeypatch):
    monkeypatch.setenv("CREDENTIALS_DIR", str(tmp_path))
    adapter = SQLiteManager()
    await adapter.initialize()
    try:
        result = await import_antigravity_refresh_tokens(
            extract_refresh_token_candidates([_refresh_token(1)]),
            storage_adapter=adapter,
            get_api_url=fake_api_url,
            fetch_project=fake_project,
            fetch_email=fake_email,
        )
        assert result["imported_count"] == 1
        filename = result["results"][0]["filename"]
        stored = await adapter.get_credential(filename, mode="antigravity")
        state = await adapter.get_credential_state(filename, mode="antigravity")
        assert stored["refresh_token"] == _refresh_token(1)
        assert stored["project_id"] == "project-0001"
        assert state["user_email"] == "user0001@example.com"
        assert state["tier"] == "pro"
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_import_concurrency_is_bounded_and_result_order_is_stable():
    adapter = FakeStorageAdapter()
    candidates = extract_refresh_token_candidates([_refresh_token(index) for index in range(1, 9)])
    result = await import_antigravity_refresh_tokens(
        candidates,
        storage_adapter=adapter,
        get_api_url=fake_api_url,
        fetch_project=fake_project,
        fetch_email=fake_email,
    )
    assert result["imported_count"] == 8
    assert FakeCredentials.peak_active <= antigravity_import.IMPORT_CONCURRENCY
    assert [item["item"] for item in result["results"]] == list(range(1, 9))


@pytest.mark.asyncio
async def test_http_route_requires_auth_bounds_body_and_hides_tokens(monkeypatch):
    app = FastAPI()
    app.include_router(creds.router)
    transport = httpx.ASGITransport(app=app)

    async def panel_password():
        return "correct-panel-token"

    monkeypatch.setattr("src.utils.get_panel_password", panel_password)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        unauthorized = await client.post(
            "/creds/import-antigravity-refresh-tokens",
            json={"refresh_tokens": [_refresh_token(1)]},
        )
        wrong_password = await client.post(
            "/creds/import-antigravity-refresh-tokens",
            json={"refresh_tokens": [_refresh_token(1)]},
            headers={"authorization": "Bearer wrong-panel-token"},
        )
    assert unauthorized.status_code in {401, 403}
    assert wrong_password.status_code == 401

    app.dependency_overrides[verify_panel_token] = lambda: "panel-token"

    async def fake_import(candidates):
        return {
            "total": len(candidates),
            "imported_count": 1,
            "partial_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "results": [{"item": 1, "status": "success"}],
        }

    monkeypatch.setattr(creds, "import_antigravity_refresh_tokens", fake_import)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/creds/import-antigravity-refresh-tokens",
            json={"refresh_tokens": [_refresh_token(1)]},
        )
        too_large = await client.post(
            "/creds/import-antigravity-refresh-tokens",
            content=b"{}",
            headers={"content-length": str(1024 * 1024 + 1)},
        )

        async def oversized_chunks():
            yield b'{"refresh_tokens":["'
            yield b"x" * (1024 * 1024)
            yield b'"]}'

        streamed_too_large = await client.post(
            "/creds/import-antigravity-refresh-tokens",
            content=oversized_chunks(),
        )

    assert response.status_code == 200
    assert _refresh_token(1) not in response.text
    assert too_large.status_code == 413
    assert streamed_too_large.status_code == 413


def test_desktop_and_mobile_pages_expose_one_shared_import_ui():
    from pathlib import Path

    expected_ids = [
        "antigravityRefreshTokensInput",
        "antigravityManagerBackupInput",
        "antigravityManagerBackupName",
        "antigravityRefreshImportBtn",
        "antigravityRefreshImportResults",
    ]
    for page in ["front/control_panel.html", "front/control_panel_mobile.html"]:
        html = Path(page).read_text(encoding="utf-8")
        for element_id in expected_ids:
            assert html.count(f'id="{element_id}"') == 1
        assert "importAntigravityRefreshCredentials()" in html
        assert "common.js?v=20260915-selected-download" in html
