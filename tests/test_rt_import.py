import asyncio
import json
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src import antigravity_import as rt
from src.panel import creds
from src.storage.database import Database, AntigravityCredential


def credential(token="synthetic-new-token"):
    return dict(access_token="synthetic-access", refresh_token=token, client_id="client", client_secret="secret",
                project_id="shared-project", expiry="2030-01-01T00:00:00+00:00", tier="ultra")


class RTImportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = Database()
        self.db._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as connection:
            await connection.run_sync(AntigravityCredential.__table__.create)
        self.app = FastAPI()
        self.app.include_router(creds.router)
        self.app.dependency_overrides[creds.verify_panel_token] = lambda: "test-panel"
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")
        self.database_patch = patch.object(rt, "get_database", AsyncMock(return_value=self.db))
        self.database_patch.start()

    async def asyncTearDown(self):
        self.database_patch.stop()
        await self.client.aclose()
        await self.db.close()

    async def post(self, payload):
        return await self.client.post("/creds/import-antigravity-refresh-tokens", json=payload)

    async def test_supported_shapes(self):
        for payload in [
            {"refresh_tokens": ["a\nb"]}, ["a", "b"],
            [{"refresh_token": "a"}, {"refreshToken": "b"}],
            {"accounts": [{"token": {"refresh_token": "a"}}, {"token": {"refreshToken": "b"}}]},
        ]:
            self.assertEqual([c.refresh_token for c in rt.parse_import_request_payload(payload)], ["a", "b"])

    async def test_auth_required(self):
        self.app.dependency_overrides.clear()
        with patch("src.utils.get_panel_password", AsyncMock(return_value="test-panel")), patch.object(rt, "prepare_credential", AsyncMock()) as prepare:
            response = await self.post({"refresh_tokens": ["synthetic-sensitive"]})
            self.assertIn(response.status_code, (401, 403))
            response = await self.client.post("/creds/import-antigravity-refresh-tokens", json={}, headers={"Authorization": "Bearer wrong"})
            self.assertEqual(response.status_code, 401)
            prepare.assert_not_called()

    async def test_invalid_request_never_echoes_secrets(self):
        secret = "synthetic-sensitive"
        for payload in [secret, {"refresh_tokens": secret}, {"accounts": secret}, {"refresh_tokens": [secret] * 101}, {}]:
            response = await self.post(payload)
            self.assertEqual(response.status_code, 400)
            self.assertNotIn(secret, response.text)
        response = await self.client.post("/creds/import-antigravity-refresh-tokens", content='{"token":"synthetic-sensitive",')
        self.assertEqual(response.status_code, 400)
        self.assertNotIn(secret, response.text)
        response = await self.client.post("/creds/import-antigravity-refresh-tokens", content="x" * (rt.MAX_IMPORT_BODY_BYTES + 1))
        self.assertEqual(response.status_code, 413)

    async def test_mixed_import_and_duplicates(self):
        await self.db.store_credential("existing@example.invalid", credential("synthetic-existing"))
        async def prepare(token):
            if token == "synthetic-rejected":
                raise rt.ImportFailure("oauth_rejected", "RT 无效")
            return "verified@example.invalid", credential(token)
        payload = {"refresh_tokens": ["synthetic-existing", "synthetic-new", "synthetic-new", "bad token", "synthetic-rejected"]}
        with patch.object(rt, "prepare_credential", prepare):
            response = await self.post(payload)
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual((result["imported_count"], result["skipped_count"], result["failed_count"]), (1, 2, 2))
        self.assertEqual([r["item"] for r in result["results"]], [1, 2, 3, 4, 5])
        self.assertNotIn("synthetic-", response.text)
        self.assertEqual(len(await self.db.list_credentials()), 2)

    async def test_existing_account_preserves_credentials_and_state(self):
        await self.db.store_credential("verified@example.invalid", credential("old-token"))
        await self.db.update_credential_state("verified@example.invalid", {"disabled": True, "model_cooldowns": {"argon": 9999999999}, "error_codes": [403]})
        before = await self.db.get_credential("verified@example.invalid")
        with patch.object(rt, "prepare_credential", AsyncMock(return_value=("verified@example.invalid", credential()))):
            response = await self.post({"refresh_tokens": ["synthetic-new"]})
        self.assertEqual(response.json()["skipped_count"], 1)
        self.assertEqual(await self.db.get_credential("verified@example.invalid"), before)
        self.assertFalse(await self.db.insert_imported_credential("verified@example.invalid", credential()))
        self.assertEqual(await self.db.get_credential("verified@example.invalid"), before)

    async def test_same_email_in_batch_and_shared_project(self):
        async def prepare(token):
            return ("other@example.invalid" if token == "third" else "same@example.invalid"), credential(token)
        with patch.object(rt, "prepare_credential", prepare):
            response = await self.post({"refresh_tokens": ["first", "second", "third"]})
        self.assertEqual(response.json()["imported_count"], 2)
        self.assertEqual(response.json()["skipped_count"], 1)

    async def test_timeout_leaves_no_partial_credentials(self):
        async def slow(token):
            await asyncio.sleep(.1)
            return "test@example.invalid", credential()
        with patch.object(rt, "prepare_credential", slow), patch.object(rt, "IMPORT_BATCH_TIMEOUT_SECONDS", .01):
            response = await self.post({"refresh_tokens": ["first", "second"]})
        self.assertEqual(response.json()["failed_count"], 2)
        self.assertEqual(await self.db.list_credentials(), [])

    async def test_errors_do_not_leak_tokens_to_response_or_logs(self):
        with patch.object(rt, "prepare_credential", AsyncMock(side_effect=RuntimeError("synthetic-sensitive"))), patch.object(rt.log, "warning") as warning:
            response = await self.post({"refresh_tokens": ["synthetic-sensitive"]})
        self.assertEqual(response.json()["failed_count"], 1)
        self.assertNotIn("synthetic-sensitive", response.text + str(warning.call_args_list))

    async def test_oauth_pipeline_resolves_identity_project_and_tier(self):
        token_response = httpx.Response(200, json={"access_token": "synthetic-access", "expires_in": 3600})
        project_response = httpx.Response(200, json={"cloudaicompanionProject": {"id": "shared-project"}, "paidTier": {"id": "ultra-tier"}})
        user_response = httpx.Response(200, json={"email": "VERIFIED@example.invalid", "verified_email": True})
        with patch.object(rt, "post_async", AsyncMock(side_effect=[token_response, project_response])) as post, patch.object(rt, "get_async", AsyncMock(return_value=user_response)), patch.object(rt, "get_oauth_proxy_url", AsyncMock(return_value="https://oauth.invalid")), patch.object(rt, "get_googleapis_proxy_url", AsyncMock(return_value="https://google.invalid")), patch.object(rt, "get_antigravity_api_url", AsyncMock(return_value="https://ag.invalid")):
            response = await self.post({"manager_backup": [{"email": "forged@example.invalid", "refresh_token": "synthetic-sensitive"}]})
        result = response.json()
        self.assertEqual(result["imported_count"], 1)
        self.assertEqual(result["results"][0]["email"], "verified@example.invalid")
        self.assertEqual(result["results"][0]["tier"], "ultra")
        saved = await self.db.get_credential("verified@example.invalid")
        datetime.fromisoformat(saved["expiry"])
        self.assertNotIn("synthetic-sensitive", response.text)
        self.assertEqual(post.call_args_list[0].kwargs["timeout"], 30)

    async def test_oauth_and_project_rejection_never_persist(self):
        with patch.object(rt, "get_oauth_proxy_url", AsyncMock(return_value="https://oauth.invalid")), patch.object(rt, "post_async", AsyncMock(return_value=httpx.Response(400, text="synthetic-sensitive"))):
            response = await self.post({"refresh_tokens": ["synthetic-sensitive"]})
        self.assertEqual(response.json()["failed_count"], 1)
        self.assertNotIn("synthetic-sensitive", response.text)
        self.assertEqual(await self.db.list_credentials(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
