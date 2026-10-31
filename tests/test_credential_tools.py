"""Isolated API regressions: no production database or upstream requests."""
import io
import json
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from src.panel import creds


class CredentialToolsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FastAPI()
        self.app.include_router(creds.router)
        self.app.dependency_overrides[creds.verify_panel_token] = lambda: "test-only"
        self.db = AsyncMock()
        self.rows = {
            email: dict(access_token="test-access", refresh_token="test-refresh", project_id="test-project",
                        tier="ultra", expiry=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
            for email in ["a@example.invalid", "b@example.invalid", "../odd@example.invalid"]
        }
        self.db.get_credential.side_effect = lambda email: self.rows.get(email)
        self.db_patch = patch.object(creds, "get_database", AsyncMock(return_value=self.db))
        self.db_patch.start()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()
        self.db_patch.stop()

    async def test_selected_zip_exact_subset_deduplicated_safe_filenames(self):
        selected = ["a@example.invalid", "../odd@example.invalid", "a@example.invalid"]
        response = await self.client.post("/creds/download-selected", json={"emails": selected})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["x-credential-count"], "2")
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            self.assertEqual(len(archive.namelist()), 2)
            self.assertTrue(all("/" not in name for name in archive.namelist()))
            records = [json.loads(archive.read(name)) for name in archive.namelist()]
        self.assertEqual({row["email"] for row in records}, set(selected))
        self.assertTrue(all(set(row) == {"email", "refresh_token", "project_id", "tier"} for row in records))
        self.db.store_credential.assert_not_called()

    async def test_missing_selection_fails_instead_of_partial_export(self):
        response = await self.client.post("/creds/download-selected", json={"emails": ["a@example.invalid", "missing"]})
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("application/zip", response.headers["content-type"])
        for emails in [[], ["a"] * 1001]:
            response = await self.client.post("/creds/download-selected", json={"emails": emails})
            self.assertEqual(response.status_code, 422)

    async def test_export_requires_panel_authentication(self):
        self.app.dependency_overrides.clear()
        with patch("src.utils.get_panel_password", AsyncMock(return_value="test-panel")):
            response = await self.client.post("/creds/download-selected", json={"emails": ["a@example.invalid"]})
            self.assertIn(response.status_code, [401, 403])
            response = await self.client.post("/creds/download-selected", json={"emails": ["a@example.invalid"]}, headers={"Authorization": "Bearer wrong"})
            self.assertEqual(response.status_code, 401)
        self.db.get_credential.assert_not_called()

    async def test_model_and_error_status_preserved_without_disabling(self):
        with patch.object(creds, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")):
            for code in [200, 401, 403, 404, 429]:
                self.db.update_credential_state.reset_mock()
                upstream = httpx.Response(code, json={"response": {"candidates": [{"content": {"parts": [{"text": "OK"}]}, "finishReason": "STOP"}]}}) if code == 200 else httpx.Response(code, text="test response")
                with patch("src.httpx_client.post_async", AsyncMock(return_value=upstream)) as post:
                    response = await self.client.post("/creds/test/a@example.invalid", params={"model": "chosen-model"})
                self.assertEqual(response.status_code, code)
                self.assertEqual(response.json()["status_code"], code)
                self.assertEqual(post.call_args.kwargs["json"]["model"], "chosen-model")
                self.assertEqual(post.call_args.kwargs["timeout"], 30)
                for call in self.db.update_credential_state.call_args_list:
                    self.assertNotIn("disabled", call.args[1])

    async def test_expired_token_refreshes_using_real_signature_and_saves_iso(self):
        self.rows["a@example.invalid"]["expiry"] = "2020-01-01T00:00:00+00:00"
        async def refresh(instance):
            instance.access_token = "refreshed-test-access"
            instance.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        upstream = httpx.Response(200, json={"response": {"candidates": [{"content": {"parts": [{"text": "OK"}]}, "finishReason": "STOP"}]}})
        with patch.object(creds.Credentials, "refresh", refresh), patch.object(creds, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")), patch("src.httpx_client.post_async", AsyncMock(return_value=upstream)):
            response = await self.client.post("/creds/test/a@example.invalid", params={"model": "chosen-model"})
        self.assertEqual(response.status_code, 200)
        saved = self.db.store_credential.call_args.args[1]
        self.assertEqual(saved["access_token"], "refreshed-test-access")
        self.assertIsInstance(saved["expiry"], str)

    async def test_quota_groups_preserved_and_unexpired_token_does_not_write(self):
        groups = [{"displayName": "Argon", "buckets": [
            {"window": "FIVE_HOURS", "remainingFraction": 0.8},
            {"window": "WEEKLY", "remainingFraction": 0.2}]}]
        with patch.object(creds, "fetch_quota_info", AsyncMock(return_value={"success": True, "quotaGroups": groups})):
            response = await self.client.get("/creds/quota/a@example.invalid")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["quotaGroups"], groups)
        self.db.store_credential.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
