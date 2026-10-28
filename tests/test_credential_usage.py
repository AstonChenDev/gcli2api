import unittest
from unittest.mock import AsyncMock, patch
import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.panel import usage_logs
from src.storage.database import Database, UsageLog, AntigravityCredential


class CredentialUsageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = Database()
        self.db._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as conn:
            await conn.run_sync(UsageLog.__table__.create)
            await conn.run_sync(AntigravityCredential.__table__.create)
        async with self.db.session() as s:
            for email, model, upstream, ok, code, message, timestamp in [
                ("a@example.invalid", "alias", "argon-medium", True, 200, "", 100),
                ("a@example.invalid", "argon-medium", "", True, 200, "", 101),
                ("a@example.invalid", "argon-medium", "argon-medium", False, 403, "PERMISSION_DENIED", 102),
                ("a@example.invalid", "argon-medium", "argon-medium", False, 429, "RESOURCE_EXHAUSTED", 103),
                ("a@example.invalid", "argon-medium", "argon-medium", False, 429, "RESOURCE_EXHAUSTED", 104),
                ("a@example.invalid", "argon-high", "", True, 200, "", 105),
                ("b@example.invalid", "argon-medium", "", False, 404, "MODEL_NOT_FOUND", 106),
                ("", "argon-medium", "", False, 500, "No credentials", 107),
                ("a@example.invalid", "argon-medium", "", False, 200, "Stream error", 108),
                ("a@example.invalid", "argon-medium", "", False, 0, "", 50),
            ]:
                s.add(UsageLog(credential_email=email, model_name=model, upstream_model_name=upstream,
                    success=ok, status_code=code, error_message=message, created_at=timestamp))
        self.app = FastAPI()
        self.app.include_router(usage_logs.router)
        self.app.dependency_overrides[usage_logs.verify_panel_token] = lambda: "test-panel"
        self.db_patch = patch.object(usage_logs, "get_database", AsyncMock(return_value=self.db)); self.db_patch.start()
        self.enabled_patch = patch.object(usage_logs, "get_usage_log_enabled", AsyncMock(return_value=True)); self.enabled_patch.start()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")

    async def asyncTearDown(self):
        self.db_patch.stop(); self.enabled_patch.stop()
        await self.client.aclose(); await self.db.close()

    async def test_groups_actual_model_and_exact_failure_counts(self):
        r = await self.client.get("/usage-logs/credential-stats", params={"start_timestamp": 100, "end_timestamp": 108})
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["total"], 4)
        self.assertEqual(data["summary"]["total_requests"], 9)
        self.assertEqual(data["summary"]["success_count"], 3)
        self.assertEqual(data["summary"]["failure_count"], 6)
        a = next(x for x in data["items"] if x["credential_email"] == "a@example.invalid" and x["model_name"] == "argon-medium")
        self.assertEqual((a["total_requests"], a["success_count"], a["failure_count"]), (6, 2, 4))
        self.assertEqual(a["errors"], [{"status_code": 200, "count": 1}, {"status_code": 403, "count": 1}, {"status_code": 429, "count": 2}])
        self.assertTrue(any(x["credential_email"] == "" for x in data["items"]))

    async def test_filter_pagination_and_summary_not_limited_to_page(self):
        data = await self.db.credential_usage_stats(limit=1, offset=0, start_timestamp=100, end_timestamp=108)
        second = await self.db.credential_usage_stats(limit=1, offset=1, start_timestamp=100, end_timestamp=108)
        self.assertEqual(data["summary"], second["summary"])
        self.assertNotEqual(data["items"], second["items"])
        filtered = await self.db.credential_usage_stats(model_name="argon-medium", credential_email="a@example.invalid", start_timestamp=102, end_timestamp=104)
        self.assertEqual(filtered["summary"]["total_requests"], 3)
        self.assertEqual(filtered["summary"]["failure_count"], 3)

    async def test_all_failure_messages_available_and_not_cross_account(self):
        params = {"credential_email": "a@example.invalid", "model_name": "argon-medium", "start_timestamp": 100, "limit": 1}
        r = await self.client.get("/usage-logs/credential-errors", params=params)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["total"], 3)
        self.assertEqual(r.json()["items"][0]["error_message"], "RESOURCE_EXHAUSTED")
        self.assertEqual(r.json()["items"][0]["count"], 2)
        all_errors = await self.db.credential_usage_errors(credential_email="a@example.invalid", model_name="argon-medium")
        self.assertEqual(all_errors["total"], 4)
        self.assertNotIn("MODEL_NOT_FOUND", str(all_errors))
        unassigned = await self.db.credential_usage_errors(credential_email="", model_name="argon-medium")
        self.assertEqual(unassigned["items"][0]["error_message"], "No credentials")

    async def test_empty_and_bad_ranges(self):
        empty = await self.db.credential_usage_stats(start_timestamp=1000)
        self.assertEqual(empty["total"], 0)
        self.assertEqual(empty["summary"]["failure_count"], 0)
        for params in [{"limit": 201}, {"offset": -1}, {"start_timestamp": 200, "end_timestamp": 100}]:
            r = await self.client.get("/usage-logs/credential-stats", params=params)
            self.assertIn(r.status_code, (400, 422))

    async def test_current_tier_changes_and_deleted_credentials_preserve_counts(self):
        baseline = await self.db.credential_usage_stats()
        for tier in ("ultra", "pro", "free", "unrecognized"):
            await self.db.store_credential("a@example.invalid", {"tier": tier})
            response = await self.client.get("/usage-logs/credential-stats")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["summary"], baseline["summary"])
            self.assertEqual(data["total"], baseline["total"])
            for row in data["items"]:
                expected = tier if row["credential_email"] == "a@example.invalid" and tier != "unrecognized" else None
                self.assertEqual(row["credential_tier"], expected)

    async def test_auth_and_disabled_logging_flag(self):
        with patch.object(usage_logs, "get_usage_log_enabled", AsyncMock(return_value=False)):
            r = await self.client.get("/usage-logs/credential-stats")
            self.assertFalse(r.json()["logging_enabled"])
        self.app.dependency_overrides.clear()
        for path in ["/usage-logs/credential-stats", "/usage-logs/credential-errors?credential_email=a&model_name=m"]:
            r = await self.client.get(path)
            self.assertIn(r.status_code, (401, 403))


if __name__ == "__main__":
    unittest.main(verbosity=2)
