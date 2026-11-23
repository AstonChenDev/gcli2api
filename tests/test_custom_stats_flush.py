"""Custom panel queries must include completed calls still in the log queue."""
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src import usage_log
from src.panel import usage_logs
from src.storage.database import Database


class CustomStatsFlushTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        temporary = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.db = Database()
        self.db._engine = create_async_engine(f"sqlite+aiosqlite:///{temporary}/usage.db")
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as connection:
            await connection.run_sync(self.db._create_tables)
        self.addAsyncCleanup(self.db.close)
        self.stack.enter_context(patch.object(usage_log, "get_database", AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(usage_logs, "get_database", AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(usage_log.config, "get_usage_log_enabled", AsyncMock(return_value=True)))
        self.stack.enter_context(patch.object(usage_logs, "get_usage_log_enabled", AsyncMock(return_value=True)))
        # Queries, not the periodic worker, must make these records visible.
        self.stack.enter_context(patch.object(usage_log, "_FLUSH_INTERVAL", 3600))
        self.addAsyncCleanup(usage_log.close_usage_logs)

        app = FastAPI()
        app.include_router(usage_logs.router)
        app.dependency_overrides[usage_logs.verify_panel_token] = lambda: "synthetic"
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(self.client.aclose)

        base = {"model_name": "synthetic", "credential_email": "test@example.invalid"}
        await self.db.insert_usage_log({**base, "request_id": "retry", "success": False,
                                        "status_code": 429, "error_message": "old failure"})
        await usage_log.record_usage_log({**base, "request_id": "retry", "success": True, "status_code": 200})
        await usage_log.record_usage_log({**base, "request_id": "new", "success": False,
                                          "status_code": 429, "error_message": "new failure"})
        _, total = await self.db.query_usage_logs()
        self.assertEqual(total, 1)

    async def query(self, path, **params):
        response = await self.client.get(f"/usage-logs/{path}", params=params)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        return response.json()

    async def test_overall_stats_sees_retry_success_and_new_failure(self):
        result = await self.query("overall-stats")
        self.assertEqual(result["requests"]["summary"], {
            "total_requests": 2, "success_count": 1, "failure_count": 1, "success_rate": 50.0,
        })
        self.assertEqual(result["attempts"]["summary"]["total_requests"], 3)

    async def test_overall_errors_uses_final_retry_result(self):
        result = await self.query("overall-errors")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["error_message"], "new failure")

    async def test_lookup_finds_just_completed_request(self):
        result = await self.query("lookup", identifier="new")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["items"][0]["request_id"], "new")

    async def test_credential_stats_includes_pending_attempts(self):
        result = await self.query("credential-stats")
        self.assertEqual(result["summary"]["total_requests"], 3)
        self.assertEqual(result["summary"]["success_count"], 1)
        self.assertEqual(result["summary"]["failure_count"], 2)

    async def test_credential_errors_includes_new_failure(self):
        result = await self.query("credential-errors", credential_email="test@example.invalid", model_name="synthetic")
        self.assertEqual(result["total"], 2)
        self.assertEqual({row["error_message"] for row in result["items"]}, {"old failure", "new failure"})
