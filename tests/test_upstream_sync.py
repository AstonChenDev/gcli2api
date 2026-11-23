"""Upstream optimizations must preserve in-flight streams and local accounting."""
import asyncio
import copy
import json
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from src import usage_log
from src.api import antigravity
from src.panel import creds, usage_logs


class UsageQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.db = AsyncMock()
        self.stack.enter_context(patch.object(usage_log, "get_database", AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(usage_log.config, "get_usage_log_enabled", AsyncMock(return_value=True)))
        self.addAsyncCleanup(usage_log.close_usage_logs)

    async def test_queue_retains_completion_timestamp_and_shutdown_drains(self):
        await usage_log.record_usage_log({"request_id": "one", "created_at": 123.0})
        self.db.insert_usage_log.assert_not_awaited()
        await usage_log.close_usage_logs()
        self.db.insert_usage_log.assert_awaited_once()
        self.assertEqual(self.db.insert_usage_log.call_args.args[0]["created_at"], 123.0)

    async def test_truncate_waits_for_detached_batch_before_deleting(self):
        started, release = asyncio.Event(), asyncio.Event()
        order = []
        async def insert(entry):
            started.set()
            await release.wait()
            order.append("insert")
        async def delete(cutoff):
            order.append("delete")
            return 1, []
        self.db.insert_usage_log.side_effect = insert
        self.db.delete_usage_logs_with_files.side_effect = delete
        await usage_log.record_usage_log({"request_id": "old"})
        writer = asyncio.create_task(usage_log.flush_usage_logs())
        await started.wait()
        with patch.object(usage_logs, "get_database", AsyncMock(return_value=self.db)):
            clearing = asyncio.create_task(usage_logs.truncate_logs(token="test"))
            await asyncio.sleep(0)
            self.db.delete_usage_logs_with_files.assert_not_awaited()
            release.set()
            await writer
            response = await clearing
        self.assertEqual(order, ["insert", "delete"])
        self.assertEqual(json.loads(response.body)["deleted"], 1)

    async def test_full_buffer_applies_backpressure_without_dropping_logs(self):
        with patch.object(usage_log, "_MAX_PENDING", 2):
            for i in range(5):
                await usage_log.record_usage_log({"request_id": str(i)})
            await usage_log.close_usage_logs()
        self.assertEqual([c.args[0]["request_id"] for c in self.db.insert_usage_log.call_args_list], list("01234"))


class UpstreamCredentialTests(unittest.IsolatedAsyncioTestCase):
    async def test_import_reuses_valid_access_token_and_preserves_note(self):
        data = {"email": "test@example.invalid", "refresh_token": "test-refresh", "access_token": "test-access",
                "expiry": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                "tier": "ultra", "project_id": "test-project", "note": "saved note"}
        with patch.object(creds.Credentials, "refresh", AsyncMock()) as refresh, patch.object(creds, "add_credential", AsyncMock()) as save:
            result = await creds._import_single_credential(data)
        self.assertEqual(result["status"], "success")
        refresh.assert_not_awaited()
        saved = save.call_args.args[1]
        self.assertEqual((saved["access_token"], saved["note"]), ("test-access", "saved note"))

    async def test_import_refreshes_expired_or_missing_expiry(self):
        for expiry in ("", "2000-01-01T00:00:00+00:00", "invalid"):
            data = {"email": "test@example.invalid", "refresh_token": "test-refresh", "access_token": "stale-access",
                    "expiry": expiry, "tier": "ultra", "project_id": "test-project"}
            async def refresh(instance):
                instance.access_token = "new-access"
                instance.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
            with patch.object(creds.Credentials, "refresh", refresh), patch.object(creds, "add_credential", AsyncMock()) as save:
                result = await creds._import_single_credential(data)
            self.assertEqual(result["status"], "success")
            self.assertEqual(save.call_args.args[1]["access_token"], "new-access")

    async def test_safety_settings_forwarded_without_mutating_request(self):
        request = {"contents": [], "safetySettings": [{"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"}],
                   "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}}}
        original = copy.deepcopy(request)
        payload, _ = await antigravity.wrap_cli_request(request, "test-model", "test-project")
        self.assertEqual(payload["request"]["safetySettings"], original["safetySettings"])
        self.assertEqual(request, original)
