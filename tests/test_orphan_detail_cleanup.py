"""Orphan reclamation must not remove queued, concurrent or referenced files."""
import asyncio
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src import request_body_log as detail, usage_log
from src.api import antigravity as api
from src.panel import usage_logs
import test_audit_fixes


class OrphanDetailTests(unittest.IsolatedAsyncioTestCase):
    enqueue = test_audit_fixes.UsageCancellationTests.enqueue

    async def asyncSetUp(self):
        await test_audit_fixes.UsageCancellationTests.asyncSetUp(self)
        self.root = Path(self.tmp) / "request_logs"
        self.stack.enter_context(patch.object(detail, "REQUEST_LOG_DIR", str(self.root)))
        self.stack.enter_context(patch.object(usage_log, "_FLUSH_INTERVAL", 3600))

    async def body(self):
        return await detail.record_request_body(request_id="synthetic", model_name="test",
            upstream_model_name="test", credential_email="test@example.invalid", is_stream=True,
            status_code=200, url="https://example.invalid", headers={}, payload={"text": "test"})

    def old_file(self, name="old", age=60):
        ts = time.time() - age
        self.root.mkdir(exist_ok=True)
        path = f"{int(ts * 1000)}_{name}.json"
        (self.root / path).write_text("{}")
        os.utime(self.root / path, (ts, ts))
        return path

    async def test_historical_orphan_and_storage_failure_are_reclaimed(self):
        old = self.old_file()
        failed = await self.body()
        with patch.object(self.db, "insert_usage_log", AsyncMock(return_value=False)):
            await self.enqueue("failed", request_log_file=failed)
            await usage_log.flush_usage_logs()
        response = json.loads((await usage_logs.truncate_logs(token="test")).body)
        self.assertEqual(response["files_deleted"], 2)
        self.assertFalse((self.root / old).exists())
        self.assertFalse((self.root / failed).exists())

    async def test_disabled_usage_skips_detail_creation_and_releases_existing_file(self):
        path = await self.body()
        with patch.object(usage_log.config, "get_usage_log_enabled", AsyncMock(return_value=False)), \
             patch.object(api, "get_request_log_enabled", AsyncMock(return_value=True)):
            await self.enqueue("disabled", request_log_file=path)
            value = await api._record_request_body_detail(200, "", {}, url="", headers={},
                payload={}, response_body="", is_stream=False)
        self.assertEqual(value, "")
        self.assertEqual(json.loads((await usage_logs.truncate_logs(token="test")).body)["files_deleted"], 1)

    async def test_pending_file_is_preserved_until_late_index_commit(self):
        path = await self.body()
        response = await usage_logs.truncate_logs(token="test")
        self.assertEqual(json.loads(response.body)["files_deleted"], 0)
        self.assertTrue((self.root / path).exists())
        await self.enqueue("late", request_log_file=path)
        await usage_log.flush_usage_logs()
        # A retained row references an older file; file age alone cannot delete it.
        self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 0)
        self.assertTrue((self.root / path).exists())

    async def test_queue_handoff_survives_producer_task_exit(self):
        async def producer():
            path = await self.body()
            await self.enqueue("queued", request_log_file=path)
            return path
        path = await asyncio.create_task(producer())
        await asyncio.sleep(0)
        self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 0)
        await usage_log.flush_usage_logs()
        self.assertTrue((self.root / path).exists())

    async def test_abandoned_producer_releases_ownership(self):
        path = await asyncio.create_task(self.body())
        await asyncio.sleep(0)
        self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 1)
        self.assertFalse((self.root / path).exists())

    async def test_cancelled_write_keeps_lock_until_writer_finishes(self):
        started, release, written = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = asyncio.to_thread
        async def delayed(function, *args, **kwargs):
            if function.__name__ == "write":
                started.set()
                await release.wait()
                result = await original(function, *args, **kwargs)
                written.set()
                return result
            return await original(function, *args, **kwargs)
        with patch.object(detail.asyncio, "to_thread", delayed):
            producer = asyncio.create_task(self.body())
            try:
                await asyncio.wait_for(started.wait(), 2)
                producer.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await producer
                await asyncio.sleep(0)
                self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 0)
            finally:
                release.set()
                await asyncio.wait_for(written.wait(), 2)
                await asyncio.sleep(0)
                await asyncio.sleep(0)
        self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 1)

    async def test_clear_age_and_concurrent_new_file(self):
        old = self.old_file("old", age=40 * 86400)
        recent = self.old_file("recent")
        original = self.db.delete_usage_logs_with_files
        new = None
        async def concurrent(cutoff):
            nonlocal new
            new = self.old_file("new", age=-1)
            return await original(cutoff)
        with patch.object(self.db, "delete_usage_logs_with_files", concurrent):
            response = await usage_logs.clear_logs(token="test", days=30)
        self.assertEqual(json.loads(response.body)["files_deleted"], 1)
        self.assertFalse((self.root / old).exists())
        self.assertTrue((self.root / recent).exists())
        self.assertTrue((self.root / new).exists())

    async def test_other_process_file_lock_and_symlink_are_respected(self):
        path = self.old_file("locked", age=86400)
        code = "from src.request_body_log import _lock_file; import sys; f=open(sys.argv[1], 'r+b'); _lock_file(f); print('ready', flush=True); sys.stdin.read()"
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", code, str(self.root / path),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        try:
            self.assertEqual(await asyncio.wait_for(process.stdout.readline(), 5), b"ready\n")
            self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 0)
        finally:
            process.stdin.close()
            await asyncio.wait_for(process.wait(), 5)
        external = Path(self.tmp) / "external.json"
        external.write_text("{}")
        (self.root / "1_link.json").symlink_to(external)
        self.assertEqual(await detail.delete_orphan_request_body_logs(self.db, time.time()), 1)
        self.assertTrue(external.exists())
