"""Upstream optimizations must preserve in-flight streams and local accounting."""
import asyncio
import copy
import json
import os
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from src import httpx_client, usage_log
from src.api import antigravity
from src.panel import creds, usage_logs


class SharedClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addAsyncCleanup(httpx_client.close_shared_clients)
        self.stack.enter_context(patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}))
        self.proxy = self.stack.enter_context(patch.object(httpx_client, "get_proxy_config", AsyncMock(return_value="")))
        self.stack.enter_context(patch.object(httpx_client, "get_stream_handshake_timeout_enabled", AsyncMock(return_value=True)))
        self.deadline = self.stack.enter_context(patch.object(httpx_client, "get_stream_handshake_timeout_seconds", AsyncMock(return_value=40)))

    async def test_reuses_real_tcp_connection_and_closes_at_shutdown(self):
        connections, tasks = [], set()
        async def serve(reader, writer):
            tasks.add(asyncio.current_task())
            connections.append(writer)
            try:
                for _ in range(2):
                    await reader.readuntil(b"\r\n\r\n")
                    writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                    await writer.drain()
                await reader.read()
            except asyncio.IncompleteReadError:
                pass
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        try:
            url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"
            for _ in range(2):
                async with httpx_client.get_client() as client:
                    self.assertEqual((await client.get(url)).text, "OK")
            self.assertEqual(len(connections), 1)
            self.assertFalse(client.is_closed)
            self.proxy.return_value = "not-a-valid-proxy"
            with self.assertRaises(ValueError):
                async with httpx_client.get_client():
                    pass
            self.proxy.return_value = ""
            async with httpx_client.get_client() as recovered:
                self.assertIs(recovered, client)
            await httpx_client.close_shared_clients()
            self.assertTrue(client.is_closed)
            await asyncio.wait_for(asyncio.gather(*tasks), 2)
        finally:
            server.close()
            await server.wait_closed()
            for writer in connections:
                writer.close()

    async def test_proxy_rotation_waits_for_all_active_leases(self):
        async with httpx_client.get_streaming_client() as old:
            async with httpx_client.get_streaming_client() as same:
                self.assertIs(old, same)
                self.proxy.return_value = "http://127.0.0.1:1"
                async with httpx_client.get_streaming_client() as new:
                    self.assertIsNot(old, new)
                    self.assertFalse(old.is_closed)
                self.assertFalse(old.is_closed)
            self.assertFalse(old.is_closed)
        self.assertTrue(old.is_closed)

    async def test_invalid_proxy_keeps_active_stream_managed_until_release(self):
        async with httpx_client.get_streaming_client() as old:
            self.proxy.return_value = "not-a-valid-proxy"
            with self.assertRaises(ValueError):
                async with httpx_client.get_streaming_client():
                    pass
            await httpx_client.close_shared_clients()
            self.assertFalse(old.is_closed)
        self.assertTrue(old.is_closed)

    async def test_handshake_hot_reload_and_shutdown_preserve_active_client(self):
        async with httpx_client.get_streaming_client() as old:
            self.deadline.return_value = 15
            async with httpx_client.get_streaming_client() as new:
                self.assertEqual((old.handshake_timeout, new.handshake_timeout), (40, 15))
                self.assertIsNone(new.timeout.read)
                await httpx_client.close_shared_clients()
                self.assertFalse(new.is_closed)
                self.assertFalse(old.is_closed)
            self.assertTrue(new.is_closed)
        self.assertTrue(old.is_closed)


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
