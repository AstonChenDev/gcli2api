"""Cancellation must not lose/replay usage, and disabled retries must stay off."""
import asyncio
import json
import tempfile
import time
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import anyio
import httpx
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import test_custom_audit
from src import usage_log
from src.api import antigravity as api
from src.panel import usage_logs
from src.storage.database import Database


class RetrySwitchTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_network_retries_follow_switch_and_limit(self):
        for enabled in (False, True):
            for error in (httpx.ReadError, httpx.ConnectTimeout):
                with self.subTest(enabled=enabled, error=error):
                    self.saved.reset_mock()
                    with patch.object(api, 'get_retry_config', AsyncMock(return_value={
                        'retry_enabled': enabled, 'max_retries': 2, 'retry_interval': 0,
                    })), patch.object(api, 'post_async', AsyncMock(side_effect=error('synthetic'))) as upstream:
                        response = await api.non_stream_request({'model': 'gemini-test', 'request': {'contents': []}})
                    self.assertEqual(response.status_code, 500)
                    self.assertEqual(upstream.await_count, 3 if enabled else 1)
                    self.assertEqual(self.saved.await_count, upstream.await_count)

    async def test_disabled_retry_preserves_upstream_429(self):
        with patch.object(api, 'get_retry_config', AsyncMock(return_value={
            'retry_enabled': False, 'max_retries': 2, 'retry_interval': 0,
        })), patch.object(api, 'post_async', AsyncMock(return_value=httpx.Response(429, json={'error': 'rate limited'}))) as upstream, \
             patch.object(api, 'record_api_call_error', AsyncMock()), \
             patch.object(api, 'parse_and_log_cooldown', AsyncMock(return_value=None)):
            response = await api.non_stream_request({'model': 'gemini-test', 'request': {'contents': []}})
        self.assertEqual(response.status_code, 429)
        self.assertEqual(upstream.await_count, 1)
        self.assertEqual(self.saved.await_count, 1)


class UsageCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.db = Database()
        self.db._engine = create_async_engine(f'sqlite+aiosqlite:///{self.tmp}/test.db')
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as connection:
            await connection.run_sync(self.db._create_tables)
        self.addAsyncCleanup(self.db.close)
        self.stack.enter_context(patch.object(usage_log, 'get_database', AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(usage_logs, 'get_database', AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(usage_log.config, 'get_usage_log_enabled', AsyncMock(return_value=True)))
        self.addAsyncCleanup(usage_log.close_usage_logs)

    async def enqueue(self, name, **extra):
        await usage_log.record_usage_log({'request_id': name, 'model_name': 'synthetic', 'success': True, **extra})

    async def ids(self):
        rows, _ = await self.db.query_usage_logs()
        return sorted(row['request_id'] for row in rows)

    async def test_cancel_before_commit_and_after_commit_never_loses_or_duplicates(self):
        for after_commit in (False, True):
            with self.subTest(after_commit=after_commit):
                started, release = asyncio.Event(), asyncio.Event()
                original = self.db.insert_usage_log
                prefix = str(after_commit)
                async def delayed(entry):
                    if after_commit:
                        result = await original(entry)
                    started.set()
                    await release.wait()
                    return result if after_commit else await original(entry)
                with patch.object(self.db, 'insert_usage_log', delayed):
                    for i in range(3):
                        await self.enqueue(prefix + str(i))
                    waiter = asyncio.create_task(usage_log.flush_usage_logs())
                    try:
                        await asyncio.wait_for(started.wait(), 2)
                        waiter.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await waiter
                    finally:
                        release.set()
                    await usage_log.close_usage_logs()
                ids = await self.ids()
                self.assertEqual([name for name in ids if name.startswith(prefix)], [prefix + str(i) for i in range(3)])

    async def test_cancel_at_buffer_limit_retains_newly_accepted_record(self):
        started, release = asyncio.Event(), asyncio.Event()
        original = self.db.insert_usage_log
        async def delayed(entry):
            started.set()
            await release.wait()
            return await original(entry)
        with patch.object(usage_log, '_MAX_PENDING', 2), patch.object(self.db, 'insert_usage_log', delayed):
            await self.enqueue('one')
            second = asyncio.create_task(self.enqueue('two'))
            try:
                await asyncio.wait_for(started.wait(), 2)
                second.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await second
            finally:
                release.set()
            await usage_log.close_usage_logs()
        self.assertEqual(await self.ids(), ['one', 'two'])

    async def test_anyio_cancelled_waiter_does_not_cancel_writer(self):
        started, release = asyncio.Event(), asyncio.Event()
        original = self.db.insert_usage_log
        async def delayed(entry):
            started.set()
            await release.wait()
            return await original(entry)
        with patch.object(self.db, 'insert_usage_log', delayed):
            await self.enqueue('one')
            try:
                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(usage_log.flush_usage_logs)
                    await asyncio.wait_for(started.wait(), 2)
                    tasks.cancel_scope.cancel()
            finally:
                release.set()
            await usage_log.close_usage_logs()
        self.assertEqual(await self.ids(), ['one'])

    async def test_concurrent_flush_includes_records_queued_after_first_batch_started(self):
        started, release = asyncio.Event(), asyncio.Event()
        original = self.db.insert_usage_log
        async def delayed(entry):
            started.set()
            await release.wait()
            return await original(entry)
        with patch.object(self.db, 'insert_usage_log', delayed):
            await self.enqueue('one')
            first = asyncio.create_task(usage_log.flush_usage_logs())
            try:
                await asyncio.wait_for(started.wait(), 2)
                await self.enqueue('two')
                second = asyncio.create_task(usage_log.flush_usage_logs())
                await asyncio.sleep(0)
                self.assertFalse(second.done())
            finally:
                release.set()
            await asyncio.gather(first, second)
        self.assertEqual(await self.ids(), ['one', 'two'])

    async def test_clear_waits_for_writer_after_its_original_waiter_was_cancelled(self):
        started, release = asyncio.Event(), asyncio.Event()
        original = self.db.insert_usage_log
        async def delayed(entry):
            started.set()
            await release.wait()
            return await original(entry)
        with patch.object(self.db, 'insert_usage_log', delayed):
            await self.enqueue('old', created_at=time.time() - 60)
            waiter = asyncio.create_task(usage_log.flush_usage_logs())
            try:
                await asyncio.wait_for(started.wait(), 2)
                waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiter
                clearing = asyncio.create_task(usage_logs.truncate_logs(token='synthetic'))
                await asyncio.sleep(0)
                self.assertFalse(clearing.done())
                await self.enqueue('new', created_at=time.time() + 10)
            finally:
                release.set()
            response = await clearing
            await usage_log.close_usage_logs()
        self.assertEqual(json.loads(response.body)['deleted'], 1)
        self.assertEqual(await self.ids(), ['new'])
