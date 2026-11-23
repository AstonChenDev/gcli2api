"""Log clearing must preserve the schema and concurrent rows/detail files."""
import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from fastapi import HTTPException

from src.panel import usage_logs
from src import request_body_log
from src.storage.database import Database


class LogClearConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'request_logs'
        self.root.mkdir()
        self.db = Database()
        self.db._engine = create_async_engine(f'sqlite+aiosqlite:///{self.tmp.name}/test.db')
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as conn:
            await conn.execute(text('PRAGMA journal_mode=WAL'))
            await conn.run_sync(self.db._create_tables)
        self.addAsyncCleanup(self.db.close)
        for patcher in (patch.object(usage_logs, 'get_database', AsyncMock(return_value=self.db)),
                        patch.object(request_body_log, 'REQUEST_LOG_DIR', str(self.root))):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def insert(self, request_id, timestamp=None):
        path = f'{request_id}.json'
        (self.root / path).write_text('{}')
        ok = await self.db.insert_usage_log({'request_id':request_id, 'model_name':'synthetic',
             'created_at':timestamp or time.time(), 'request_log_file':path, 'status_code':200, 'success':True})
        self.assertTrue(ok)

    async def schema(self):
        async with self.db.session() as s:
            return (await s.execute(text("SELECT type,name,sql FROM sqlite_master WHERE tbl_name='usage_log' ORDER BY type,name"))).all()

    async def test_truncate_keeps_concurrent_writer_and_its_detail_file(self):
        await self.insert('old', time.time() - 100)
        before = await self.schema()
        deleted, commit = asyncio.Event(), asyncio.Event()
        original = AsyncSession.execute
        async def pause_delete(session, statement, *args, **kwargs):
            result = await original(session, statement, *args, **kwargs)
            if getattr(statement, 'is_delete', False):
                deleted.set()
                await commit.wait()
            return result
        with patch.object(AsyncSession, 'execute', pause_delete):
            clearing = asyncio.create_task(usage_logs.truncate_logs(token='synthetic'))
            try:
                await asyncio.wait_for(deleted.wait(), 2)
                writer = asyncio.create_task(self.insert('new'))
                # A reader still sees a valid table/snapshot while DELETE holds
                # the write lock; the writer waits rather than losing its row.
                rows, total = await self.db.query_usage_logs()
                self.assertEqual(total, 1)
                commit.set()
                response, _ = await asyncio.wait_for(asyncio.gather(clearing, writer), 3)
            finally:
                commit.set()
                await clearing
        payload = json.loads(response.body)
        self.assertEqual(payload['deleted'], 1)
        self.assertEqual(payload['files_deleted'], 1)
        rows, total = await self.db.query_usage_logs()
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]['request_id'], 'new')
        self.assertTrue((self.root / 'new.json').exists())
        self.assertFalse((self.root / 'old.json').exists())
        self.assertEqual(before, await self.schema())

    async def test_new_completion_before_delete_and_unindexed_files_survive(self):
        await self.insert('old', time.time() - 100)
        (self.root / 'inflight.json').write_text('{}')
        original = self.db.delete_usage_logs_with_files
        async def concurrent(before_timestamp):
            await self.insert('new', before_timestamp + .01)
            return await original(before_timestamp)
        with patch.object(self.db, 'delete_usage_logs_with_files', concurrent):
            response = await usage_logs.truncate_logs(token='synthetic')
        self.assertEqual(json.loads(response.body)['deleted'], 1)
        self.assertTrue((self.root / 'new.json').exists())
        self.assertTrue((self.root / 'inflight.json').exists())
        rows, total = await self.db.query_usage_logs()
        self.assertEqual([r['request_id'] for r in rows], ['new'])

    async def test_clear_days_and_clear_zero_only_delete_matching_files(self):
        await self.insert('old', time.time() - 40 * 86400)
        await self.insert('recent', time.time() - 60)
        (self.root / 'inflight.json').write_text('{}')
        response = await usage_logs.clear_logs(token='synthetic', days=30)
        self.assertEqual(json.loads(response.body)['deleted'], 1)
        self.assertTrue((self.root / 'recent.json').exists())
        response = await usage_logs.clear_logs(token='synthetic', days=0)
        self.assertEqual(json.loads(response.body)['deleted'], 1)
        self.assertFalse((self.root / 'recent.json').exists())
        self.assertTrue((self.root / 'inflight.json').exists())
        response = await usage_logs.truncate_logs(token='synthetic')
        self.assertEqual(json.loads(response.body)['deleted'], 0)

    async def test_commit_failure_rolls_back_and_does_not_delete_files(self):
        await self.insert('old', time.time() - 60)
        with patch.object(AsyncSession, 'commit', AsyncMock(side_effect=RuntimeError('synthetic commit failure'))):
            with self.assertRaises(HTTPException):
                await usage_logs.truncate_logs(token='synthetic')
        rows, total = await self.db.query_usage_logs()
        self.assertEqual(total, 1)
        self.assertTrue((self.root / 'old.json').exists())

    async def test_file_deletion_rejects_traversal_and_does_not_remove_directory(self):
        outside = Path(self.tmp.name) / 'outside.json'
        outside.write_text('{}')
        (self.root / 'old.json').write_text('{}')
        deleted = request_body_log.delete_request_body_logs(['../outside.json', '', 'old.json', 'old.json', 'missing.json'])
        self.assertEqual(deleted, 1)
        self.assertTrue(outside.exists())
        self.assertTrue(self.root.is_dir())
