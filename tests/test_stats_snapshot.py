"""Statistics must describe one snapshot while completed requests keep arriving."""
import tempfile
import unittest
from unittest.mock import patch
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from src.storage.database import Database, UsageLog, AntigravityCredential


class StatisticsSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database()
        self.db._engine = create_async_engine(f'sqlite+aiosqlite:///{self.tmp.name}/test.db')
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as conn:
            await conn.execute(text('PRAGMA journal_mode=WAL'))
            await conn.run_sync(UsageLog.__table__.create)
            await conn.run_sync(AntigravityCredential.__table__.create)

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def test_concurrent_insert_and_cleanup_do_not_mix_statistics_snapshots(self):
        for method in ('credential_usage_stats', 'credential_usage_errors', 'overall_usage_stats', 'overall_usage_errors'):
            for change in ('insert', 'delete'):
                with self.subTest(method=method, change=change):
                    async with self.db.session() as session:
                        await session.execute(delete(UsageLog))
                        session.add(UsageLog(request_id='first', credential_email='first@example.invalid', model_name='medium',
                            status_code=400, success=False, error_message='first error', created_at=100))
                    changed = False

                    async def mutate_after_read(original, session, statement, *args, **kwargs):
                        nonlocal changed
                        result = await original(session, statement, *args, **kwargs)
                        if not changed and getattr(statement, 'is_select', False):
                            changed = True
                            async with self.db.session() as writer:
                                if change == 'delete':
                                    await writer.execute(delete(UsageLog))
                                else:
                                    writer.add(UsageLog(request_id='second', credential_email='second@example.invalid', model_name='medium',
                                        status_code=500, success=False, error_message='second error', created_at=101))
                        return result

                    execute, scalar = AsyncSession.execute, AsyncSession.scalar
                    async def wrapped_execute(session, statement, *args, **kwargs):
                        return await mutate_after_read(execute, session, statement, *args, **kwargs)
                    async def wrapped_scalar(session, statement, *args, **kwargs):
                        return await mutate_after_read(scalar, session, statement, *args, **kwargs)
                    with patch.object(AsyncSession, 'execute', wrapped_execute), patch.object(AsyncSession, 'scalar', wrapped_scalar):
                        result = await getattr(self.db, method)()
                    self.assertTrue(changed)
                    if method == 'overall_usage_stats':
                        self.assertEqual(result['requests']['summary'], result['attempts']['summary'])
                        self.assertEqual(result['requests']['summary']['total_requests'], 1)
                    else:
                        self.assertEqual(result['total'], 1)
                        self.assertEqual(len(result['items']), 1)
                        if method == 'credential_usage_stats':
                            self.assertEqual(result['summary']['total_requests'], 1)
                            self.assertEqual(result['items'][0]['errors'], [{'status_code':400, 'count':1}])
                    # A subsequent request must see the committed writer, not a cached snapshot.
                    latest = await self.db.overall_usage_stats()
                    self.assertEqual(latest['attempts']['summary']['total_requests'], 2 if change == 'insert' else 0)
