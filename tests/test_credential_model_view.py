import json
import time
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src import credential_entitlements as ent
from src.panel import creds
from src.storage.database import Database, CredentialEntitlement


class CredentialModelViewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.db = Database()
        self.db._engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        self.addAsyncCleanup(self.db.close)
        async with self.db._engine.begin() as conn:
            await conn.run_sync(self.db._create_tables)
        self.email = 'models@example.invalid'
        await self.db.store_credential(self.email, {
            'access_token': 'secret-access', 'refresh_token': 'secret-refresh',
            'client_secret': 'secret-client', 'project_id': 'secret-project',
            'tier': 'ultra', 'note': '备注\n第二行',
            'expiry': (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        })
        self.app = FastAPI()
        self.app.include_router(creds.router)
        self.app.dependency_overrides[creds.verify_panel_token] = lambda: 'test'
        self.stack.enter_context(patch.object(creds, 'get_database', AsyncMock(return_value=self.db)))
        self.post = self.stack.enter_context(patch.object(ent, 'post_async', AsyncMock(side_effect=self.upstream)))
        self.stack.enter_context(patch.object(ent, 'get_antigravity_api_url', AsyncMock(return_value='https://upstream.invalid')))
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test')
        self.addAsyncCleanup(self.client.aclose)
        self.path = f'/creds/models/{self.email}'
        self.paid = 'g1-ultra-tier'
        self.catalog = {'gemini-4-hydrogen-medium': {}, 'gemini-4-hydrogen-disabled': {'disabled': True}}

    async def upstream(self, url, **kwargs):
        payload = {'paidTier': {'id': self.paid}} if url.endswith('loadCodeAssist') else {'models': self.catalog}
        return httpx.Response(200, json=payload, request=httpx.Request('POST', url))

    async def snapshot(self, data):
        async with self.db.session() as session:
            row = await session.get(CredentialEntitlement, self.email)
            if row is None:
                row = CredentialEntitlement(email=self.email)
                session.add(row)
            row.snapshot = json.dumps(data)
            row.next_refresh = 0

    async def test_get_distinguishes_unknown_from_confirmed_empty_and_never_queries(self):
        response = await self.client.get(self.path)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['models_known'])
        await self.snapshot({'models': [], 'models_checked_at': time.time()})
        result = (await self.client.get(self.path)).json()
        self.assertTrue(result['models_known'])
        self.assertEqual(result['models'], [])
        self.assertFalse(result['stale'])
        self.post.assert_not_awaited()

    async def test_disabled_account_preserves_note_and_cooldowns_without_leaking_secrets(self):
        now = time.time()
        await self.db.update_credential_state(self.email, {'disabled': True, 'model_cooldowns': {'gemini': now+300, 'argon': now-10}})
        await self.snapshot({'models': ['gemini-4-hydrogen-medium', 'gemini-4-argon-low'], 'models_checked_at': now-400, 'access_token': 'snapshot-secret', 'errors': ['models:ReadTimeout', 'auth:secret-error-detail']})
        before = await self.db.get_credential_state(self.email)
        response = await self.client.get(self.path)
        result = response.json()
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertTrue(result['disabled'])
        self.assertTrue(result['stale'])
        self.assertEqual(result['note'], '备注\n第二行')
        self.assertEqual(result['failed_checks'], ['auth', 'models'])
        models = {m['name']: m['quota_cooldown_until'] for m in result['models']}
        self.assertEqual(models['gemini-4-hydrogen-medium'], now+300)
        self.assertIsNone(models['gemini-4-argon-low'])
        for secret in ('secret-access', 'secret-refresh', 'secret-client', 'secret-project', 'snapshot-secret', 'secret-error-detail'):
            self.assertNotIn(secret, response.text)
        self.assertEqual(before, await self.db.get_credential_state(self.email))

    async def test_refresh_uses_entitlements_throttles_and_preserves_disabled(self):
        await self.db.update_credential_state(self.email, {'disabled': True})
        response = await self.client.post(self.path+'/refresh')
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()['refresh_deferred'])
        self.assertEqual(response.json()['models'], [{'name': 'gemini-4-hydrogen-medium', 'quota_cooldown_until': None}])
        self.assertTrue(response.json()['disabled'])
        self.assertEqual(self.post.await_count, 2)
        response = await self.client.post(self.path+'/refresh')
        self.assertTrue(response.json()['refresh_deferred'])
        self.assertGreater(response.json()['retry_after_seconds'], 0)
        self.assertEqual(self.post.await_count, 2)

    async def test_failed_catalog_keeps_last_known_list_and_success_time(self):
        checked = time.time()-400
        await self.snapshot({'models': ['old-model'], 'models_checked_at': checked})
        async def upstream(url, **kwargs):
            if url.endswith('fetchAvailableModels'):
                raise httpx.ReadTimeout('not-for-panel')
            return await self.upstream(url, **kwargs)
        self.post.side_effect = upstream
        result = (await self.client.post(self.path+'/refresh')).json()
        self.assertEqual(result['models'][0]['name'], 'old-model')
        self.assertEqual(result['checked_at'], checked)
        self.assertEqual(result['failed_checks'], ['models'])
        self.assertTrue(result['stale'])

    async def test_refresh_keeps_existing_free_auto_disable_rule(self):
        self.paid = 'free-tier'
        self.catalog = {'gemini-4-hydrogen-disabled': {'disabled': True}}
        result = (await self.client.post(self.path+'/refresh')).json()
        self.assertEqual(result['tier'], 'free')
        self.assertTrue(result['disabled'])
        self.assertTrue(result['models_known'])
        self.assertEqual(result['models'], [])
        self.assertEqual(result['note'], '备注\n第二行')

    async def test_missing_and_unauthorized_requests_do_not_fetch_upstream(self):
        for method, suffix in [('GET', ''), ('POST', '/refresh')]:
            response = await self.client.request(method, '/creds/models/missing'+suffix)
            self.assertEqual(response.status_code, 404)
        self.app.dependency_overrides.clear()
        with patch('src.utils.get_panel_password', AsyncMock(return_value='test-panel')):
            for method, suffix in [('GET', ''), ('POST', '/refresh')]:
                response = await self.client.request(method, self.path+suffix)
                self.assertIn(response.status_code, (401, 403))
        self.post.assert_not_awaited()

    async def test_malformed_snapshot_is_unknown_not_empty_permissions(self):
        for data in ([], {'models': [None]}, {'models': 'wrong', 'errors': 'wrong'}):
            await self.snapshot(data)
            result = (await self.client.get(self.path)).json()
            self.assertFalse(result['models_known'])
            self.assertTrue(result['stale'])
