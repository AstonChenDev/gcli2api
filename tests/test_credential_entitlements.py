import asyncio
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
from src import credential_entitlements as ent
from src import credential_manager as manager
from src.credential_cooldown import CooldownStore
from src.storage.database import Database, CredentialEntitlement


class EntitlementTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {'CREDENTIALS_DIR': self.temp.name, 'POSTGRESQL_URI': ''}))
        self.db = Database()
        await self.db.initialize()
        self.addAsyncCleanup(self.db.close)
        self.email = 'test@example.invalid'
        self.cred = dict(access_token='synthetic', refresh_token='synthetic-refresh', client_id='client',
                         project_id='project', tier='ultra', note='keep note',
                         expiry=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
        await self.db.store_credential(self.email, self.cred)
        self.stack.enter_context(patch.object(ent, 'get_antigravity_api_url', AsyncMock(return_value='https://upstream.invalid')))
        self.stack.enter_context(patch.object(manager, 'get_database', AsyncMock(return_value=self.db)))
        self.post = self.stack.enter_context(patch.object(ent, 'post_async', AsyncMock(side_effect=self.response)))
        self.paid = 'free-tier'
        self.catalog = {'gemini-4-argon-disabled': {'disabled': True}, 'gemini-flash': {}}

    async def response(self, url, **kwargs):
        data = {'paidTier': {'id': self.paid}, 'currentTier': {'id': 'free-tier'}} if url.endswith('loadCodeAssist') else {'models': self.catalog}
        return httpx.Response(200, json=data, request=httpx.Request('POST', url))

    async def snapshot(self):
        async with self.db.session() as s:
            row = await s.get(CredentialEntitlement, self.email)
            return json.loads(row.snapshot)

    async def due(self):
        async with self.db.session() as s:
            row = await s.get(CredentialEntitlement, self.email)
            snapshot = json.loads(row.snapshot);snapshot['attempted_at'] = 0
            row.snapshot = json.dumps(snapshot);row.next_refresh = 0

    async def test_free_downgrade_disables_all_models_and_selection_paths(self):
        await self.db.set_model_cooldown(self.email, 'claude-test', 9999999999)
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        row = await self.db.get_credential(self.email)
        self.assertEqual((row['tier'], row['note']), ('free', 'keep note'))
        self.assertIsNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))
        self.assertIsNone(await self.db.get_next_available_credential('gemini-flash'))
        self.assertIsNone(await self.db.get_available_credential_by_email(self.email, 'claude-test'))
        for mode in ('random', 'cache', 'sticky'):
            manager._sticky_email = self.email
            with patch.object(manager, 'get_credential_select_mode', AsyncMock(return_value=mode)), patch.object(
                manager.session_cache, 'get_session_binding', AsyncMock(return_value=self.email)
            ), patch.object(manager.session_cache, 'delete_session_binding', AsyncMock()):
                self.assertIsNone(await manager.get_valid_credential('gemini-4-argon-medium', 'session'))
        key, _ = await CooldownStore(self.db).admit(self.email, 'gemini-flash', 'prefetched')
        self.assertIsNone(key)
        async with self.db.session() as s:
            stored = await s.get(ent.AntigravityCredential, 1)
            self.assertTrue(stored.disabled)
            self.assertEqual(json.loads(stored.model_cooldowns), {'claude_gpt': 9999999999})

    async def test_paid_tier_wins_and_upgrade_restores_model_without_enabling_account(self):
        await self.db.update_credential_state(self.email, {'disabled': True})
        await ent.refresh_one(self.email, self.db)
        await self.due()
        self.paid = 'g1-ultra-tier';self.catalog = {'gemini-4-argon-medium': {}}
        await ent.refresh_one(self.email, self.db)
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'ultra')
        self.assertIn('gemini-4-argon-medium', (await self.snapshot())['models'])
        self.assertIsNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))
        await self.db.update_credential_state(self.email, {'disabled': False})
        self.assertIsNotNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))

    async def test_failed_refresh_keeps_last_known_permissions_and_tier(self):
        await ent.refresh_one(self.email, self.db)
        await self.due()
        self.post.side_effect = httpx.ReadTimeout('synthetic')
        self.assertFalse(await ent.refresh_one(self.email, self.db))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'free')
        self.assertEqual((await self.snapshot())['models'], ['gemini-flash'])
        self.assertIsNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))

    async def test_unknown_and_malformed_results_are_not_free_or_empty_permissions(self):
        self.paid = 'new-unknown-tier';self.catalog = {}
        self.assertFalse(await ent.refresh_one(self.email, self.db))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'ultra')
        self.assertNotIn('models', await self.snapshot())
        self.assertIsNotNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))

    async def test_workers_share_lease_and_404_hints_are_rate_limited(self):
        other = Database();await other.initialize();self.addAsyncCleanup(other.close)
        results = await asyncio.gather(ent.refresh_one(self.email, self.db), ent.refresh_one(self.email, other))
        self.assertEqual(sum(results), 1);self.assertEqual(self.post.await_count, 2)
        for _ in range(5):
            await ent.queue_refresh(self.email, self.db)
            self.assertFalse(await ent.refresh_one(self.email, other))
        self.assertEqual(self.post.await_count, 2)
        await self.due();self.assertTrue(await ent.refresh_one(self.email, other))

    async def test_delete_and_reimport_fences_old_network_response(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def delayed(**kwargs):
            started.set();await release.wait();return await self.response(**kwargs)
        self.post.side_effect = delayed
        task = asyncio.create_task(ent.refresh_one(self.email, self.db))
        await started.wait()
        await self.db.delete_credential(self.email)
        await self.db.store_credential(self.email, {**self.cred, 'tier': 'pro'})
        release.set();self.assertFalse(await task)
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'pro')

    async def test_token_renewal_does_not_restore_stale_ultra_tier(self):
        await ent.refresh_one(self.email, self.db)
        async def token_refresh(creds):
            creds.access_token = 'new-synthetic-token'
            creds.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch.object(manager.Credentials, 'refresh', token_refresh):
            self.assertIsNotNone(await manager.refresh_token(dict(self.cred), self.email))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'free')

    async def test_catalog_and_tier_refresh_independently(self):
        async def partial(**kwargs):
            if kwargs['url'].endswith('loadCodeAssist'):
                raise httpx.ReadTimeout('synthetic')
            return await self.response(**kwargs)
        self.post.side_effect = partial
        self.assertFalse(await ent.refresh_one(self.email, self.db))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'ultra')
        self.assertIsNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))

    async def test_known_supported_account_is_preferred_to_unknown(self):
        self.paid = 'g1-ultra-tier'
        await ent.refresh_one(self.email, self.db)
        await self.db.store_credential('unknown@example.invalid', self.cred)
        for _ in range(5):
            result = await self.db.get_next_available_credential('gemini-flash')
            self.assertEqual(result[0], self.email)

    async def test_replacement_clears_snapshot_but_token_update_does_not(self):
        self.paid = 'g1-ultra-tier'
        await ent.refresh_one(self.email, self.db)
        await self.db.update_access_token(self.email, 'synthetic-refresh', 'new', self.cred['expiry'])
        self.assertIn('models', await self.snapshot())
        await self.db.store_credential(self.email, {**self.cred, 'refresh_token': 'replacement'})
        self.assertIsNotNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))

    async def test_due_worker_refreshes_disabled_accounts_without_reenabling_them(self):
        await self.db.update_credential_state(self.email, {'disabled': True})
        self.assertEqual(await ent.refresh_due(self.db), [True])
        self.assertEqual(await ent.refresh_due(self.db), [])
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'free')
        self.assertIsNone(await self.db.get_available_credential_by_email(self.email))

    async def test_401_refreshes_token_once_and_does_not_overwrite_notes(self):
        async def refresh(creds):
            creds.access_token = 'new-synthetic-token'
            creds.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        async def unauthorized(**kwargs):
            if kwargs['headers']['Authorization'] != 'Bearer new-synthetic-token':
                return httpx.Response(401, request=httpx.Request('POST', kwargs['url']))
            return await self.response(**kwargs)
        self.post.side_effect = unauthorized
        with patch.object(ent.Credentials, 'refresh', refresh):
            self.assertTrue(await ent.refresh_one(self.email, self.db))
        row = await self.db.get_credential(self.email)
        self.assertEqual((row['access_token'], row['note'], row['tier']), ('new-synthetic-token', 'keep note', 'free'))
        self.assertEqual(self.post.await_count, 3)

    async def test_confirmed_free_disables_even_if_catalog_request_fails(self):
        async def catalog_failure(**kwargs):
            if kwargs['url'].endswith('fetchAvailableModels'):
                raise httpx.ReadTimeout('synthetic')
            return await self.response(**kwargs)
        self.post.side_effect = catalog_failure
        self.assertFalse(await ent.refresh_one(self.email, self.db))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'free')
        self.assertIsNone(await self.db.get_available_credential_by_email(self.email))

    async def test_paid_membership_does_not_disable_when_current_tier_is_free(self):
        for paid, expected in [('g1-ultra-tier', 'ultra'), ('g1-ultra-lite-tier', 'ultra_lite'), ('g1-pro-tier', 'pro')]:
            self.paid = paid
            await ent.queue_refresh(self.email, self.db)
            await self.due()
            self.assertTrue(await ent.refresh_one(self.email, self.db))
            self.assertEqual((await self.db.get_credential(self.email))['tier'], expected)
            self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email))

    async def test_unknown_or_incomplete_paid_tier_does_not_fall_back_to_free(self):
        for paid in ({'name': 'Google AI Ultra'}, {'id': 'future-free-trial-tier'}, {'id': ''}, 'invalid'):
            async def malformed(**kwargs):
                if kwargs['url'].endswith('loadCodeAssist'):
                    return httpx.Response(200, json={'paidTier': paid, 'currentTier': {'id': 'free-tier'}},
                                          request=httpx.Request('POST', kwargs['url']))
                return await self.response(**kwargs)
            self.post.side_effect = malformed
            await ent.queue_refresh(self.email, self.db)
            await self.due()
            self.assertFalse(await ent.refresh_one(self.email, self.db))
            self.assertEqual((await self.db.get_credential(self.email))['tier'], 'ultra')
            self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email))

    async def test_metadata_http_errors_do_not_disable_or_change_membership(self):
        for status in (401, 404, 429, 500):
            async def failure(**kwargs):
                return httpx.Response(status, request=httpx.Request('POST', kwargs['url']))
            self.post.side_effect = failure
            await ent.queue_refresh(self.email, self.db)
            await self.due()
            with patch.object(ent.Credentials, 'refresh', AsyncMock()):
                self.assertFalse(await ent.refresh_one(self.email, self.db))
            self.assertEqual((await self.db.get_credential(self.email))['tier'], 'ultra')
            self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email))

    async def test_explicit_current_free_without_paid_tier_disables(self):
        async def no_paid(**kwargs):
            if kwargs['url'].endswith('loadCodeAssist'):
                return httpx.Response(200, json={'currentTier': {'id': 'free-tier'}},
                                      request=httpx.Request('POST', kwargs['url']))
            return await self.response(**kwargs)
        self.post.side_effect = no_paid
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        self.assertEqual((await self.db.get_credential(self.email))['tier'], 'free')
        self.assertIsNone(await self.db.get_available_credential_by_email(self.email))


if __name__ == '__main__':
    unittest.main()
