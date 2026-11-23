import asyncio
import json
import os
import tempfile
import time
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from src import credential_entitlements as ent
from src import credential_manager as manager
from src import auth, credential_proxy
from src.api import antigravity as api, utils as api_utils
from src import google_oauth_api as oauth
from src.panel import creds as panel
from fastapi import HTTPException, Response
from src.credential_cooldown import CooldownStore, DispatchLimited, QuotaCooling
from src.credential_refresh import refresh_stored
from src.google_oauth_api import Credentials, TokenError
from src.storage.database import Database, CredentialEntitlement, PoolModelUnavailable


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

    async def test_selection_survives_confirmed_account_disabled_during_token_read(self):
        model = "gemini-flash"
        self.paid = "g1-ultra-tier"
        self.catalog = {model: {}}
        await ent.refresh_one(self.email, self.db)
        other = "other@example.invalid"
        await self.db.store_credential(other, self.cred)
        original_scalar = AsyncSession.scalar

        async def disable_before_read(session, stmt, **kwargs):
            await self.db.update_credential_state(self.email, {"disabled": True})
            return await original_scalar(session, stmt, **kwargs)

        with patch.object(AsyncSession, "scalar", disable_before_read):
            result = await self.db.get_next_available_credential(model, report_missing_model=True)
        self.assertEqual(result[0], other)

        await self.db.update_credential_state(self.email, {"disabled": False})
        await ent.mark_model_unavailable(other, model, self.cred["access_token"], self.db)
        with patch.object(AsyncSession, "scalar", disable_before_read), self.assertRaises(PoolModelUnavailable):
            await self.db.get_next_available_credential(model, report_missing_model=True)

    async def test_quota_refreshes_missing_access_token_with_future_expiry(self):
        await self.db.store_credential(self.email, dict(self.cred, access_token=""))
        upstream = httpx.Response(200, request=httpx.Request("POST", "https://oauth.invalid/token"),
                                  json={"access_token": "new-access", "expires_in": 3600, "refresh_token": None})
        with patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
            oauth, "get_oauth_proxy_url", AsyncMock(return_value="https://oauth.invalid")
        ), patch.object(oauth, "post_async", AsyncMock(return_value=upstream)) as exchange, patch.object(
            panel, "fetch_quota_info", AsyncMock(return_value={"success": True, "models": {}})
        ) as quota:
            result = await panel.get_credential_quota(self.email, token="test")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(exchange.await_count, 1)
        self.assertEqual(quota.await_args.args[0], "new-access")
        self.assertEqual((await self.db.get_credential(self.email))["refresh_token"], self.cred["refresh_token"])

    async def test_malformed_oauth_success_preserves_working_tokens(self):
        upstream = httpx.Response(200, request=httpx.Request("POST", "https://oauth.invalid/token"),
                                  json={"access_token": "  ", "expires_in": 3600, "refresh_token": "replacement"})
        with patch.object(oauth, "get_oauth_proxy_url", AsyncMock(return_value="https://oauth.invalid")), patch.object(
            oauth, "post_async", AsyncMock(return_value=upstream)
        ):
            self.assertIsNone(await manager.refresh_token(dict(self.cred), self.email))
        saved = await self.db.get_credential(self.email)
        self.assertEqual((saved["access_token"], saved["refresh_token"]),
                         (self.cred["access_token"], self.cred["refresh_token"]))
        self.assertFalse((await self.db.get_credential_state(self.email))["disabled"])

    async def test_panel_uses_upstream_failure_to_update_generation_routing(self):
        for status in (404, 429, 503):
            model = f"model-{status}"
            with self.subTest(status=status), patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
                panel, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
            ), patch.object(panel.httpx_client, "post_async", AsyncMock(return_value=httpx.Response(
                status, json={"error": {"code": status}}, headers={"Retry-After": "100"}
            ))) as post:
                result = await panel.test_credential(self.email, model, "test")
                self.assertEqual(result.status_code, status)
                post.assert_awaited_once()
                self.assertIsNone(await self.db.get_available_credential_by_email(self.email, model))
                self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email, "another-model"))
                if status in (429, 503):
                    self.assertIn(int(result.headers["Retry-After"]), range(98, 101))

    async def test_empty_pool_reports_missing_model_only_with_unanimous_evidence(self):
        model = "gemini-4-argon-medium"
        unknown = "unknown@example.invalid"

        async def select(**options):
            return await self.db.get_next_available_credential(model, report_missing_model=True, **options)

        self.assertEqual((await select())[0], self.email)
        await ent.mark_model_unavailable(self.email, model, self.cred["access_token"], self.db)
        with self.assertRaisesRegex(PoolModelUnavailable, model):
            await select(exclude_emails=[self.email])
        self.assertIsNone(await self.db.get_next_available_credential(model))
        await self.db.store_credential(unknown, dict(self.cred, access_token=""))
        self.assertEqual((await select())[0], unknown)
        self.assertIsNone(await select(require_ready_token=True))
        self.assertIsNone(await select(exclude_emails=[unknown]))
        await self.db.update_credential_state(unknown, {"disabled": True})
        with self.assertRaisesRegex(PoolModelUnavailable, model):
            await select(require_ready_token=True)
        await self.db.update_credential_state(self.email, {"disabled": True})
        self.assertIsNone(await select())

    async def test_catalog_refresh_after_selection_preserves_generation_404(self):
        model = "gemini-4-argon-medium"
        self.paid = "g1-ultra-tier"
        self.catalog = {model: {}}
        worker = Database()
        await worker.initialize()
        self.addAsyncCleanup(worker.close)
        select = self.db.get_next_available_credential

        async def refresh_after_selection(*args, **kwargs):
            try:
                return await select(*args, **kwargs)
            finally:
                self.assertTrue(await ent.refresh_one(self.email, worker))

        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                await ent.mark_model_unavailable(self.email, model, self.cred["access_token"], self.db)
                await self.due()
                body = {"model": model, "request": {"contents": []}}
                with patch.object(self.db, "get_next_available_credential", refresh_after_selection), patch.object(
                    api.usage_log, "record_usage_log", AsyncMock()
                ) as logged, patch.object(api, "_record_request_body_detail", AsyncMock(return_value="")):
                    if streaming:
                        response, = [chunk async for chunk in api.stream_request(body)]
                    else:
                        response = await api.non_stream_request(body)
                self.assertEqual(response.status_code, 404)
                self.assertIn(model, json.loads(response.body)["error"])
                self.assertNotIn("retry-after", response.headers)
                self.assertEqual(logged.await_args.args[0]["status_code"], 404)
                result = await manager.get_valid_credential(model, report_missing_model=True)
                self.assertEqual(result[0], self.email)

    async def test_old_generation_response_does_not_disable_or_cool_replacement(self):
        for streaming in (False, True):
            for status in (403, 429, 404):
                with self.subTest(streaming=streaming, status=status), ExitStack() as stack:
                    await self.db.store_credential(self.email, self.cred)
                    await self.db.update_credential_state(self.email, {"disabled": False, "model_cooldowns": {}})
                    async def replace_and_reject(**kwargs):
                        await self.db.store_credential(self.email, dict(self.cred, refresh_token="replacement"))
                        await self.db.update_credential_state(self.email, {"error_codes": [502]})
                        return httpx.Response(status, text="old result", headers={"retry-after": "120"})
                    async def stream(**kwargs):
                        response = await replace_and_reject(**kwargs)
                        yield Response(content=response.content, status_code=status, headers=response.headers)
                    stack.enter_context(patch.object(api, "get_antigravity_api_url", AsyncMock(return_value="https://mock.invalid")))
                    stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={"retry_enabled": False, "max_retries": 0, "retry_interval": 0})))
                    stack.enter_context(patch.object(api, "get_auto_ban_error_codes", AsyncMock(return_value=[403])))
                    stack.enter_context(patch.object(api_utils, "check_should_auto_ban", AsyncMock(side_effect=lambda code: code == 403)))
                    stack.enter_context(patch.object(ent, "get_database", AsyncMock(return_value=self.db)))
                    stack.enter_context(patch.object(api.usage_log, "record_usage_log", AsyncMock()))
                    stack.enter_context(patch.object(api, "_record_request_body_detail", AsyncMock(return_value="")))
                    stack.enter_context(patch.object(api, "post_async", replace_and_reject))
                    stack.enter_context(patch.object(api, "stream_post_async", stream))
                    body = {"model": "gemini-flash", "request": {"contents": []}}
                    if streaming:
                        response, = [chunk async for chunk in api.stream_request(body)]
                    else:
                        response = await api.non_stream_request(body)
                    self.assertEqual(response.status_code, status)
                    state = await self.db.get_credential_state(self.email)
                    self.assertEqual((state["disabled"], state["model_cooldowns"], state["error_codes"]), (False, {}, [502]))
                    self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email, "gemini-flash"))

    async def test_concurrent_workers_can_upgrade_pre_revision_database(self):
        async with self.db._engine.begin() as conn:
            await conn.execute(text("ALTER TABLE antigravity_credentials DROP COLUMN credential_revision"))
        workers = [Database() for _ in range(4)]
        try:
            await asyncio.gather(*(worker.initialize() for worker in workers))
            for worker in workers:
                row = await worker.get_credential(self.email)
                self.assertEqual((row["credential_revision"], row["note"]), ("", self.cred["note"]))
        finally:
            await asyncio.gather(*(worker.close() for worker in workers))

    async def test_late_success_after_delete_and_readd_with_identical_tokens(self):
        old = await self.db.get_credential(self.email)
        await self.db.delete_credential(self.email)
        await self.db.store_credential(self.email, self.cred)
        current = await self.db.get_credential(self.email)
        self.assertNotEqual(old["credential_revision"], current["credential_revision"])
        await self.db.update_credential_state(self.email, {"error_codes": [403]})
        done = asyncio.Event()
        write = self.db.record_success
        async def observe_write(*args, **kwargs):
            await write(*args, **kwargs)
            done.set()
        with patch.object(self.db, "record_success", observe_write):
            await api_utils.record_api_call_success(self.email, "gemini-flash", expected_revision=old["credential_revision"])
            await asyncio.wait_for(done.wait(), 2)
        self.assertEqual((await self.db.get_credential_state(self.email))["error_codes"], [403])

    async def test_token_rotation_and_note_edits_keep_current_requests_authoritative(self):
        old = await self.db.get_credential(self.email)
        async def rotate(creds):
            creds.access_token = "new-access"
            creds.refresh_token = "rotated-token"
        with patch.object(Credentials, "refresh", rotate):
            await refresh_stored(self.db, self.email, Credentials.from_dict(old))
        await self.db.update_credential_state(self.email, {"note": "updated", "rpm_limit": 3})
        current = await self.db.get_credential(self.email)
        self.assertEqual((current["credential_revision"], current["refresh_token"]), (old["credential_revision"], "rotated-token"))
        with patch.object(api_utils, "check_should_auto_ban", AsyncMock(return_value=True)):
            await api_utils.handle_error_with_retry(403, self.email, False, 0, 0, expected_revision=old["credential_revision"])
        self.assertTrue((await self.db.get_credential_state(self.email))["disabled"])

    async def test_late_refresh_after_oauth_rotation_reuses_only_same_identity(self):
        await self.db.store_credential(self.email, dict(self.cred, expiry=""))
        old = await self.db.get_credential(self.email)
        async def rotate(creds):
            creds.access_token = "published"
            creds.refresh_token = "rotated"
            creds.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        exchange = AsyncMock(side_effect=rotate)
        with patch.object(Credentials, "refresh", autospec=True, side_effect=exchange):
            await refresh_stored(self.db, self.email, Credentials.from_dict(old))
            result = await manager.refresh_token(old.copy(), self.email)
            self.assertIsNotNone(result)
            self.assertEqual((result["token"], result["refresh_token"]), ("published", "rotated"))
            exchange.assert_awaited_once()
            await self.db.store_credential(self.email, dict(self.cred, refresh_token="replacement"))
            self.assertIsNone(await manager.refresh_token(old, self.email))
            exchange.assert_awaited_once()

    async def test_browser_reauthorization_keeps_disabled_status_and_last_verified_metadata(self):
        await self.db.update_credential_state(self.email, {"disabled": True, "error_codes": [403]})
        with patch.object(auth, "get_user_email", AsyncMock(return_value=self.email.upper())), patch.object(
            auth, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
        ), patch.object(auth, "get_database", AsyncMock(return_value=self.db)), patch.object(
            credential_proxy, "get_database", AsyncMock(return_value=self.db)
        ), patch.object(oauth, "_load_code_assist_data", AsyncMock(side_effect=TimeoutError())):
            result = await auth._save_antigravity_credential(Credentials.from_dict(self.cred), "test", {})
        self.assertEqual(await self.db.list_credentials(), [self.email])
        state = await self.db.get_credential_state(self.email)
        self.assertEqual((state["disabled"], state["error_codes"], state["tier"], state["project_id"]),
                         (True, [403], "ultra", "project"))
        self.assertEqual((result["tier"], result["auto_detected_project"]), ("ultra", False))

    async def test_oauth_and_file_verified_free_are_disabled_on_save(self):
        email = "new@example.invalid"
        account = {"paidTier": {"id": "free-tier"}, "cloudaicompanionProject": {"id": " object-project "}}
        with patch.object(auth, "get_user_email", AsyncMock(return_value=email)), patch.object(
            auth, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
        ), patch.object(auth, "get_database", AsyncMock(return_value=self.db)), patch.object(
            credential_proxy, "get_database", AsyncMock(return_value=self.db)
        ), patch.object(oauth, "_load_code_assist_data", AsyncMock(return_value=account)), patch.object(
            panel, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
        ):
            await auth._save_antigravity_credential(Credentials.from_dict(self.cred), "test", {})
            file_result = await panel._import_single_credential(dict(self.cred, email=self.email, project_id="", tier="ultra"))
        self.assertEqual(file_result["status"], "success")
        for identity in (email, self.email):
            state = await self.db.get_credential_state(identity)
            self.assertEqual((state["disabled"], state["tier"], state["project_id"]), (True, "free", "object-project"))
            self.assertIsNone(await self.db.get_available_credential_by_email(identity, "gemini-flash"))

    async def test_affinity_refresh_failure_only_tried_once_per_selection(self):
        await self.db.store_credential(self.email, dict(self.cred, expiry=""))
        for mode in ("cache", "sticky"):
            with self.subTest(mode=mode), patch.object(manager, "_sticky_email", self.email), patch.object(
                manager, "get_credential_select_mode", AsyncMock(return_value=mode)
            ), patch.object(manager, "refresh_token", AsyncMock(return_value=None)) as refresh:
                await manager.session_cache.set_session_binding("test-retry", self.email)
                self.assertIsNone(await manager.get_valid_credential("gemini-flash", "test-retry"))
                self.assertEqual(refresh.await_count, 1)
        await manager.session_cache.delete_session_binding("test-retry")

    async def test_stale_affinity_lookup_preserves_concurrent_rebinding(self):
        other = "new@example.invalid"
        async def rebind_and_fail(*args):
            await manager._after_picked(mode, other, "test-rebind")
            return None
        for mode in ("cache", "sticky"):
            with self.subTest(mode=mode), patch.object(manager, "_sticky_email", self.email), patch.object(
                manager, "get_credential_select_mode", AsyncMock(return_value=mode)
            ), patch.object(manager, "_validate_or_refresh", rebind_and_fail), patch.object(
                self.db, "get_next_available_credential", AsyncMock(return_value=None)
            ):
                await manager.session_cache.set_session_binding("test-rebind", self.email)
                await manager.get_valid_credential("gemini-flash", "test-rebind")
                current = manager._sticky_email if mode == "sticky" else await manager.session_cache.get_session_binding("test-rebind")
                self.assertEqual(current, other)
        await manager.session_cache.delete_session_binding("test-rebind")

    async def test_batch_reports_only_rows_actually_changed(self):
        for action in ("disable", "enable", "delete"):
            result = await panel.creds_batch_action(panel.CredFileBatchActionRequest(
                emails=[self.email, "missing@example.invalid"], action=action), token="test")
            body = json.loads(result.body)
            self.assertEqual(body["success_count"], 1, action)
            self.assertIn("missing@example.invalid", body["message"])

    async def test_three_failed_refreshes_still_find_fresh_credential(self):
        # SQL trim can consider a tab-only token ready; final validation must
        # continue to the usable account without a fourth OAuth exchange.
        await self.db.store_credential("blank@example.invalid", dict(self.cred, access_token="\t"))
        for index in range(4):
            await self.db.store_credential(f"expired{index}@example.invalid", dict(self.cred, expiry=""))
        with patch.object(manager, "get_credential_select_mode", AsyncMock(return_value="random")), patch(
            "src.storage.database.random.choice", side_effect=min
        ), patch.object(manager, "refresh_token", AsyncMock(return_value=None)) as refresh:
            result = await manager.get_valid_credential("gemini-flash")
            self.assertIsNotNone(result)
            self.assertEqual(result[0], self.email)
            self.assertEqual(refresh.await_count, 3)

    async def test_selection_uses_current_token_and_limits_after_old_refresh_fails(self):
        model = "gemini-4-argon-medium"
        for limited in (False, True):
            with self.subTest(limited=limited):
                await self.db.store_credential(self.email, dict(self.cred, expiry=""))
                async def replace_before_oauth_returns(*args):
                    await self.db.store_credential(self.email, dict(
                        self.cred, access_token="new-access", refresh_token="new-refresh"
                    ))
                    if limited:
                        await self.db.update_credential_state(self.email, {"rpm_limit": 1})
                        store = CooldownStore(self.db)
                        key, _ = await store.admit(self.email, model, "concurrent-call")
                        self.assertIsNotNone(key)
                        await store.finish(self.email, key, success=True)
                    raise TokenError("invalid_grant")

                with patch.object(Credentials, "refresh", AsyncMock(side_effect=replace_before_oauth_returns)) as exchange:
                    if limited:
                        with self.assertRaises(DispatchLimited) as raised:
                            await manager.get_valid_credential(model)
                        self.assertGreater(raised.exception.retry_after, 0)
                    else:
                        email, credential = await manager.get_valid_credential(model)
                        self.assertEqual((email, credential["access_token"]), (self.email, "new-access"))
                    exchange.assert_awaited_once()

    def test_oauth_expiry_preserves_western_timezone(self):
        token = Credentials.from_dict({"expiry": "2026-11-22T09:00:00-08:00"})
        self.assertEqual(token.expires_at.astimezone(timezone.utc).hour, 17)

    async def test_slow_refresh_rechecks_rpm_window_and_keeps_caller_exclusions(self):
        await self.db.store_credential("expired@example.invalid", dict(self.cred, expiry=""))
        excluded = "excluded@example.invalid"
        await self.db.store_credential(excluded, self.cred)
        with patch.object(self.db, "get_dispatch_limit_waits", AsyncMock(side_effect=[
            {self.email: 1, excluded: 1}, {}
        ])), patch.object(manager, "get_credential_select_mode", AsyncMock(return_value="random")), patch.object(
            manager, "refresh_token", AsyncMock(return_value=None)
        ), patch("src.storage.database.random.choice", side_effect=min):
            result = await manager.get_valid_credential("gemini-flash", exclude_emails=[excluded])
            self.assertEqual(result[0], self.email)

    async def test_exhausted_refresh_attempts_report_remaining_limited_credential(self):
        for index in range(3):
            await self.db.store_credential(f"expired{index}@example.invalid", dict(self.cred, expiry=""))
        with patch.object(self.db, "get_dispatch_limit_waits", AsyncMock(return_value={self.email: 12})), patch.object(
            manager, "get_credential_select_mode", AsyncMock(return_value="random")
        ), patch.object(manager, "refresh_token", AsyncMock(return_value=None)) as refresh:
            with self.assertRaises(DispatchLimited) as raised:
                await manager.get_valid_credential("gemini-flash")
            self.assertEqual(raised.exception.retry_after, 12)
            self.assertEqual(len({call.args[1] for call in refresh.await_args_list}), 3)

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

    async def test_cancelled_refresh_releases_lease_without_erasing_catalog(self):
        await ent.refresh_one(self.email, self.db)
        await self.due()
        started = asyncio.Event()
        async def hang(**kwargs):
            started.set()
            await asyncio.Event().wait()
        self.post.side_effect = hang
        task = asyncio.create_task(ent.refresh_one(self.email, self.db))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        view = await ent.get_model_view(self.email, self.db)
        self.assertFalse(view["refresh_in_progress"])
        self.assertTrue(view["models_known"])
        self.assertLessEqual(view["retry_after_seconds"], ent.RETRY_SECONDS)

    async def test_404_during_catalog_refresh_keeps_early_recheck(self):
        self.paid = "g1-ultra-tier"
        async def concurrent_hint(url, **kwargs):
            if url.endswith("fetchAvailableModels"):
                await ent.mark_model_unavailable(self.email, "gemini-flash", "synthetic", self.db)
            return await self.response(url, **kwargs)
        self.post.side_effect = concurrent_hint
        await ent.refresh_one(self.email, self.db)
        self.assertIsNone(await self.db.get_next_available_credential("gemini-flash"))
        async with self.db.session() as session:
            state = await session.get(CredentialEntitlement, self.email)
            self.assertLessEqual(state.next_refresh, json.loads(state.snapshot)["attempted_at"] + ent.RETRY_SECONDS + 1)

    async def rotate_stored_tokens(self):
        old = Credentials.from_dict(await self.db.get_credential(self.email))
        async def rotate(creds):
            creds.access_token = "new-access"
            creds.refresh_token = "new-refresh"
        with patch.object(Credentials, "refresh", rotate):
            await refresh_stored(self.db, self.email, old)
        return old

    async def test_catalog_and_free_result_survive_parallel_oauth_rotation(self):
        async def rotate_before_catalog_response(url, **kwargs):
            if url.endswith("fetchAvailableModels"):
                await self.rotate_stored_tokens()
            return await self.response(url, **kwargs)
        self.post.side_effect = rotate_before_catalog_response
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        view = await ent.get_model_view(self.email, self.db)
        self.assertEqual((view["tier"], view["disabled"], view["models_known"], view["refresh_in_progress"]),
                         ("free", True, True, False))

    async def test_model_404_survives_token_rotation_only_for_same_identity(self):
        old = await self.rotate_stored_tokens()
        await ent.mark_model_unavailable(self.email, "missing", old.access_token, self.db,
                                         expected_revision=old.credential_revision)
        self.assertIsNone(await self.db.get_available_credential_by_email(self.email, "missing"))
        await self.db.store_credential(self.email, self.cred)
        await ent.mark_model_unavailable(self.email, "missing", old.access_token, self.db,
                                         expected_revision=old.credential_revision)
        self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email, "missing"))

    async def test_manual_project_change_fences_old_project(self):
        old = await self.db.get_credential(self.email)
        await ent.mark_model_unavailable(self.email, "missing", old["access_token"], self.db)
        with patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
            panel, "get_antigravity_api_url", AsyncMock(return_value="http://example.invalid")
        ), patch.object(panel, "fetch_project_id_and_tier", AsyncMock(return_value=("new-project", "ultra"))):
            result = await panel.verify_credential_project_common(self.email)
        self.assertEqual(result.status_code, 200)
        current = await self.db.get_credential(self.email)
        self.assertEqual(current["project_id"], "new-project")
        self.assertFalse(await self.db.update_credential_state(self.email, {"disabled": True},
                                                              expected_revision=old["credential_revision"]))
        self.assertIsNotNone(await self.db.get_available_credential_by_email(self.email, "missing"))

    async def test_superseded_background_tier_still_refreshes_catalog(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def fetch(url, **kwargs):
            reply = await self.response(url, **kwargs)
            if url.endswith("fetchAvailableModels"):
                started.set()
                await release.wait()
            return reply

        self.post.side_effect = fetch
        with patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
            panel, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
        ), patch.object(panel, "fetch_project_id_and_tier", AsyncMock(return_value=("project", "ultra"))):
            background = asyncio.create_task(ent.refresh_one(self.email, self.db))
            try:
                await asyncio.wait_for(started.wait(), 2)
                await panel.verify_credential_project_common(self.email)
            finally:
                release.set()
                await background
        view = await ent.get_model_view(self.email, self.db)
        self.assertEqual((view["tier"], view["disabled"]), ("ultra", False))
        self.assertEqual([m["name"] for m in view["models"]], ["gemini-flash"])

    async def test_superseded_manual_check_cannot_reenable_verified_free(self):
        async def verify(**kwargs):
            await ent.refresh_one(self.email, self.db)
            return "project", "ultra"

        with patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
            panel, "get_antigravity_api_url", AsyncMock(return_value="https://upstream.invalid")
        ), patch.object(panel, "fetch_project_id_and_tier", verify):
            with self.assertRaises(HTTPException) as stale:
                await panel.verify_credential_project_common(self.email)
        self.assertEqual(stale.exception.status_code, 409)
        self.assertIsNone(await self.db.get_next_available_credential("gemini-flash"))
        self.assertEqual((await self.db.get_credential_state(self.email))["tier"], "free")

    async def test_manual_tier_verification_survives_parallel_rotation(self):
        async def verify(**kwargs):
            await self.rotate_stored_tokens()
            return "project", "free"
        with patch.object(panel, "get_database", AsyncMock(return_value=self.db)), patch.object(
            panel, "get_antigravity_api_url", AsyncMock(return_value="http://example.invalid")
        ), patch.object(panel, "fetch_project_id_and_tier", verify):
            result = await panel.verify_credential_project_common(self.email)
        self.assertEqual(result.status_code, 200)
        state = await self.db.get_credential_state(self.email)
        self.assertEqual((state["tier"], state["disabled"]), ("free", True))

    async def test_already_dispatched_results_do_not_reopen_cooling_family(self):
        model = "gemini-4-argon-medium"
        deadline = time.time() + 120
        await self.db.set_model_cooldown(self.email, model, deadline)
        await self.db.update_credential_state(self.email, {"error_codes": [429]})
        await self.db.record_success(self.email, "gemini-4-argon-low")
        state = await self.db.get_credential_state(self.email)
        self.assertEqual((state["model_cooldowns"].get("argon"), state["error_codes"]), (deadline, [429]))
        await self.db.set_model_cooldown(self.email, model, deadline - 60)
        self.assertEqual((await self.db.get_credential_state(self.email))["model_cooldowns"]["argon"], deadline)
        self.assertIsNone(await self.db.get_next_available_credential(model))
        self.assertIsNotNone(await self.db.get_next_available_credential("claude-test"))
        await self.db.set_model_cooldown(self.email, model, None)
        self.assertIsNotNone(await self.db.get_next_available_credential(model))

    async def test_pool_with_only_upstream_quota_wait_reports_earliest_eligible_reset(self):
        fast, excluded = "fast@example.invalid", "skip@example.invalid"
        now = time.time()
        for email, seconds in [(self.email, 90), (fast, 60), (excluded, 5)]:
            await self.db.store_credential(email, self.cred)
            await self.db.set_model_cooldown(email, "gemini-4-argon-high", now + seconds)
        with patch.object(manager, "get_credential_select_mode", AsyncMock(return_value="random")):
            with self.assertRaises(QuotaCooling) as limited:
                await manager.get_valid_credential("gemini-4-argon-medium", exclude_emails={excluded})
            self.assertEqual((limited.exception.status_code, limited.exception.retry_after), (429, 60))
            self.assertIn("上游", str(limited.exception))
            self.assertIsNotNone(await manager.get_valid_credential("claude-test"))

    async def test_quota_retry_wait_accounts_for_overlapping_rpm_per_credential(self):
        model = "gemini-4-argon-medium"
        now = time.time()
        store = CooldownStore(self.db, clock=lambda: now)
        await self.db.update_credential_state(self.email, {"rpm_limit": 1})
        lease, _ = await store.admit(self.email, model, "first")
        await store.finish(self.email, lease)
        await self.db.set_model_cooldown(self.email, model, now + 3)
        with patch("src.storage.database.time.time", return_value=now), patch.object(
            manager, "get_credential_select_mode", AsyncMock(return_value="random")
        ):
            with self.assertRaises(QuotaCooling) as single:
                await manager.get_valid_credential(model)
            self.assertEqual(single.exception.retry_after, 60)
            with self.assertRaises(QuotaCooling) as at_dispatch:
                await store.admit(self.email, model, "selected-before-cooldown")
            self.assertEqual(at_dispatch.exception.retry_after, 60)
            other = "other@example.invalid"
            await self.db.store_credential(other, self.cred)
            await self.db.set_model_cooldown(other, model, now + 8)
            with self.assertRaises(QuotaCooling) as pool:
                await manager.get_valid_credential(model)
            self.assertEqual(pool.exception.retry_after, 8)

    async def test_models_union_excludes_disabled_accounts_and_recent_404(self):
        self.paid = 'g1-ultra-tier'
        self.catalog = {'gemini-flash': {}, 'gemini-4-argon-medium': {}}
        self.assertIsNone(await self.db.get_enabled_model_catalog())
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        other = 'second@example.invalid'
        await self.db.store_credential(other, self.cred)
        self.catalog = {'gemini-flash': {}, 'gemini-4-hydrogen-high': {}}
        self.assertTrue(await ent.refresh_one(other, self.db))
        self.assertEqual(await self.db.get_enabled_model_catalog(),
                         ['gemini-4-argon-medium', 'gemini-4-hydrogen-high', 'gemini-flash'])
        await self.db.update_credential_state(other, {'disabled': True})

        await ent.mark_model_unavailable(self.email, 'gemini-4-argon-medium', 'stale', self.db)
        self.assertIsNotNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))
        await ent.mark_model_unavailable(self.email, 'gemini-4-argon-medium', 'synthetic', self.db)
        self.assertEqual(await self.db.get_enabled_model_catalog(), ['gemini-flash'])
        self.assertIsNone(await self.db.get_next_available_credential('gemini-4-argon-medium'))
        self.assertIsNotNone(await self.db.get_next_available_credential('gemini-flash'))
        self.assertFalse((await self.db.get_credential_state(self.email))['disabled'])
        self.assertEqual((await ent.get_model_view(self.email, self.db))['models'],
                         [{'name': 'gemini-flash', 'quota_cooldown_until': None}])

        await self.due()
        self.catalog = {'gemini-4-argon-medium': {}}
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        self.assertEqual(await self.db.get_enabled_model_catalog(), ['gemini-4-argon-medium'])

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

    async def test_token_refresh_waiters_share_rotated_token_and_release_lease_on_cancel(self):
        previous = Credentials.from_dict(self.cred)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []
        async def delayed(creds):
            calls.append(creds)
            started.set()
            await release.wait()
            creds.access_token = 'updated'
            creds.refresh_token = 'rotated'
            creds.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch.object(Credentials, 'refresh', delayed):
            first = asyncio.create_task(refresh_stored(self.db, self.email, previous))
            await asyncio.wait_for(started.wait(), 2)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            started.clear()
            first = asyncio.create_task(refresh_stored(self.db, self.email, previous))
            await asyncio.wait_for(started.wait(), 2)
            others = [asyncio.create_task(refresh_stored(self.db, self.email, previous)) for _ in range(3)]
            await asyncio.sleep(.25)
            release.set()
            results = await asyncio.wait_for(asyncio.gather(first, *others), 3)
        self.assertEqual(len(calls), 2)  # cancelled owner, then one successful refresh
        self.assertTrue(all((r.access_token, r.refresh_token) == ('updated', 'rotated') for r in results))

    async def test_old_refresh_rejection_does_not_disable_replacement(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def reject(creds):
            started.set()
            await release.wait()
            exc = TokenError("invalid_grant")
            exc.status_code = 400
            raise exc
        with patch.object(Credentials, 'refresh', reject):
            old = asyncio.create_task(manager.refresh_token(dict(self.cred), self.email))
            await asyncio.wait_for(started.wait(), 2)
            new_data = {**self.cred, 'refresh_token': 'replacement'}
            await self.db.store_credential(self.email, new_data)
            release.set()
            self.assertIsNone(await old)
            state = await self.db.get_credential_state(self.email)
            self.assertFalse(state['disabled'])
            self.assertEqual(state['error_codes'], [])
            self.assertIsNone(await manager.refresh_token(new_data, self.email))
            state = await self.db.get_credential_state(self.email)
            self.assertTrue(state['disabled'])
            self.assertEqual(state['error_codes'], [400])

    async def test_waiter_takes_over_cancelled_token_refresh(self):
        started = asyncio.Event()
        calls = []
        async def refresh(creds):
            calls.append(creds)
            if len(calls) == 1:
                started.set()
                await asyncio.Event().wait()
            creds.access_token = 'replacement-access'
            creds.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        previous = Credentials.from_dict(self.cred)
        with patch.object(Credentials, 'refresh', refresh):
            owner = asyncio.create_task(refresh_stored(self.db, self.email, previous))
            await asyncio.wait_for(started.wait(), 2)
            waiters = [asyncio.create_task(refresh_stored(self.db, self.email, previous)) for _ in range(2)]
            await asyncio.sleep(.25)
            owner.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await owner
            results = await asyncio.wait_for(asyncio.gather(*waiters), 3)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(r.access_token == 'replacement-access' for r in results))

    async def test_manual_verification_preserves_disabled_state_on_unknown_and_disables_free(self):
        self.stack.enter_context(patch.object(panel, "get_database", AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(panel, "get_antigravity_api_url", AsyncMock(return_value="http://example.invalid")))
        self.stack.enter_context(patch.object(oauth, "post_async", self.post))
        await self.db.update_credential_state(self.email, {"disabled": True, "error_codes": [403]})
        self.paid = "unknown-plan"
        with self.assertRaises(HTTPException) as failed:
            await panel.verify_credential_project_common(self.email)
        state = await self.db.get_credential_state(self.email)
        self.assertEqual(failed.exception.status_code, 502)
        self.assertEqual((state["disabled"], state["tier"], state["error_codes"]), (True, "ultra", [403]))
        for plan, disabled in [("free-tier", True), ("g1-ultra-tier", False)]:
            self.paid = plan
            await panel.verify_credential_project_common(self.email)
            state = await self.db.get_credential_state(self.email)
            self.assertEqual(state["disabled"], disabled)
            self.assertEqual(state["tier"], "free" if disabled else "ultra")
            self.assertEqual(state["project_id"], "project")

    async def test_manual_verification_rejects_result_for_replaced_token(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def verify(*args, **kwargs):
            started.set()
            await release.wait()
            return 'old-project', 'free'
        with patch.object(panel, 'get_database', AsyncMock(return_value=self.db)), patch.object(
            panel, 'fetch_project_id_and_tier', verify
        ), patch.object(panel, 'get_antigravity_api_url', AsyncMock(return_value='http://example.invalid')):
            task = asyncio.create_task(panel.verify_credential_project_common(self.email))
            await asyncio.wait_for(started.wait(), 2)
            await self.db.store_credential(self.email, {**self.cred, 'refresh_token': 'replacement'})
            release.set()
            with self.assertRaises(HTTPException) as stale:
                await task
        self.assertEqual(stale.exception.status_code, 409)
        state = await self.db.get_credential_state(self.email)
        self.assertEqual((state['tier'], state['project_id'], state['disabled']), ('ultra', 'project', False))

    async def test_malformed_model_entry_preserves_last_routable_catalog(self):
        self.paid = "g1-ultra-tier"
        model = "gemini-4-argon-medium"
        self.catalog = {model: {"disabled": False}}
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        for invalid in ({model: {"disabled": "false"}}, {"": {}}):
            self.catalog = invalid
            await self.due()
            self.assertFalse(await ent.refresh_one(self.email, self.db))
            self.assertEqual(await self.db.get_enabled_model_catalog(), [model])
            self.assertIsNotNone(await self.db.get_next_available_credential(model))
        self.catalog = {model: {"disabled": True}}
        await self.due()
        self.assertTrue(await ent.refresh_one(self.email, self.db))
        self.assertIsNone(await self.db.get_next_available_credential(model))

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
        for paid in ({}, {'name': 'Google AI Ultra'}, {'id': 'future-free-trial-tier'}, {'id': ''}, 'invalid'):
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
