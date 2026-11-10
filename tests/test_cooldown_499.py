import asyncio
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from sqlalchemy import select
import httpx
from fastapi import FastAPI, HTTPException

from src import credential_cooldown as cooldown
from src.api import antigravity as api
from src.storage.database import Database, Credential499State
from src.stream_delivery import StreamDelivery, current_delivery


class CooldownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {"CREDENTIALS_DIR": self.temp.name, "POSTGRESQL_URI": ""}))
        self.db = Database()
        await self.db.initialize()
        self.addAsyncCleanup(self.db.close)
        self.email = "test@example.invalid"
        self.cred = {"access_token": "synthetic", "project_id": "test"}
        await self.db.store_credential(self.email, self.cred)
        self.now = 1000.
        self.store = cooldown.CooldownStore(self.db, lambda: self.now)
        self.stack.enter_context(patch.object(cooldown, "get_database", AsyncMock(return_value=self.db)))
        await self.settings(mode="enforce", threshold=3, min_wait_seconds=90)

    async def settings(self, **values):
        await cooldown.save_settings(self.db, {cooldown.PREFIX + k: v for k, v in values.items()})

    async def admit(self, req="request", model="gemini-4-argon"):
        return await self.store.admit(self.email, model, req)

    async def qualifying_failure(self, req, model="gemini-4-argon", **kwargs):
        key, _ = await self.admit(req, model)
        self.assertIsNotNone(key)
        await self.store.finish(self.email, key, elapsed=100, cancelled_empty=True, **kwargs)

    async def state(self):
        async with self.db.session() as session:
            row = await session.get(Credential499State, self.email)
            return json.loads(row.state)

    async def trip(self):
        for i in range(3):
            await self.qualifying_failure(str(i))

    async def test_manual_clear_preserves_live_leases_and_fences_old_results(self):
        old, _ = await self.admit("old")
        await self.trip()
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
        self.assertTrue(await self.store.clear(self.email))
        self.assertTrue(await self.store.renew(self.email, old))
        self.assertIsNotNone((await self.admit("new"))[0])
        await self.store.finish(self.email, old, cancelled_empty=True, elapsed=100)
        group = (await self.state())["groups"]["*"]
        self.assertEqual((group["phase"], group["failures"], group["trips"]), ("closed", [], 1))
        await self.qualifying_failure("after-1")
        await self.qualifying_failure("after-2")
        await self.qualifying_failure("after-3")
        self.assertIsNone((await self.admit("blocked-again"))[0])

    async def test_deleted_credential_does_not_leave_visible_or_reimported_cooldown(self):
        await self.trip()
        self.assertTrue(await self.db.delete_credential(self.email))
        self.assertEqual((await self.store.status())["items"], [])
        self.assertFalse(await self.store.clear(self.email))
        await self.db.store_credential(self.email, self.cred)
        self.assertIsNotNone((await self.admit("reimported"))[0])

    async def test_delete_reimport_retains_transports_but_fences_old_failures(self):
        await self.settings(threshold=1)
        old, _ = await self.admit("old-account")
        self.assertTrue(await self.db.delete_credential(self.email))
        self.assertTrue(await self.store.renew(self.email, old))
        await self.db.store_credential(self.email, self.cred)
        new, _ = await self.admit("new-account")
        self.assertIsNotNone(new)
        await self.store.finish(self.email, old, cancelled_empty=True, elapsed=100)
        self.assertEqual((await self.state())["groups"]["*"]["phase"], "closed")
        await self.store.finish(self.email, new, cancelled_empty=True, elapsed=100)
        self.assertIsNone((await self.admit("new-failure-still-counts"))[0])

    async def test_delete_serializes_with_admission_and_clear_across_workers(self):
        other = Database()
        await other.initialize()
        self.addAsyncCleanup(other.close)
        worker = cooldown.CooldownStore(other, lambda: self.now)
        await self.settings(threshold=1)
        for i in range(8):
            await self.db.store_credential(self.email, self.cred)
            old, _ = await self.admit(f"before-delete-{i}")
            outcomes = await asyncio.wait_for(asyncio.gather(
                self.db.delete_credential(self.email),
                worker.admit(self.email, "gemini-4-argon", f"racing-{i}"),
                worker.clear(self.email)), 5)
            self.assertTrue(outcomes[0])
            self.assertIsNone(await self.db.get_credential(self.email))
            self.assertEqual((await self.store.status())["items"], [])
            await self.db.store_credential(self.email, self.cred)
            fresh, _ = await self.admit(f"after-delete-{i}")
            self.assertIsNotNone(fresh)
            for key in (old, outcomes[1][0]):
                if key:
                    self.assertTrue(await worker.renew(self.email, key))
                    await worker.finish(self.email, key, cancelled_empty=True, elapsed=100)
            self.assertEqual((await self.state())["groups"]["*"]["phase"], "closed")
            await self.store.finish(self.email, fresh, success=True)

    async def test_manual_clear_scopes_and_disabled_history_preserved(self):
        await self.trip()
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
        before = await self.state()
        self.assertTrue(await self.store.clear(self.email, "quota"))
        self.assertEqual(await self.state(), before)
        self.assertIsNone((await self.admit())[0])
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
        await self.db.update_credential_state(self.email, {"disabled": True, "note": "keep", "error_codes": [403]})
        cred_before = await self.db.get_credential_state(self.email)
        self.assertTrue(await self.store.clear(self.email, "protection"))
        self.assertEqual(await self.db.get_credential_state(self.email), cred_before)
        self.assertEqual((await self.state())["groups"]["*"]["phase"], "closed")
        await self.store.clear(self.email)
        cred_after = await self.db.get_credential_state(self.email)
        for field in ("disabled", "note", "error_codes"):
            self.assertEqual(cred_after[field], cred_before[field])
        self.assertEqual(cred_after["model_cooldowns"], {})
        self.assertIsNone((await self.admit())[0])

    async def test_manual_clear_all_families_probe_and_missing_credential(self):
        await self.settings(scope="family", threshold=1)
        await self.qualifying_failure("a")
        await self.qualifying_failure("b", "claude-sonnet")
        self.now += 1200
        probe, _ = await self.admit("probe")
        await self.store.clear(self.email, "protection")
        await self.qualifying_failure("new-cycle")
        await self.store.finish(self.email, probe, success=True)
        self.assertIsNone((await self.admit())[0])
        self.assertIsNotNone((await self.admit("claude", "claude-sonnet"))[0])
        self.assertFalse(await self.store.clear("missing@example.invalid"))
        async with self.db.session() as session:
            self.assertIsNone(await session.get(Credential499State, "missing@example.invalid"))

    async def test_manual_clear_cross_worker_atomic_with_admission(self):
        other = Database()
        await other.initialize()
        self.addAsyncCleanup(other.close)
        worker = cooldown.CooldownStore(other, lambda: self.now)
        await self.trip()
        await asyncio.gather(self.store.clear(self.email), worker.admit(self.email, "gemini-4-argon", "racing"))
        self.assertIsNotNone((await worker.admit(self.email, "gemini-4-argon", "after"))[0])
        state = await self.state()
        self.assertEqual(state["groups"]["*"]["phase"], "closed")
        for key in state["leases"]:
            self.assertTrue(await worker.renew(self.email, key))

    async def test_cancelled_clear_rolls_back_both_types_and_keeps_live_ownership(self):
        from sqlalchemy.ext.asyncio import AsyncSession
        old, _ = await self.admit("still-running")
        await self.trip()
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
        original_commit = AsyncSession.commit
        for scope in ("all", "quota", "protection"):
            with self.subTest(scope=scope):
                before = await self.state()
                entered = asyncio.Event()
                clearing = None
                async def pause_before_commit(session):
                    if asyncio.current_task() is clearing:
                        await session.flush()  # SQL sent, transaction not committed.
                        entered.set()
                        await asyncio.Event().wait()
                    await original_commit(session)
                with patch.object(AsyncSession, "commit", pause_before_commit):
                    clearing = asyncio.create_task(self.store.clear(self.email, scope))
                    await asyncio.wait_for(entered.wait(), 3)
                    clearing.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await clearing
                self.assertEqual(await self.state(), before)
                self.assertEqual((await self.db.get_credential_state(self.email))["model_cooldowns"], {"argon": 5000})
                self.assertTrue(await self.store.renew(self.email, old))
        await self.store.finish(self.email, old, success=True)
        self.assertEqual((await self.state())["groups"]["*"]["phase"], "cooling")

    async def test_duplicate_batch_clear_preserves_unselected_account_and_other_scope(self):
        from src.panel import creds
        second = "unselected@example.invalid"
        await self.db.store_credential(second, self.cred)
        await self.trip()
        for email in (self.email, second):
            await self.db.set_model_cooldown(email, "gemini-4-argon", 5000)
        breaker_before = await self.state()
        with patch.object(creds, "get_database", AsyncMock(return_value=self.db)):
            result = await creds.clear_credential_cooldowns(
                creds.ClearCooldownRequest(emails=[self.email, self.email], scope="quota"), token="synthetic")
        self.assertEqual((result["total_count"], result["success_count"]), (1, 1))
        self.assertEqual(await self.state(), breaker_before)
        self.assertEqual((await self.db.get_credential_state(self.email))["model_cooldowns"], {})
        self.assertEqual((await self.db.get_credential_state(second))["model_cooldowns"], {"argon": 5000})

    async def test_manual_clear_cannot_resurrect_quota_snapshot_from_inflight_writer(self):
        from sqlalchemy.ext.asyncio import AsyncSession
        other = Database()
        await other.initialize()
        self.addAsyncCleanup(other.close)
        clearer = cooldown.CooldownStore(other, lambda: self.now)
        original_scalar = AsyncSession.scalar
        for scope in ("quota", "all"):
            for operation in ("limit", "success"):
                with self.subTest(scope=scope, operation=operation):
                    await self.db.clear_all_model_cooldowns(self.email)
                    await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
                    await self.db.set_model_cooldown(self.email, "gemini-flash", 5000)
                    read, release = asyncio.Event(), asyncio.Event()
                    writer = None
                    async def pause_writer(session, *args, **kwargs):
                        result = await original_scalar(session, *args, **kwargs)
                        if asyncio.current_task() is writer:
                            read.set()
                            await release.wait()
                        return result
                    with patch.object(AsyncSession, "scalar", pause_writer):
                        writer = asyncio.create_task(
                            self.db.set_model_cooldown(self.email, "claude-sonnet", 6000)
                            if operation == "limit" else self.db.record_success(self.email, "gemini-flash"))
                        await asyncio.wait_for(read.wait(), 3)
                        clearing = asyncio.create_task(clearer.clear(self.email, scope))
                        try:
                            # An unlocked writer allows clear to commit first;
                            # a serialized writer correctly holds it until release.
                            await asyncio.wait_for(asyncio.shield(clearing), .2)
                        except asyncio.TimeoutError:
                            pass
                        finally:
                            release.set()
                        await asyncio.gather(writer, clearing)
                    saved = (await self.db.get_credential_state(self.email))["model_cooldowns"]
                    self.assertNotIn("argon", saved, "pre-clear quota snapshot was restored")

    async def test_manual_clear_corrupt_state_is_not_reported_as_missing_account(self):
        await self.trip()
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
        async with self.db.session() as session:
            row = await session.get(Credential499State, self.email)
            state = json.loads(row.state)
            del state["groups"]["*"]["trips"]
            row.state = json.dumps(state)
        with self.assertRaises(KeyError):
            await self.store.clear(self.email)
        # Clearing both types is atomic even if the second reset fails.
        self.assertEqual((await self.db.get_credential_state(self.email))["model_cooldowns"], {"argon": 5000})
        from src.panel import creds
        valid_email = "valid@example.invalid"
        await self.db.store_credential(valid_email, self.cred)
        await self.db.set_model_cooldown(valid_email, "gemini-4-argon", 5000)
        with patch.object(creds, "get_database", AsyncMock(return_value=self.db)):
            result = await creds.clear_credential_cooldowns(
                creds.ClearCooldownRequest(emails=[self.email, valid_email]), token="synthetic")
        self.assertEqual(result["success_count"], 1)
        self.assertIn("清除失败", result["errors"][0])
        self.assertNotIn("凭证不存在", result["errors"][0])
        self.assertEqual((await self.db.get_credential_state(valid_email))["model_cooldowns"], {})

    async def test_clear_cooldown_endpoint_auth_validation_and_partial_batch(self):
        from src.panel import creds
        from src.utils import verify_panel_token
        app = FastAPI()
        app.include_router(creds.router)
        self.stack.enter_context(patch.object(creds, "get_database", AsyncMock(return_value=self.db)))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            payload = {"emails": [self.email], "scope": "all"}
            self.assertEqual((await client.post("/creds/clear-cooldown", json=payload)).status_code, 401)
            app.dependency_overrides[verify_panel_token] = lambda: "synthetic"
            for bad in ({"emails": []}, {"emails": [" "]}, {"emails": [self.email], "scope": "invalid"}, {"emails": [self.email] * 1001}):
                self.assertEqual((await client.post("/creds/clear-cooldown", json=bad)).status_code, 422)
            await self.db.set_model_cooldown(self.email, "gemini-4-argon", 5000)
            payload["emails"] = [self.email, self.email, "missing@example.invalid"]
            response = await client.post("/creds/clear-cooldown", json=payload)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual((data["success_count"], data["total_count"], len(data["errors"])), (1, 2, 1))
            self.assertEqual((await self.db.get_credential_state(self.email))["model_cooldowns"], {})

    async def test_drain_starts_real_idle_clock_and_old_success_cannot_clear(self):
        old, _ = await self.admit("old")
        await self.trip()
        value = (await self.state())["groups"]["*"]
        self.assertEqual(value["phase"], "draining")
        self.assertEqual((await self.admit("blocked"))[0], None)
        self.now += 100
        await self.store.finish(self.email, old, success=True)
        value = (await self.state())["groups"]["*"]
        self.assertEqual((value["phase"], value["until"]), ("cooling", 2300))
        self.now = 2299
        self.assertIsNone((await self.admit())[0])
        self.now = 2300
        probe, _ = await self.admit("probe")
        self.assertIsNotNone(probe)
        self.assertIsNone((await self.admit("concurrent"))[0])
        await self.store.finish(self.email, probe, success=True)
        self.assertIsNotNone((await self.admit("healthy"))[0])

    async def test_qualifying_probe_failure_recools_and_other_errors_do_not_reset(self):
        await self.trip()
        self.now += 1200
        probe, _ = await self.admit("probe-1")
        await self.store.finish(self.email, probe)  # unrelated 503/401 etc.
        self.assertEqual((await self.state())["groups"]["*"]["phase"], "probe")
        await self.qualifying_failure("probe-2")
        self.assertEqual((await self.state())["groups"]["*"]["until"], self.now + 1200)

    async def test_dedup_window_short_cancel_and_success_reset(self):
        await self.qualifying_failure("same")
        await self.qualifying_failure("same")
        key, _ = await self.admit("short")
        await self.store.finish(self.email, key, cancelled_empty=True, elapsed=89)
        self.assertEqual(len((await self.state())["groups"]["*"]["failures"]), 1)
        success, _ = await self.admit("success")
        concurrent, _ = await self.admit("later-failure")
        await self.store.finish(self.email, success, success=True)
        await self.store.finish(self.email, concurrent, cancelled_empty=True, elapsed=100)
        self.assertEqual(len((await self.state())["groups"]["*"]["failures"]), 1)
        self.now += 301
        await self.qualifying_failure("outside-window")
        self.assertEqual(len((await self.state())["groups"]["*"]["failures"]), 1)

    async def test_scope_mode_hot_edit_and_latched_duration(self):
        await self.settings(scope="family", duration_seconds=10)
        await self.trip()
        self.assertIsNotNone((await self.admit("different", "claude-sonnet"))[0])
        await self.settings(duration_seconds=100)
        self.now += 10
        self.assertIsNotNone((await self.admit("probe"))[0])
        await self.settings(mode="off")
        self.assertIsNotNone((await self.admit("off"))[0])
        await self.settings(mode="enforce", scope="account", threshold=1)
        await self.qualifying_failure("new-policy")
        self.assertIsNone((await self.admit("blocked-other", "claude-sonnet"))[0])

    async def test_observe_never_blocks_off_does_not_accumulate(self):
        await self.settings(mode="observe")
        await self.trip()
        self.assertIsNotNone((await self.admit("observed"))[0])
        value = (await self.state())["groups"]["*"]
        self.assertEqual(value["would_block"], 1)
        await self.settings(mode="off")
        await self.trip()
        self.assertEqual((await self.state())["groups"]["*"]["failures"], [])

    async def test_shared_sqlite_atomic_probe_limit_and_restart(self):
        await self.trip()
        self.now += 1200
        await self.settings(probe_concurrency=3)
        other_db = Database()
        await other_db.initialize()
        try:
            other = cooldown.CooldownStore(other_db, lambda: self.now)
            results = await asyncio.gather(*[
                (self.store if i % 2 else other).admit(self.email, "gemini-4-argon", f"probe-{i}")
                for i in range(24)])
            self.assertEqual(sum(key is not None for key, _ in results), 3)
        finally:
            await other_db.close()
        self.assertEqual(len((await self.state())["leases"]), 3)

    async def test_lost_worker_leases_expire_conservatively(self):
        old, _ = await self.admit("crashed")
        await self.trip()
        self.now += cooldown.LEASE_SECONDS + 1
        self.assertIsNone((await self.admit("not-yet"))[0])
        self.assertEqual((await self.state())["groups"]["*"]["until"], self.now + 1200)
        self.now += 1200
        self.assertIsNotNone((await self.admit("crashed-probe"))[0])
        self.now += cooldown.LEASE_SECONDS + 1
        self.assertIsNone((await self.admit("no-fake-recovery"))[0])

    async def test_existing_success_and_disabled_state_cannot_bypass(self):
        await self.trip()
        await self.db.record_success(self.email, "gemini-4-argon")
        self.assertIsNone((await self.admit())[0])
        self.now += 1200
        await self.db.update_credential_state(self.email, {"disabled": True})
        self.assertIsNone((await self.admit())[0])
        self.assertTrue((await self.db.get_all_credential_states())[self.email]["disabled"])

    async def test_renewal_and_duplicate_completion(self):
        key, _ = await self.admit()
        self.now += 100
        self.assertTrue(await self.store.renew(self.email, key))
        self.now += 100
        await self.store.finish(self.email, key, cancelled_empty=True, elapsed=200)
        await self.store.finish(self.email, key, cancelled_empty=True, elapsed=200)
        self.assertEqual(len((await self.state())["groups"]["*"]["failures"]), 1)

    async def test_config_validation_is_strict_and_roundtrips_all_parameters(self):
        for key, (lo, hi) in cooldown.LIMITS.items():
            for value in (True, 1.2, "1", None, lo - 1, hi + 1):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    await self.settings(**{key: value})
            await self.settings(**{key: hi})
        for key, value in (("mode", "bad"), ("scope", "bad"), ("revision", "bad")):
            with self.assertRaises(ValueError):
                await self.settings(**{key: value})
        async with self.db.session() as session:
            cfg = await cooldown.read_settings(session)
        for key, (_, hi) in cooldown.LIMITS.items():
            self.assertEqual(cfg[key], hi)

    def patch_api(self):
        for name, value in {
            "get_valid_credential": (self.email, self.cred), "get_antigravity_api_url": "https://example.invalid",
            "get_auto_ban_error_codes": [], "get_retry_config": {"max_retries": 0, "retry_interval": 0, "retry_enabled": False},
            "record_api_call_success": None, "_record_request_body_detail": "",
        }.items():
            self.stack.enter_context(patch.object(api, name, AsyncMock(return_value=value)))
        self.saved = self.stack.enter_context(patch("src.usage_log.record_usage_log", AsyncMock()))

    async def test_actual_stream_disconnect_empty_qualifies_but_thought_and_output_do_not(self):
        self.patch_api()
        await self.settings(threshold=1, min_wait_seconds=0)
        for event, bytes_sent, qualifies in ((None, 0, True), ("thought", 0, False), (None, 10, False)):
            await self.settings(mode="off")
            await self.settings(mode="enforce")
            delivery = StreamDelivery()
            delivery.bytes_sent = bytes_sent
            ctx = current_delivery.set(delivery)
            entered = asyncio.Event()

            async def upstream(**kwargs):
                if event:
                    yield 'data: ' + json.dumps({"response": {"candidates": [{"content": {"parts": [{"text": "thinking", "thought": True}]}}]}}) + '\n\n'
                entered.set()
                await asyncio.Event().wait()
                yield ""

            async def consume():
                async for _ in api.stream_request({"model": "gemini-4-argon", "request": {"contents": []}}):
                    pass

            try:
                with patch.object(api, "stream_post_async", upstream):
                    task = asyncio.create_task(consume())
                    await asyncio.wait_for(entered.wait(), 3)
                    delivery.disconnect_source = "http.disconnect"
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                    await delivery.finalize()
                value = (await self.state())["groups"]["*"]
                self.assertEqual(value["phase"] == "cooling", qualifies)
                self.assertEqual((await self.state())["leases"], {})
                self.assertEqual(self.saved.call_args.args[0]["status_code"], 499)
            finally:
                current_delivery.reset(ctx)

    async def test_dispatch_rechecks_prefetch_and_searches_alternatives(self):
        await self.trip()
        # Real clock follows this store's frozen clock for deterministic admission.
        second = "healthy@example.invalid"
        await self.db.store_credential(second, self.cred)
        selector = AsyncMock(return_value=(second, self.cred))
        with patch.object(cooldown, "CooldownStore", return_value=self.store):
            picked, lease = await cooldown.acquire_dispatch((self.email, self.cred), "gemini-4-argon", "session", "req", selector)
        self.assertEqual(picked[0], second)
        self.assertEqual(selector.call_args.kwargs["exclude_emails"], {self.email})
        await lease.result(success=True)
        await lease.close()

    async def test_pool_cooling_returns_503_without_calling_upstream(self):
        self.patch_api()
        await self.trip()
        with patch.object(cooldown, "CooldownStore", return_value=self.store), patch.object(api, "post_async", AsyncMock()) as post:
            response = await api.non_stream_request({"model": "gemini-4-argon", "request": {"contents": []}})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["retry-after"], "1200")
        post.assert_not_awaited()

    async def test_panel_roundtrip_and_fresh_read_in_another_worker(self):
        import config
        from src.models import ConfigSaveRequest
        from src.panel import config_routes
        other = Database()
        await other.initialize()  # This worker's generic config cache is now stale.
        try:
            self.stack.enter_context(patch("src.storage.database.get_database", AsyncMock(return_value=self.db)))
            self.stack.enter_context(patch.object(config, "_config_initialized", True))
            self.stack.enter_context(patch.object(config, "_config_cache", {}))
            values = dict(zip(cooldown.DEFAULTS, ("observe", 5, 600, 120, 1800, "family", 4)))
            with patch.object(config_routes, "get_database", AsyncMock(return_value=self.db)):
                response = await config_routes.save_config(ConfigSaveRequest(config=values), token="synthetic")
                self.assertEqual(response.status_code, 200)
                bad = {**values, "cooldown_499_duration_seconds": -1}
                with self.assertRaises(HTTPException):
                    await config_routes.save_config(ConfigSaveRequest(config=bad), token="synthetic")
            with patch.object(config_routes, "get_database", AsyncMock(return_value=other)):
                current = json.loads((await config_routes.get_config(token="synthetic")).body)["config"]
            self.assertEqual({key: current[key] for key in values}, values)
            self.assertNotIn("cooldown_499_revision", current)
        finally:
            await other.close()

    async def test_all_protocols_preserve_pool_503_and_retry_after(self):
        from src.router.antigravity import openai, anthropic, gemini
        self.patch_api()
        await self.trip()
        cases = [
            (openai, "/v1/chat/completions", {"model": "gemini-4-argon", "messages": [{"role": "user", "content": "test"}]}),
            (anthropic, "/v1/messages", {"model": "gemini-4-argon", "max_tokens": 8, "messages": [{"role": "user", "content": "test"}]}),
            (gemini, "/v1beta/models/gemini-4-argon:generateContent", {"contents": [{"role": "user", "parts": [{"text": "test"}]}]}),
        ]
        with patch.object(cooldown, "CooldownStore", return_value=self.store), patch.object(api, "post_async", AsyncMock()) as post, patch.object(api, "stream_post_async", AsyncMock()) as stream:
            for module, path, body in cases:
                for streaming in (False, True):
                    app = FastAPI()
                    app.include_router(module.router)
                    auth = gemini.authenticate_gemini_flexible if module is gemini else module.authenticate_bearer
                    app.dependency_overrides[auth] = lambda: "synthetic"
                    request_path = path.replace(":generateContent", ":streamGenerateContent") if streaming and module is gemini else path
                    request_body = body if module is gemini else {**body, "stream": streaming}
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
                        response = await client.post(request_path, json=request_body)
                    with self.subTest(module=module.__name__, streaming=streaming):
                        self.assertEqual(response.status_code, 503)
                        self.assertEqual(response.headers.get("retry-after"), "1200")
            post.assert_not_awaited()
            stream.assert_not_called()

    async def test_manual_panel_test_cannot_interrupt_idle(self):
        await self.trip()
        with patch.object(cooldown, "CooldownStore", return_value=self.store):
            with self.assertRaises(cooldown.PoolCooling):
                await cooldown.acquire_test_dispatch(self.email, "claude-sonnet")
        self.assertEqual((await self.state())["leases"], {})

    async def test_successful_stream_probe_only_reopens_after_final_delivery(self):
        self.patch_api()
        await self.trip()
        self.now += 1200
        delivery = StreamDelivery()
        ctx = current_delivery.set(delivery)
        async def upstream(**kwargs):
            yield 'data: ' + json.dumps({"response": {"candidates": [{"content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"}]}}) + '\n\n'
        try:
            with patch.object(cooldown, "CooldownStore", return_value=self.store), patch.object(api, "stream_post_async", upstream):
                async for chunk in api.stream_request({"model": "gemini-2.5-flash", "request": {"contents": []}}):
                    delivery.sent({"body": chunk.encode(), "more_body": True})
                self.assertEqual((await self.state())["groups"]["*"]["phase"], "probe")
                await delivery.finalize()
                state = await self.state()
                self.assertEqual(state["groups"]["*"]["phase"], "closed")
                self.assertEqual(state["leases"], {})
        finally:
            current_delivery.reset(ctx)

    async def test_lost_ownership_and_renewal_error_cancel_the_transport(self):
        for raises in (False, True):
            key, _ = await self.admit(f"renew-{raises}")
            leases = []
            async def transport():
                lease = cooldown.Lease(self.store, self.email, key)
                leases.append(lease)
                try:
                    await asyncio.Event().wait()
                finally:
                    await lease.result()
                    await lease.close()
            renewal = AsyncMock(side_effect=RuntimeError("synthetic storage outage")) if raises else AsyncMock(return_value=False)
            with patch.object(self.store, "renew", renewal), patch.object(cooldown, "RENEW_SECONDS", .005):
                task = asyncio.create_task(transport())
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 1)
            self.assertTrue(leases[0].lost)
            self.assertTrue(leases[0].heartbeat.done())
            self.assertEqual((await self.state())["leases"], {})

    async def custom_groups(self, rules, **options):
        await self.settings(scope="family", custom_families_enabled=True,
                            family_rules=rules, threshold=1, **options)

    async def test_custom_groups_split_builtin_family_and_merge_other_models(self):
        await self.custom_groups([{"name": "高档", "patterns": ["gemini-4-argon-high*", "claude-*"]}])
        await self.qualifying_failure("high-failure", "gemini-4-argon-high")
        self.assertIsNone((await self.admit("same", "gemini-4-argon-high-d"))[0])
        self.assertIsNone((await self.admit("merged", "claude-sonnet"))[0])
        self.assertIsNotNone((await self.admit("separate", "gemini-4-argon-medium"))[0])
        await self.db.store_credential("other@example.invalid", self.cred)
        self.assertIsNotNone((await self.store.admit("other@example.invalid", "claude-sonnet", "other"))[0])
        item = (await self.store.status())["items"][0]
        self.assertEqual(item["group_label"], "高档（自定义）")

    async def test_custom_rules_never_bypass_existing_quota_cooldown(self):
        await self.custom_groups([{"name": "高档", "patterns": ["*argon*"]}])
        await self.db.set_model_cooldown(self.email, "gemini-4-argon", self.now + 300)
        self.assertIsNone((await self.admit("quota", "gemini-4-argon-high"))[0])
        self.assertIsNotNone((await self.admit("unrelated", "claude-sonnet"))[0])

    async def test_family_rule_edit_regroups_inflight_and_fences_old_success(self):
        await self.settings(scope="family")
        old, _ = await self.admit("old", "claude-sonnet")
        await self.custom_groups([{"name": "共享", "patterns": ["*argon*", "claude-*"]}])
        await self.qualifying_failure("trigger", "gemini-4-argon-high")
        group = (await self.state())["groups"]["custom:共享"]
        self.assertEqual(group["phase"], "draining")
        self.now += 10
        await self.store.finish(self.email, old, success=True)
        group = (await self.state())["groups"]["custom:共享"]
        self.assertEqual((group["phase"], group["until"]), ("cooling", self.now + 1200))

    async def test_legacy_inflight_leases_are_conservatively_drained_after_rule_edit(self):
        await self.settings(scope="family")
        old, _ = await self.admit("legacy", "claude-sonnet")
        async with self.db.session() as session:
            row = await session.get(Credential499State, self.email)
            state = json.loads(row.state)
            del state["leases"][old]["model"]
            row.state = json.dumps(state)
        await self.custom_groups([{"name": "新组", "patterns": ["*argon*"]}])
        await self.qualifying_failure("trigger")
        self.assertEqual((await self.state())["groups"]["custom:新组"]["phase"], "draining")
        await self.store.finish(self.email, old)
        self.assertEqual((await self.state())["groups"]["custom:新组"]["phase"], "cooling")

    async def test_family_fallback_and_disable_hot_reload_in_other_worker(self):
        await self.custom_groups([], family_fallback="model")
        await self.qualifying_failure("high", "gemini-4-argon-high")
        self.assertIsNotNone((await self.admit("medium", "gemini-4-argon-medium"))[0])
        await self.settings(custom_families_enabled=False)
        other = Database()
        await other.initialize()
        try:
            store = cooldown.CooldownStore(other, lambda: self.now)
            key, _ = await store.admit(self.email, "gemini-4-argon-high", "new-high")
            await store.finish(self.email, key, cancelled_empty=True, elapsed=100)
            self.assertIsNone((await self.admit("same-default", "gemini-4-argon-low"))[0])
        finally:
            await other.close()

    async def test_family_settings_roundtrip_and_reordering_changes_priority(self):
        rules = [{"name": "高档", "patterns": ["*argon-high*"]}, {"name": "其他Argon", "patterns": ["*argon*"]}]
        await self.custom_groups(rules)
        before = await cooldown.public_settings(self.db)
        self.assertEqual(before["cooldown_499_family_rules"], rules)
        await self.qualifying_failure("high", "gemini-4-argon-high")
        await self.settings(family_rules=rules[::-1])
        key, _ = await self.admit("changed", "gemini-4-argon-high")
        state = await self.state()
        self.assertEqual(state["leases"][key]["group"], "custom:其他Argon")
        self.assertNotIn("custom:高档", state["groups"])

    async def test_family_preview_auth_validation_and_no_persistence(self):
        from src.panel import config_routes
        from src.utils import verify_panel_token
        app = FastAPI()
        app.include_router(config_routes.router)
        body = {"model": "gemini-4-argon-high", "config": {
            "cooldown_499_scope": "family", "cooldown_499_custom_families_enabled": True,
            "cooldown_499_family_rules": [{"name": "预览", "patterns": ["*argon-high*"]}]}}
        before = await cooldown.public_settings(self.db)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            self.assertEqual((await client.post("/config/cooldown499/resolve", json=body)).status_code, 401)
            app.dependency_overrides[verify_panel_token] = lambda: "synthetic"
            result = await client.post("/config/cooldown499/resolve", json=body)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()["label"], "预览（自定义）")
            for bad in ({**body, "model": " "}, {**body, "config": {"unknown": 1}},
                        {**body, "config": {"cooldown_499_family_rules": "bad"}}):
                self.assertEqual((await client.post("/config/cooldown499/resolve", json=bad)).status_code, 400)
        self.assertEqual(await cooldown.public_settings(self.db), before)
        async with self.db.session() as session:
            self.assertEqual(list(await session.scalars(select(Credential499State))), [])

    async def test_invalid_group_edit_cannot_partially_save_other_settings(self):
        before = await cooldown.public_settings(self.db)
        with self.assertRaises(ValueError):
            await self.settings(mode="off", family_rules=[{"name": "missing-patterns"}])
        self.assertEqual(await cooldown.public_settings(self.db), before)


class FamilyMatchingTests(unittest.TestCase):
    def test_exact_wildcard_case_priority_and_reserved_names(self):
        cfg = {"custom_families_enabled": True, "family_fallback": "default", "family_rules": [
            {"name": "*", "patterns": ["GEMINI-4-ARGON-HIGH", "claude-?"]},
            {"name": "argon", "patterns": ["*argon*"]},
        ]}
        self.assertEqual(cooldown.resolve_family("gemini-4-argon-high", cfg), "custom:*")
        self.assertEqual(cooldown.resolve_family("claude-x", cfg), "custom:*")
        self.assertEqual(cooldown.resolve_family("gemini-4-argon-high-d", cfg), "custom:argon")
        self.assertEqual(cooldown.resolve_family("gemini-2.5-flash", cfg), "gemini")
        self.assertEqual(cooldown.resolve_family("claude-xx", cfg), "claude_gpt")
        self.assertEqual(cooldown.resolve_family("custom:argon", cfg), "custom:argon")  # explicitly matches *argon*
        self.assertEqual(cooldown.resolve_family("*", cfg), "model:*")
        cfg["family_fallback"] = "model"
        self.assertEqual(cooldown.resolve_family("GPT-X", cfg), "model:gpt-x")
        cfg["custom_families_enabled"] = False
        self.assertEqual(cooldown.resolve_family("gemini-4-argon-high", cfg), "argon")

    def test_group_validation_rejects_ambiguous_or_unbounded_input(self):
        bad_rules = [None, {}, "x", [None], [{"name": "g", "patterns": []}],
            [{"name": " g", "patterns": ["*"]}], [{"name": "", "patterns": ["*"]}],
            [{"name": "g", "patterns": "*"}], [{"name": "g", "patterns": [True]}],
            [{"name": "g", "patterns": ["bad rule"]}], [{"name": "g", "patterns": ["[ab]"]}],
            [{"name": "g", "patterns": ["x" * 129]}], [{"name": "g", "patterns": ["*"] * 33}],
            [{"name": "G", "patterns": ["x"]}, {"name": "g", "patterns": ["y"]}],
            [{"name": str(i), "patterns": ["x"]} for i in range(33)]]
        for rules in bad_rules:
            with self.subTest(rules=rules), self.assertRaises(ValueError):
                cooldown.validate_settings({"cooldown_499_family_rules": rules})
        for value in (1, "true", None):
            with self.assertRaises(ValueError):
                cooldown.validate_settings({"cooldown_499_custom_families_enabled": value})
        with self.assertRaises(ValueError):
            cooldown.validate_settings({"cooldown_499_family_fallback": "skip"})


if __name__ == "__main__":
    unittest.main()
