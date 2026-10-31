"""Local feature regressions, isolated from credentials and external model APIs."""
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.panel import creds, usage_logs
from src.storage.database import Database, UsageLog, AntigravityCredential
from src.api import antigravity as api
from src import request_body_log
from src.usage_log import extract_response_id


class FeaturesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.db = Database()
        self.db._engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as conn:
            await conn.run_sync(self.db._create_tables)
        self.app = FastAPI()
        for module in (creds, usage_logs):
            self.app.include_router(module.router)
            self.stack.enter_context(patch.object(module, "get_database", AsyncMock(return_value=self.db)))
        self.app.dependency_overrides[creds.verify_panel_token] = lambda: "test"
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test")
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(patch.object(request_body_log, "REQUEST_LOG_DIR", self.tmp))

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.db.close()

    async def test_note_persists_through_refresh_and_only_changes_note(self):
        email = "note@example.invalid"
        await self.db.store_credential(email, {"refresh_token": "synthetic", "tier": "ultra_lite"})
        await self.db.update_credential_state(email, {"disabled": True, "model_cooldowns": {"argon": 9999999999}, "error_codes": [403]})
        before = await self.db.get_credential_state(email)
        response = await self.client.patch(f"/creds/note/{email}", json={"note": "  中文备注\n第二行  "})
        self.assertEqual(response.status_code, 200)
        after = await self.db.get_credential_state(email)
        self.assertEqual(after, {**before, "note": "中文备注\n第二行"})
        await self.db.store_credential(email, {"access_token": "new-synthetic", "tier": "pro"})
        self.assertEqual((await self.db.get_credential_state(email))["note"], "中文备注\n第二行")
        summary = await self.db.get_credentials_summary()
        self.assertEqual(summary["items"][0]["note"], "中文备注\n第二行")
        await self.client.patch(f"/creds/note/{email}", json={"note": ""})
        self.assertEqual((await self.db.get_credential_state(email))["note"], "")

    async def test_note_validation_missing_and_auth(self):
        for value in (123, None, "x" * 1001):
            res = await self.client.patch("/creds/note/missing", json={"note": value})
            self.assertEqual(res.status_code, 422)
        self.assertEqual((await self.client.patch("/creds/note/missing", json={"note": "abc"})).status_code, 404)
        self.app.dependency_overrides.clear()
        for method, path, kwargs in [
            ("PATCH", "/creds/note/missing", {"json": {"note": "abc"}}),
            ("GET", "/usage-logs/lookup?identifier=test", {}),
            ("GET", "/usage-logs/request-body?path=test.json", {}),
        ]:
            self.assertIn((await self.client.request(method, path, **kwargs)).status_code, (401, 403))

    async def test_lookup_all_ids_retries_pagination_and_body(self):
        path = await request_body_log.record_request_body(request_id="request-a", response_id="response-a", model_name="argon", upstream_model_name="argon", credential_email="a@example.invalid", is_stream=True, status_code=200, url="https://example.invalid", headers={"Authorization": "secret"}, payload={"contents": [{"text": "中文"}]})
        for response_id, code in [("response-a", 200), ("response-b", 400)]:
            await self.db.insert_usage_log({"request_id": "request-a", "response_id": response_id, "model_name": "argon", "success": code == 200, "status_code": code, "request_log_file": path})
        for kind, identifier, expected in [("auto", "request-a", 2), ("request_id", "request-a", 2), ("response_id", "response-a", 1), ("auto", "response-b", 1), ("log_id", "1", 1), ("auto", "1", 1), ("auto", "no-match", 0)]:
            response = await self.client.get("/usage-logs/lookup", params={"identifier": identifier, "id_type": kind, "limit": 1})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["total"], expected)
            self.assertEqual(len(response.json()["items"]), min(1, expected))
        a = (await self.client.get("/usage-logs/lookup", params={"identifier": "request-a", "limit": 1, "offset": 1})).json()
        self.assertEqual(a["items"][0]["response_id"], "response-a")
        body = await self.client.get("/usage-logs/request-body", params={"path": a["items"][0]["request_log_file"]})
        self.assertEqual(body.json()["request_body"], {"contents": [{"text": "中文"}]})
        self.assertEqual(body.json()["request_headers"]["Authorization"], "(已脱敏)")
        self.assertEqual((await self.client.get("/usage-logs/request-body", params={"path": "../../missing"})).status_code, 404)

    async def test_lookup_invalid_and_numeric_collisions(self):
        await self.db.insert_usage_log({"request_id": "first", "success": True})
        await self.db.insert_usage_log({"request_id": "1", "success": False})
        for kind, expected in [("auto", 2), ("log_id", 1), ("request_id", 1)]:
            self.assertEqual((await self.client.get("/usage-logs/lookup", params={"identifier": "1", "id_type": kind})).json()["total"], expected)
        for params in [{"identifier": " "}, {"identifier": "9" * 30, "id_type": "log_id"}, {"identifier": "-1", "id_type": "log_id"}, {"identifier": "a", "id_type": "bad"}, {"identifier": "a", "offset": -1}]:
            self.assertIn((await self.client.get("/usage-logs/lookup", params=params)).status_code, (400, 422))

    async def test_token_groups_and_summary_filters(self):
        await self.db.store_credential("a@example.invalid", {"tier": "ultra"})
        await self.db.update_credential_state("a@example.invalid", {"note": "测试统计"})
        for email, model, timestamp, prompt, completion in [("a@example.invalid", "medium", 100, 1500000, 250000), ("a@example.invalid", "medium", 101, 500000, 100000), ("b@example.invalid", "high", 102, 3000000, 600000)]:
            await self.db.insert_usage_log({"credential_email": email, "model_name": model, "created_at": timestamp, "success": True, "prompt_tokens": prompt, "completion_tokens": completion, "cached_tokens": 100000})
        all_rows = await self.db.credential_usage_stats(limit=1)
        self.assertEqual(all_rows["summary"]["prompt_tokens"], 5000000)
        self.assertEqual(all_rows["summary"]["completion_tokens"], 950000)
        self.assertEqual(all_rows["items"][0]["prompt_tokens"], 2000000)
        self.assertEqual(all_rows["items"][0]["credential_note"], "测试统计")
        filtered = await self.db.credential_usage_stats(credential_email="a@example.invalid", model_name="medium", start_timestamp=101)
        self.assertEqual(filtered["summary"]["prompt_tokens"], 500000)
        self.assertEqual(filtered["summary"]["completion_tokens"], 100000)
        empty = await self.db.credential_usage_stats(start_timestamp=1000)
        self.assertEqual(empty["summary"]["prompt_tokens"], 0)

    async def test_additive_migration_preserves_data_and_is_idempotent(self):
        await self.db.store_credential("old@example.invalid", {"refresh_token": "keep", "tier": "ultra_lite"})
        await self.db.update_credential_state("old@example.invalid", {"disabled": True, "error_codes": [403], "model_cooldowns": {"argon": 9999999999}})
        await self.db.insert_usage_log({"request_id": "old-request", "success": True, "prompt_tokens": 12345})
        async with self.db._engine.begin() as conn:
            await conn.execute(text("DROP INDEX idx_ul_response_id"))
            await conn.execute(text("DROP INDEX idx_ul_request_id"))
            await conn.execute(text("ALTER TABLE usage_log DROP COLUMN response_id"))
            await conn.execute(text("ALTER TABLE antigravity_credentials DROP COLUMN note"))
            await conn.run_sync(self.db._create_tables)
            await conn.run_sync(self.db._create_tables)
            indices = await conn.run_sync(lambda c: {i["name"] for i in inspect(c).get_indexes("usage_log")})
        self.assertTrue({"idx_ul_response_id", "idx_ul_request_id"} <= indices)
        state = await self.db.get_credential_state("old@example.invalid")
        self.assertEqual(state["note"], "")
        self.assertTrue(state["disabled"])
        self.assertEqual(state["model_cooldowns"], {"argon": 9999999999})
        self.assertEqual((await self.db.get_credential("old@example.invalid"))["refresh_token"], "keep")
        rows, total = await self.db.query_usage_logs(identifier="old-request")
        self.assertEqual(total, 1)
        self.assertEqual((rows[0]["response_id"], rows[0]["prompt_tokens"]), ("", 12345))


class CaptureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.tmp = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.stack.enter_context(patch.object(request_body_log, "REQUEST_LOG_DIR", self.tmp))
        for name, value in {
            "get_request_log_enabled": True, "get_request_log_all_requests": True,
            "get_request_log_error_codes": [400],
            "get_valid_credential": ("a@example.invalid", {"access_token": "synthetic", "project_id": "project"}),
            "get_antigravity_api_url": "https://example.invalid", "get_auto_ban_error_codes": [],
            "get_retry_config": {"max_retries": 0, "retry_interval": 0, "retry_enabled": False},
            "record_api_call_success": None, "record_api_call_error": None,
            "wrap_cli_request": ({"model": "synthetic-model", "request": {"contents": [{"text": "中文 request"}]}}, None),
        }.items():
            self.stack.enter_context(patch.object(api, name, AsyncMock(return_value=value)))
        self.saved = self.stack.enter_context(patch("src.usage_log.record_usage_log", AsyncMock()))
        self.body = {"model": "synthetic-model", "request": {"contents": []}}

    def assert_capture(self, response_id, success):
        row = self.saved.call_args.args[0]
        self.assertEqual((row["response_id"], row["success"]), (response_id, success))
        detail = request_body_log.read_request_body_log(row["request_log_file"])
        self.assertEqual(detail["response_id"], response_id)
        self.assertEqual(detail["credential_email"], row["credential_email"])
        self.assertEqual(detail["request_body"]["request"]["contents"], [{"text": "中文 request"}])
        self.assertEqual(detail["request_headers"]["Authorization"], "(已脱敏)")
        return row, detail

    async def test_non_stream_success_and_error_captured(self):
        for code in (200, 400):
            response = {"response": {"responseId": f"upstream-{code}", "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 20, "thoughtsTokenCount": 5, "cachedContentTokenCount": 40}}}
            with patch.object(api, "post_async", AsyncMock(return_value=httpx.Response(code, json=response))):
                result = await api.non_stream_request(self.body, log_ctx={"request_id": "req-test"})
            self.assertEqual(result.status_code, code)
            row, detail = self.assert_capture(f"upstream-{code}", code == 200)
            if code == 200:
                self.assertEqual((row["prompt_tokens"], row["completion_tokens"], row["cached_tokens"]), (100, 25, 40))
            else:
                self.assertIn("upstream-400", detail["response_body"])

    async def test_stream_response_id_and_last_usage_preserved(self):
        async def stream(**kwargs):
            for n in (1, 20):
                yield "data: " + json.dumps({"response": {"responseId": "stream-id", "candidates": [{"finishReason": "STOP" if n == 20 else "FINISH_REASON_UNSPECIFIED", "content": {"parts": [{"text": "answer"}]}}], "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": n, "thoughtsTokenCount": 5}}}) + "\n\n"
        with patch.object(api, "stream_post_async", stream):
            chunks = [x async for x in api.stream_request(self.body)]
        self.assertEqual(len(chunks), 2)
        row, _ = self.assert_capture("stream-id", True)
        self.assertEqual(row["completion_tokens"], 25)

    async def test_retry_records_actual_credential_and_clears_old_error(self):
        for streamed in (False, True):
            self.saved.reset_mock()
            credentials = [(f"{name}@example.invalid", {"access_token": name, "project_id": name}) for name in ("first", "second")]
            count = 0
            async def stream(**kwargs):
                nonlocal count
                count += 1
                if count == 1:
                    yield Response(status_code=429, content='{"error":"synthetic retry"}')
                else:
                    yield 'data: {"response":{"responseId":"retry-success","candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"ok"}]}}]}}\n\n'
            with ExitStack() as stack:
                stack.enter_context(patch.object(api, "get_valid_credential", AsyncMock(side_effect=credentials)))
                stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={"max_retries": 1, "retry_interval": 0, "retry_enabled": True})))
                stack.enter_context(patch.object(api, "handle_error_with_retry", AsyncMock(return_value=True)))
                stack.enter_context(patch.object(api, "parse_and_log_cooldown", AsyncMock(return_value=None)))
                if streamed:
                    stack.enter_context(patch.object(api, "stream_post_async", stream))
                    [x async for x in api.stream_request(self.body)]
                else:
                    stack.enter_context(patch.object(api, "post_async", AsyncMock(side_effect=[httpx.Response(429, json={"error":"synthetic retry"}), httpx.Response(200, json={"responseId":"retry-success"})])))
                    await api.non_stream_request(self.body)
            rows = [call.args[0] for call in self.saved.call_args_list]
            self.assertEqual(len(rows), 2)
            details = [request_body_log.read_request_body_log(row["request_log_file"]) for row in rows]
            self.assertEqual([d["credential_email"] for d in details], [c[0] for c in credentials])
            self.assertEqual(details[1]["response_body"], "")
            self.assertEqual(details[1]["response_id"], "retry-success")
            self.assertIn("synthetic retry", details[0]["response_body"])

    async def test_error_only_and_disabled_capture_modes(self):
        kwargs = dict(url="", headers={}, payload={}, response_body="", is_stream=False)
        with patch.object(api, "get_request_log_all_requests", AsyncMock(return_value=False)):
            self.assertEqual(await api._record_request_body_detail(200, "", {}, **kwargs), "")
            self.assertTrue(await api._record_request_body_detail(400, "bad", {}, **kwargs))
        with patch.object(api, "get_request_log_enabled", AsyncMock(return_value=False)):
            self.assertEqual(await api._record_request_body_detail(400, "bad", {}, **kwargs), "")

    async def test_early_failure_keeps_request_payload(self):
        with patch.object(api, "get_valid_credential", AsyncMock(return_value=None)):
            for streamed in (False, True):
                if streamed:
                    [x async for x in api.stream_request(self.body)]
                else:
                    await api.non_stream_request(self.body)
                row = self.saved.call_args.args[0]
                detail = request_body_log.read_request_body_log(row["request_log_file"])
                self.assertEqual(detail["request_body"], self.body)
                self.assertEqual(row["status_code"], 500)

    async def test_detail_files_unique_same_timestamp_and_request(self):
        with patch.object(request_body_log.time, "time", return_value=100):
            paths = [await api._record_request_body_detail(200, "", {"request_id": "same"}, url="", headers=None, payload={"attempt": i}, response_body="", is_stream=True) for i in range(2)]
        self.assertNotEqual(paths[0], paths[1])
        self.assertEqual([request_body_log.read_request_body_log(p)["request_body"]["attempt"] for p in paths], [0, 1])

    def test_response_id_parsing(self):
        for source in [{"responseId": "a"}, {"response": {"responseId": "a"}}, b'{"responseId":"a"}', 'data: {"responseId":"a"}\n\n']:
            self.assertEqual(extract_response_id(source), "a")
        for source in [None, "data: [DONE]", b"\xff", {"response": []}, {"responseId": 42}]:
            self.assertEqual(extract_response_id(source), "")
