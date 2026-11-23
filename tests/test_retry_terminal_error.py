"""The final response and final usage row must describe the same retry outcome."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock, patch
from contextlib import ExitStack
import httpx
from fastapi import Response
from src.api import antigravity as api
from src.credential_cooldown import DispatchLimited
import test_custom_audit
from src.api import utils as api_utils


class RetryTerminalErrorTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_transport_retry_releases_dispatch_before_wait(self):
        body = {"model": "gemini-flash", "request": {"contents": []}}
        answer = {"candidates": [{"content": {"parts": [{"text": "ok"}]}, "finishReason": "STOP"}]}
        for streaming in (False, True):
            leases, closed_at_wait = [], []

            async def admit(candidate, *args):
                lease = Mock(close=AsyncMock(), result=AsyncMock(), iterate=lambda source: source)
                leases.append(lease)
                return candidate, lease

            async def sleep(delay):
                if delay == 7:
                    closed_at_wait.append(leases[0].close.await_count)

            async def stream(**kwargs):
                if len(leases) == 1:
                    raise httpx.ReadError("connection closed")
                yield "data: " + json.dumps(answer)

            with ExitStack() as stack:
                stack.enter_context(patch.object(api, "acquire_dispatch", admit))
                stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                    "max_retries": 1, "retry_enabled": True, "retry_interval": 7})))
                stack.enter_context(patch.object(api.asyncio, "sleep", sleep))
                if streaming:
                    stack.enter_context(patch.object(api, "stream_post_async", stream))
                    chunks = [item async for item in api.stream_request(body)]
                    self.assertIn("ok", chunks[-1])
                else:
                    stack.enter_context(patch.object(api, "post_async", AsyncMock(side_effect=[
                        httpx.ReadError("connection closed"), httpx.Response(200, json=answer)])))
                    self.assertEqual((await api.non_stream_request(body)).status_code, 200)
            self.assertEqual(closed_at_wait, [1])
            self.assertEqual(len(leases), 2)

    async def test_alternatives_at_capacity_still_require_admission_of_original(self):
        candidate = ("a", {"access_token": "a"})
        selector = AsyncMock(side_effect=DispatchLimited(12))
        for allowed in (True, False):
            with self.subTest(allowed=allowed):
                admit = AsyncMock(return_value=(candidate, "lease"), side_effect=None if allowed else DispatchLimited(35))
                with patch.object(api, "acquire_dispatch", admit):
                    if allowed:
                        self.assertEqual(await api._dispatch_attempt(candidate, "model", None, "request", selector, {"a"}),
                                         (candidate, "lease"))
                    else:
                        with self.assertRaises(DispatchLimited) as raised:
                            await api._dispatch_attempt(candidate, "model", None, "request", selector, {"a"})
                        self.assertEqual(raised.exception.retry_after, 35)
                admit.assert_awaited_once()
                self.assertEqual(admit.await_args.args[0], candidate)

    async def test_model_miss_excludes_only_the_account_that_reported_it(self):
        await self.check_retry_order(["a", "b"], ["disconnect", 404], ["a", "b", "a"])

    async def test_retry_after_empty_response_prefers_unused_credential(self):
        await self.check_retry_order(["a", "b"], ["empty"], ["a", "b"])

    async def test_retry_after_two_internal_errors_prefers_third_credential(self):
        await self.check_retry_order(["a", "b", "c"], [500, 500], ["a", "b", "c"])

    async def check_retry_order(self, pool, failures, expected_calls):
        body = {"model": "gemini-flash", "request": {"contents": []}}
        answer = {"candidates": [{"content": {"parts": [{"text": "ok"}]}, "finishReason": "STOP"}]}
        for streaming in (False, True):
            called = []

            async def select(model_name=None, session_key=None, exclude_emails=(), **kwargs):
                for email in pool:
                    if email not in (exclude_emails or ()):
                        return email, {"access_token": email, "project_id": email}
                return None

            async def post(**kwargs):
                called.append(kwargs["credential_email"])
                if len(called) <= len(failures):
                    failure = failures[len(called) - 1]
                    if failure == "disconnect":
                        raise httpx.ReadError("interrupted")
                    if failure == "empty":
                        return httpx.Response(200, content=b"")
                    return httpx.Response(failure, json={"error": {"code": failure}})
                return httpx.Response(200, json=answer)

            async def source(**kwargs):
                response = await post(**kwargs)
                if response.status_code == 200:
                    if response.content:
                        yield "data: " + response.text
                else:
                    yield Response(status_code=response.status_code, content=response.content)

            with ExitStack() as stack:
                stack.enter_context(patch.object(api, "get_valid_credential", select))
                stack.enter_context(patch.object(api, "_record_model_not_found", AsyncMock()))
                stack.enter_context(patch.object(api, "record_api_call_error", AsyncMock()))
                stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                    "max_retries": 2, "retry_enabled": True, "retry_interval": 0})))
                if streaming:
                    stack.enter_context(patch.object(api, "stream_post_async", source))
                    chunks = [item async for item in api.stream_request(body)]
                    self.assertIn("ok", chunks[-1])
                else:
                    stack.enter_context(patch.object(api, "post_async", post))
                    self.assertEqual((await api.non_stream_request(body)).status_code, 200)
            self.assertEqual(called, expected_calls)

    async def test_input_resource_and_late_404_do_not_invalidate_the_model_or_retry(self):
        resource = {"@type": "type.googleapis.com/google.rpc.ResourceInfo",
                    "resourceType": "generativelanguage.googleapis.com/File"}
        error = {"code": 404, "message": "missing input", "details": [resource]}
        partial = {"error": {"code": 404, "message": "late error"},
                   "candidates": [{"content": {"parts": [{"text": "partial output"}]}}]}
        body = {"model": "gemini-4-argon-medium", "request": {"contents": []}}
        for streaming in (False, True):
            for status, payload in [(404, {"error": error}), (200, partial)]:
                with self.subTest(streaming=streaming, upstream_status=status), ExitStack() as stack:
                    stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                        "max_retries": 2, "retry_enabled": True, "retry_interval": 0
                    })))
                    mark = stack.enter_context(patch.object(api, "_record_model_not_found", AsyncMock()))
                    if streaming:
                        calls = []
                        async def stream(**kwargs):
                            calls.append(kwargs)
                            if status == 404:
                                yield Response(status_code=404, content=json.dumps(payload))
                            else:
                                yield "data: " + json.dumps(payload)
                        stack.enter_context(patch.object(api, "stream_post_async", stream))
                        chunks = [chunk async for chunk in api.stream_request(body)]
                        response = chunks[-1]
                        self.assertEqual(len(calls), 1)
                    else:
                        post = stack.enter_context(patch.object(api, "post_async", AsyncMock(
                            return_value=httpx.Response(status, json=payload)
                        )))
                        response = await api.non_stream_request(body)
                        post.assert_awaited_once()
                    self.assertEqual(response.status_code, 404)
                    self.assertEqual(json.loads(response.body)["error"], payload["error"])
                    mark.assert_not_awaited()

    def test_unknown_and_model_resource_404_keep_model_failover(self):
        for kind in ("generativelanguage.googleapis.com/Model", None):
            details = [{"@type": "type.googleapis.com/google.rpc.ResourceInfo", "resourceType": kind}]
            self.assertTrue(api_utils.is_model_not_found(json.dumps({"error": {"details": details}})))
        self.assertTrue(api_utils.is_model_not_found("generic upstream 404"))

    async def test_cooldown_parser_falls_through_invalid_hints(self):
        err_info = "type.googleapis.com/google.rpc.ErrorInfo"
        retry_info = "type.googleapis.com/google.rpc.RetryInfo"
        details = [None, {"@type": err_info, "metadata": {"quotaResetTimeStamp": "invalid", "quotaResetDelay": "-8s"}},
                   {"@type": retry_info, "retryDelay": "1m2.5s"}]
        with patch.object(api_utils.time, "time", return_value=1000):
            payload = json.dumps({"error": {"details": details}})
            for prefix in ("", "\ufeff"):
                for header in (None, "10"):
                    self.assertEqual(await api_utils.parse_and_log_cooldown(prefix + payload, header), 1062.5)
            self.assertEqual(await api_utils.parse_and_log_cooldown("", "Thu, 01 Jan 1970 00:17:00 GMT"), 1020)
            self.assertEqual(await api_utils.parse_and_log_cooldown("bad JSON", "12"), 1012)
            self.assertIsNone(await api_utils.parse_and_log_cooldown("", "-8"))

    async def test_empty_generation_preserves_502_when_retry_has_no_credential(self):
        body = {"model": "gemini-flash", "request": {"contents": []}}
        for streaming in (False, True):
            for status in (200, 204, 205):
                with self.subTest(streaming=streaming, status=status), ExitStack() as stack:
                    self.saved.reset_mock()
                    stack.enter_context(patch.object(api, "get_valid_credential", AsyncMock(
                        side_effect=[("only", {"access_token": "synthetic", "project_id": "p"}), None])))
                    stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                        "retry_enabled": True, "max_retries": 1, "retry_interval": 0})))
                    if streaming:
                        async def upstream(**kwargs):
                            if status != 200:
                                yield Response(status_code=status)
                        stack.enter_context(patch.object(api, "stream_post_async", upstream))
                        response, = [chunk async for chunk in api.stream_request(body)]
                    else:
                        post = stack.enter_context(patch.object(api, "post_async", AsyncMock(
                            return_value=httpx.Response(status, content=b""))))
                        response = await api.non_stream_request(body)
                        post.assert_awaited_once()
                    self.assertEqual(response.status_code, 502)
                    self.assertEqual(json.loads(response.body)["error"]["status"], "UPSTREAM_EMPTY_RESPONSE")
                    self.saved.assert_awaited_once()
                    self.assertEqual(self.saved.await_args.args[0]["status_code"], 502)

    async def test_retry_preserves_last_rejection_when_no_replacement(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming), ExitStack() as stack:
                picker = stack.enter_context(patch.object(api, "get_valid_credential", AsyncMock(
                    side_effect=[("only", {"access_token": "synthetic", "project_id": "p"}), None, None])))
                stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                    "retry_enabled": True, "max_retries": 1, "retry_interval": 0})))
                record = stack.enter_context(patch.object(api, "record_api_call_error", AsyncMock()))
                stack.enter_context(patch.object(api_utils, "check_should_auto_ban", AsyncMock(return_value=False)))
                headers = {"retry-after": "30"}
                body = {"model": "gemini-flash", "request": {"contents": []}}
                if streaming:
                    async def source(**kwargs):
                        yield Response(status_code=429, content="busy", headers=headers)
                    stack.enter_context(patch.object(api, "stream_post_async", source))
                    response, = [item async for item in api.stream_request(body)]
                else:
                    stack.enter_context(patch.object(api, "post_async", AsyncMock(return_value=
                        httpx.Response(429, content="busy", headers=headers))))
                    response = await api.non_stream_request(body)
                self.assertEqual((response.status_code, response.body, response.headers.get("retry-after")),
                                 (429, b"busy", "30"))
                self.assertIsNotNone(record.await_args.args[2])
                self.assertIn("only", picker.await_args_list[1].kwargs["exclude_emails"])

    async def test_transient_status_retries_once_after_one_configured_delay(self):
        for status in (500, 503):
            for streaming in (False, True):
                with self.subTest(status=status, streaming=streaming), ExitStack() as stack:
                    stack.enter_context(patch.object(api, "get_retry_config", AsyncMock(return_value={
                        "retry_enabled": True, "max_retries": 1, "retry_interval": 7})))
                    stack.enter_context(patch.object(api, "record_api_call_error", AsyncMock()))
                    stack.enter_context(patch.object(api_utils, "check_should_auto_ban", AsyncMock(return_value=False)))
                    sleep = stack.enter_context(patch.object(asyncio, "sleep", AsyncMock()))
                    body = {"model":"gemini-flash", "request":{"contents":[]}}
                    data = '{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}'
                    if streaming:
                        answers = iter([Response(status_code=status, content="busy"), "data: " + data])
                        async def source(**kwargs):
                            yield next(answers)
                        stack.enter_context(patch.object(api, "stream_post_async", source))
                        items = [item async for item in api.stream_request(body)]
                        self.assertTrue(any(isinstance(item, str) and "ok" in item for item in items))
                    else:
                        stack.enter_context(patch.object(api, "post_async", AsyncMock(side_effect=[
                            httpx.Response(status, text="busy"), httpx.Response(200, text=data)])))
                        self.assertEqual((await api.non_stream_request(body)).status_code, 200)
                    self.assertEqual([c.args for c in sleep.await_args_list if c.args == (7,)], [(7,)])

    async def test_transport_failure_after_retry_does_not_return_stale_http_error(self):
        for streaming in (False, True):
            for first_status in (429, 503):
                with self.subTest(streaming=streaming, first_status=first_status):
                    self.saved.reset_mock()
                    calls = 0
                    async def stream(**kwargs):
                        nonlocal calls
                        calls += 1
                        if calls == 1:
                            yield Response(status_code=first_status, content='{"error":{"message":"first attempt only"}}')
                        else:
                            raise httpx.ConnectError('synthetic final connection failure')
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={'max_retries':1,'retry_enabled':True,'retry_interval':0})))
                        stack.enter_context(patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=True)))
                        stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                        body={'model':'gemini-4-argon-medium','request':{'contents':[]}}
                        if streaming:
                            stack.enter_context(patch.object(api, 'stream_post_async', stream))
                            results=[item async for item in api.stream_request(body)]
                            self.assertEqual(len(results),1)
                            response=results[0]
                            self.assertEqual(calls,2)
                        else:
                            post=stack.enter_context(patch.object(api, 'post_async', AsyncMock(side_effect=[
                                httpx.Response(first_status, json={'error':{'message':'first attempt only'}}),
                                httpx.ConnectError('synthetic final connection failure'),
                            ])))
                            response=await api.non_stream_request(body)
                            self.assertEqual(post.await_count,2)
                    rows=[call.args[0] for call in self.saved.await_args_list]
                    self.assertEqual([row['status_code'] for row in rows], [first_status,500])
                    self.assertEqual(response.status_code,rows[-1]['status_code'])
                    self.assertNotIn(b'first attempt only',response.body)
