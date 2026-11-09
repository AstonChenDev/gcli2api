"""Explicit upstream errors survive framing failures in buffered/adjacent text."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
import test_custom_audit
from test_thought_boundary import event, text_of
from src.api import antigravity as api
from src.router.antigravity import gemini, openai, anthropic

MARKER = '<<<AGY_FINAL_0123456789abcdef>>>'
BODY = {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}


class ErrorPrecedenceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_buffered_invalid_marker_cannot_replace_http_or_sse_error(self):
        for code in (400, 429, 503):
            for wire_format in ('http', 'sse'):
                with self.subTest(code=code, wire_format=wire_format):
                    self.saved.reset_mock()
                    error = {'code': code, 'status': 'ORIGINAL_UPSTREAM_ERROR', 'message': 'original reason'}
                    async def upstream(**kwargs):
                        yield event([{'text': '<<<AGY_FINAL_wrong'}])
                        yield Response(status_code=code, content=json.dumps({'error': error})) if wire_format == 'http' else 'data: ' + json.dumps({'error': error})
                    with patch.object(api, 'stream_post_async', upstream), patch.object(
                            api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)), patch.object(
                            api, 'record_api_call_error', AsyncMock()) as record_error:
                        out = [c async for c in api.stream_request(BODY)]
                    self.assertEqual(text_of(out), '')
                    self.assertEqual(out[-1].status_code, code)
                    self.assertEqual(json.loads(out[-1].body)['error'], error)
                    self.assertEqual(self.saved.await_count, 1)
                    self.assertEqual(self.saved.call_args.args[0]['status_code'], code)
                    self.assertFalse(self.saved.call_args.args[0]['success'])
                    self.assertEqual(record_error.call_args.args[1], code)
                    diagnostics = api._record_request_body_detail.call_args.kwargs['diagnostics']
                    self.assertTrue(diagnostics['boundary_failed'])
                    self.assertEqual(diagnostics['boundary_failure_reason'], 'unfinished_private_marker')

    async def test_same_event_error_or_block_retains_its_reason_over_invalid_text(self):
        for blocked in (False, True):
            self.saved.reset_mock()
            async def upstream(**kwargs):
                payload = json.loads(event([{'text': '<<<AGY_FINAL_invalid>>>'}], 'PROHIBITED_CONTENT' if blocked else None)[5:])
                if not blocked:
                    payload['response']['error'] = {'code': 429, 'message': 'original quota error'}
                yield 'data: ' + json.dumps(payload)
            with patch.object(api, 'stream_post_async', upstream), patch.object(
                    api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)), patch.object(
                    api, 'record_api_call_error', AsyncMock()):
                out = [c async for c in api.stream_request(BODY)]
            self.assertEqual(text_of(out), '')
            self.assertEqual(out[-1].status_code, 400 if blocked else 429)
            self.assertIn('PROHIBITED_CONTENT' if blocked else 'original quota error', out[-1].body.decode())
            self.assertEqual(self.saved.await_count, 1)

    async def test_retry_policy_still_recovers_after_buffered_invalid_text_and_503(self):
        calls = 0
        async def upstream(**kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                yield event([{'text': '<<<AGY_FINAL_wrong'}])
                yield Response(status_code=503, content='{"error":{"code":503,"message":"retry me"}}')
            else:
                yield event([{'text': MARKER + 'recovered answer'}], 'STOP')
        with ExitStack() as stack:
            stack.enter_context(patch.object(api, 'stream_post_async', upstream))
            stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)))
            stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
            stack.enter_context(patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=True)))
            stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={
                'retry_enabled': True, 'max_retries': 1, 'retry_interval': 0})))
            stack.enter_context(patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))))
            out = [c async for c in api.stream_request(BODY)]
        self.assertEqual(calls, 2)
        self.assertEqual(text_of(out), 'recovered answer')
        self.assertEqual([c.args[0]['status_code'] for c in self.saved.call_args_list], [503, 200])

    async def test_all_protocols_and_continuation_keep_explicit_error(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        requests = [('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents': [{'parts': [{'text': 'test'}]}]}),
                    ('/v1/chat/completions', {'model': 'gemini-4-argon-medium', 'stream': True, 'messages': [{'role': 'user', 'content': 'test'}]}),
                    ('/v1/messages', {'model': 'gemini-4-argon-medium', 'stream': True, 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}]})]
        for anti in (False, True):
            for visible_thought in (False, True):
                for url, body in requests:
                    with self.subTest(anti=anti, visible_thought=visible_thought, url=url):
                        self.saved.reset_mock()
                        async def upstream(**kwargs):
                            if visible_thought:
                                yield event([{'text': 'native thought', 'thought': True}])
                            yield event([{'text': '<<<AGY_FINAL_wrong'}])
                            yield Response(status_code=429, content='{"error":{"code":429,"status":"ORIGINAL_QUOTA","message":"original quota error"}}')
                        with ExitStack() as stack:
                            stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                            stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)))
                            stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                            for module in (gemini, openai, anthropic):
                                stack.enter_context(patch.object(module, 'is_anti_truncation_model', return_value=anti))
                                stack.enter_context(patch.object(module, 'get_anti_truncation_max_attempts', AsyncMock(return_value=2)))
                            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                                response = await client.post(url, json=body)
                        self.assertIn('ORIGINAL_QUOTA', response.text)
                        self.assertNotIn('AGY_FINAL', response.text)
                        self.assertNotIn('UPSTREAM_OUTPUT_BOUNDARY_INVALID', response.text)
                        self.assertEqual(self.saved.await_count, 1)
                        self.assertEqual(self.saved.call_args.args[0]['status_code'], 429)
