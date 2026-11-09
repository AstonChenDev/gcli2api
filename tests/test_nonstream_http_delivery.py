import gzip
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import openai, anthropic, gemini


class NonstreamHTTPDeliveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def request(self, protocol, upstream):
        app = FastAPI()
        module, path, body = {
            'openai': (openai, '/v1/chat/completions', {'model': 'gemini-4-argon-medium', 'messages': [{'role': 'user', 'content': 'test'}], 'stream': False}),
            'anthropic': (anthropic, '/v1/messages', {'model': 'gemini-4-argon-medium', 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}], 'stream': False}),
            'gemini': (gemini, '/v1beta/models/gemini-4-argon-medium:generateContent', {'contents': [{'role': 'user', 'parts': [{'text': 'test'}]}]}),
        }[protocol]
        app.include_router(module.router)
        auth = gemini.authenticate_gemini_flexible if protocol == 'gemini' else module.authenticate_bearer
        app.dependency_overrides[auth] = lambda: 'test'
        self.saved.reset_mock()
        with patch.object(api, 'post_async', AsyncMock(return_value=upstream)), patch.object(api, 'record_api_call_error', AsyncMock()):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                result = await client.post(path, json=body)
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], result.status_code)
        return result

    async def test_plain_empty_and_json_errors_preserve_status_body_and_retry_header(self):
        for protocol in ('openai', 'anthropic', 'gemini'):
            for status, raw in ((503, b'<html>temporarily unavailable</html>'), (429, b''), (400, b'{"error":{"message":"blocked"},"partialResponse":{"text":"kept"}}')):
                with self.subTest(protocol=protocol, status=status):
                    result = await self.request(protocol, httpx.Response(status, content=raw, headers={'Retry-After': '17'}))
                    self.assertEqual(result.status_code, status)
                    self.assertEqual(result.content, raw)
                    self.assertEqual(result.headers.get('retry-after'), '17')

    async def test_decompressed_error_and_success_have_correct_downstream_headers(self):
        answer = json.dumps({'candidates': [{'content': {'parts': [{'text': 'EXPECTED ANSWER'}]}, 'finishReason': 'STOP'}]}).encode()
        for protocol in ('openai', 'anthropic', 'gemini'):
            for status, raw in ((200, answer), (503, b'upstream unavailable')):
                with self.subTest(protocol=protocol, status=status):
                    compressed = gzip.compress(raw)
                    upstream = httpx.Response(status, content=compressed, headers={
                        'content-encoding': 'gzip', 'content-length': str(len(compressed)),
                        'content-type': 'application/json', 'connection': 'keep-alive, x-internal',
                        'x-internal': 'hop-only', 'retry-after': '9'})
                    self.assertEqual(upstream.content, raw)
                    result = await self.request(protocol, upstream)
                    self.assertEqual(result.status_code, status)
                    self.assertNotIn('content-encoding', result.headers)
                    self.assertNotIn('connection', result.headers)
                    self.assertNotIn('x-internal', result.headers)
                    self.assertEqual(int(result.headers['content-length']), len(result.content))
                    if status == 200:
                        self.assertIn('EXPECTED ANSWER', result.text)
                    else:
                        self.assertEqual(result.content, raw)
