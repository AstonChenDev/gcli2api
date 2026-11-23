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

    async def request(self, protocol, upstream, *, streaming=False):
        app = FastAPI()
        module, path, body = {
            'openai': (openai, '/v1/chat/completions', {'model': 'gemini-4-argon-medium', 'messages': [{'role': 'user', 'content': 'test'}], 'stream': False}),
            'anthropic': (anthropic, '/v1/messages', {'model': 'gemini-4-argon-medium', 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}], 'stream': False}),
            'gemini': (gemini, '/v1beta/models/gemini-4-argon-medium:generateContent', {'contents': [{'role': 'user', 'parts': [{'text': 'test'}]}]}),
        }[protocol]
        if streaming:
            if protocol == 'gemini':
                path = path.replace(':generateContent', ':streamGenerateContent')
            else:
                body['stream'] = True

        async def upstream_stream(**kwargs):
            yield 'data: ' + upstream.text

        app.include_router(module.router)
        auth = gemini.authenticate_gemini_flexible if protocol == 'gemini' else module.authenticate_bearer
        app.dependency_overrides[auth] = lambda: 'test'
        self.saved.reset_mock()
        with patch.object(api, 'post_async', AsyncMock(return_value=upstream)), patch.object(api, 'stream_post_async', upstream_stream), patch.object(api, 'record_api_call_error', AsyncMock()):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                result = await client.post(path, json=body)
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], result.status_code)
        return result

    async def test_explicit_zero_cached_tokens_override_legacy_details(self):
        counts = {'promptTokenCount': 30, 'candidatesTokenCount': 7, 'thoughtsTokenCount': 2,
                  'cachedContentTokenCount': 0, 'input_tokens_details': {'cached_tokens': 4}}
        answer = {'candidates': [{'content': {'parts': [{'text': 'fixture answer'}]}, 'finishReason': 'STOP'}],
                  'usageMetadata': counts}
        for streaming in (False, True):
            for protocol in ('openai', 'anthropic', 'gemini'):
                with self.subTest(protocol=protocol, streaming=streaming):
                    result = await self.request(protocol, httpx.Response(200, json=answer), streaming=streaming)
                    self.assertEqual(result.status_code, 200)
                    self.assertIn('fixture answer', result.text)
                    if streaming:
                        events = [json.loads(line[5:]) for line in result.text.splitlines()
                                  if line.startswith('data:') and line[5:].strip() != '[DONE]']
                    else:
                        events = [result.json()]
                    if protocol == 'gemini':
                        self.assertEqual(events[0]['usageMetadata']['cachedContentTokenCount'], 0)
                    else:
                        usage = next(e['usage'] for e in reversed(events) if 'usage' in e)
                        if protocol == 'openai':
                            self.assertEqual(usage.get('prompt_tokens_details', {}).get('cached_tokens', 0), 0)
                        else:
                            self.assertEqual((usage['input_tokens'], usage.get('cache_read_input_tokens', 0)), (30, 0))
                    row = self.saved.call_args.args[0]
                    self.assertEqual((row['prompt_tokens'], row['completion_tokens'], row['cached_tokens']), (30, 9, 0))

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

    async def test_embedded_rate_limit_preserves_only_the_retry_hint(self):
        for protocol in ('openai', 'anthropic', 'gemini'):
            for code in (429, 503):
                error = {'error': {'code': code, 'message': 'busy'}}
                upstream = httpx.Response(200, json=error, headers={'Retry-After': '17', 'x-private': 'test'})
                result = await self.request(protocol, upstream)
                self.assertEqual(result.status_code, code)
                self.assertEqual(result.json(), error)
                self.assertEqual(result.headers.get('retry-after'), '17')
                self.assertNotIn('x-private', result.headers)

    async def test_json_bom_preserves_answer_and_embedded_error_across_adapters(self):
        text = "before\ufeffafter"
        for protocol in ('gemini', 'openai', 'anthropic'):
            for status, payload in [
                (200, {'candidates': [{'content': {'parts': [{'text': text}]}, 'finishReason': 'STOP'}],
                       'usageMetadata': {'promptTokenCount': 4, 'candidatesTokenCount': 3}}),
                (400, {'error': {'code': 400, 'message': 'invalid input'}}),
            ]:
                with self.subTest(protocol=protocol, status=status):
                    wire = json.dumps({'response': payload}, ensure_ascii=False).encode('utf-8-sig')
                    result = await self.request(protocol, httpx.Response(200, content=wire))
                    self.assertEqual(result.status_code, status)
                    if status == 200:
                        self.assertIn(text, json.dumps(result.json(), ensure_ascii=False))
                        self.assertEqual(self.saved.call_args.args[0]['total_tokens'], 7)
                    else:
                        self.assertEqual(result.json()['error'], payload['error'])
