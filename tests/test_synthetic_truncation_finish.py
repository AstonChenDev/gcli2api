"""An exhausted synthetic continuation still reports a token-limit finish."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import test_custom_audit
from test_thought_stream import chunk, source, parts_from
from src.api import antigravity as api
from src.converter.anti_truncation import AntiTruncationStreamProcessor, SYNTHETIC_TOOL_NAME
from src.router.antigravity import gemini, openai, anthropic


def synthetic(text, finish):
    return chunk([{'functionCall': {'name': SYNTHETIC_TOOL_NAME, 'args': {'content': text}}}], finish)


class SyntheticTruncationFinishTests(unittest.IsolatedAsyncioTestCase):
    async def test_exhausted_limit_at_eof_and_done_keeps_original_reason(self):
        for terminal in ('EOF', 'DONE'):
            for attempts in (1, 2):
                with self.subTest(terminal=terminal, attempts=attempts):
                    count = 0
                    async def request(_):
                        nonlocal count
                        count += 1
                        async def stream():
                            async for c in source([synthetic('part' + str(count), 'MAX_TOKENS')]):
                                yield c
                            if terminal == 'DONE':
                                yield b'data: [DONE]\n\n'
                        return StreamingResponse(stream())
                    out = [c async for c in AntiTruncationStreamProcessor(request, {'request': {}}, attempts).process_stream()]
                    finishes = []
                    for c in out:
                        raw = c.decode() if isinstance(c, bytes) else c
                        if not raw.startswith('data:') or raw[5:].strip() == '[DONE]':
                            continue
                        d = json.loads(raw[5:]); d = d.get('response', d)
                        finishes += [v['finishReason'] for v in d.get('candidates', []) if v.get('finishReason')]
                    self.assertEqual(count, attempts)
                    self.assertEqual(finishes, ['MAX_TOKENS'])
                    self.assertEqual(''.join(p.get('text', '') for p in parts_from(out)),
                                     ''.join('part' + str(n) for n in range(1, attempts + 1)))

    async def test_successful_continuation_still_reports_stop(self):
        count = 0
        async def request(_):
            nonlocal count
            count += 1
            return StreamingResponse(source([synthetic('part' + str(count), 'MAX_TOKENS' if count == 1 else 'STOP')]))
        out = [c async for c in AntiTruncationStreamProcessor(request, {'request': {}}, 2).process_stream()]
        finishes = []
        for c in out:
            raw = c.decode() if isinstance(c, bytes) else c
            if raw.startswith('data:') and raw[5:].strip() != '[DONE]':
                d = json.loads(raw[5:]); d = d.get('response', d)
                finishes += [v['finishReason'] for v in d.get('candidates', []) if v.get('finishReason')]
        self.assertEqual(finishes, ['STOP'])
        self.assertEqual(count, 2)


class SyntheticTruncationProtocolTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_actual_anti_truncation_routes_report_token_limit_in_all_protocols(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        model = '抗截断-gemini-4-argon-medium'
        requests = [('/v1beta/models/' + model + ':streamGenerateContent', {'contents': [{'parts': [{'text': 'test'}]}]}, 'MAX_TOKENS'),
                    ('/v1/chat/completions', {'model': model, 'stream': True, 'messages': [{'role': 'user', 'content': 'test'}]}, 'length'),
                    ('/v1/messages', {'model': model, 'stream': True, 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}]}, 'max_tokens')]
        for url, body, expected_finish in requests:
            with self.subTest(url=url):
                self.saved.reset_mock()
                async def upstream(**kwargs):
                    # Real prepare_thought_boundary bypasses the synthetic tool
                    # request. Only the network response is replaced in this test.
                    self.assertTrue(kwargs['body']['request'].get('tools'))
                    yield 'data: ' + json.dumps(synthetic('partial answer', 'MAX_TOKENS'))
                with ExitStack() as stack:
                    stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                    for module in (gemini, openai, anthropic):
                        stack.enter_context(patch.object(module, 'get_anti_truncation_max_attempts', AsyncMock(return_value=1)))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                        response = await client.post(url, json=body)
                self.assertEqual(response.status_code, 200)
                self.assertIn('partial answer', response.text)
                self.assertIn(expected_finish, response.text)
                self.assertNotIn('end_turn', response.text)
                self.assertNotIn('"finish_reason": "stop"', response.text)
                self.assertNotIn('"finishReason": "STOP"', response.text)
                self.assertEqual(self.saved.await_count, 1)
