"""Native signatures survive without turning empty output into 200."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import gemini, openai, anthropic
from stream_fixtures import event, decode, text_of

BODY = {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}
SIGNATURE = {'text': '', 'thought': True, 'thoughtSignature': 'keep-between-parts-and-answer'}


def parts(chunks):
    return [p for chunk in chunks if isinstance(chunk, str) and chunk.startswith('data:') and '[DONE]' not in chunk
            for c in decode(chunk).get('candidates', []) for p in c.get('content', {}).get('parts', [])]


class StreamSignatureOrderingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_signature_between_native_parts_keeps_original_order(self):
        cases = [
            ([{'text': ''}], [{'text': 'answer'}], [SIGNATURE, {'text': 'answer'}]),
            ([{'text': 'Thinking Process:\nplan'}], [{'text': 'answer'}],
             [{'text': 'Thinking Process:\nplan'}, SIGNATURE, {'text': 'answer'}]),
            ([{'text': 'short prefix '}], [{'text': 'suffix'}],
             [{'text': 'short prefix '}, SIGNATURE, {'text': 'suffix'}]),
        ]
        for before, after, expected in cases:
            for terminal in ('STOP', 'DONE'):
                with self.subTest(before=before, terminal=terminal):
                    self.saved.reset_mock()
                    async def upstream(**kwargs):
                        yield event(before)
                        yield event([SIGNATURE])
                        yield event(after, 'STOP' if terminal == 'STOP' else None)
                        if terminal == 'DONE':
                            yield 'data: [DONE]\n\n'
                    with patch.object(api, 'stream_post_async', upstream):
                        out = [c async for c in api.stream_request(BODY)]
                    self.assertEqual(parts(out), expected)
                    self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)

    async def test_empty_text_and_signature_without_answer_stays_single_502(self):
        for terminal in ('STOP', 'EOF', 'DONE'):
            for native in (False, True):
                with self.subTest(terminal=terminal, native=native):
                    self.saved.reset_mock()
                    signature = {**SIGNATURE}
                    if not native:
                        signature.pop('thought')
                    async def upstream(**kwargs):
                        yield event([{'text': ''}])
                        yield event([signature], 'STOP' if terminal == 'STOP' else None)
                        if terminal == 'DONE':
                            yield 'data: [DONE]\n\n'
                    with patch.object(api, 'stream_post_async', upstream):
                        out = [c async for c in api.stream_request(BODY)]
                    self.assertEqual(len(out), 1)
                    self.assertIsInstance(out[0], Response)
                    self.assertEqual(out[0].status_code, 502)
                    self.assertFalse(self.saved.call_args.args[0]['success'])

    async def test_signature_queue_does_not_delay_following_native_text_until_answer(self):
        async def upstream(**kwargs):
            yield event([{'text': ''}])
            yield event([SIGNATURE])
            yield event([{**SIGNATURE, 'thoughtSignature': 'second-signature'}])
            yield event([{'text': 'native thinking', 'thought': True}])
            yield event([{'text': 'answer'}], 'STOP')
        with patch.object(api, 'stream_post_async', upstream):
            stream = api.stream_request(BODY)
            first = await anext(stream)
            self.assertEqual(text_of([first], True), 'native thinking')
            self.assertEqual(text_of([first]), '')
            rest = [c async for c in stream]
        self.assertEqual([p.get('thoughtSignature') for p in parts([first])[:2]],
                         [SIGNATURE['thoughtSignature'], 'second-signature'])
        self.assertEqual(text_of(rest), 'answer')

    async def test_signature_is_not_sent_before_explicit_error(self):
        async def upstream(**kwargs):
            yield event([{'text': ''}])
            yield event([SIGNATURE])
            yield Response(status_code=429, content='{"error":{"code":429,"message":"original quota"}}')
        with patch.object(api, 'stream_post_async', upstream), patch.object(
                api, 'record_api_call_error', AsyncMock()):
            out = [c async for c in api.stream_request(BODY)]
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].status_code, 429)
        self.assertNotIn('keep-between', out[0].body.decode())

    async def test_upstream_thought_signature_is_preserved_despite_request_preference(self):
        async def upstream(**kwargs):
            yield event([{'text': ''}])
            yield event([SIGNATURE])
            yield event([{'text': 'answer'}], 'STOP')
        body = {'model': BODY['model'], 'request': {'contents': [], 'generationConfig': {
            'thinkingConfig': {'includeThoughts': False}}}}
        with patch.object(api, 'stream_post_async', upstream):
            out = [c async for c in api.stream_request(body)]
        self.assertEqual(parts(out), [SIGNATURE, {'text': 'answer'}])

    async def test_all_http_protocols_preserve_text_and_anthropic_signature(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        requests = [('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents': [{'parts': [{'text': 'test'}]}]}, True),
                    ('/v1/chat/completions', {'model': BODY['model'], 'stream': True, 'messages': [{'role': 'user', 'content': 'test'}]}, False),
                    ('/v1/messages', {'model': BODY['model'], 'stream': True, 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}]}, True)]
        for url, body, keeps_signature in requests:
            with self.subTest(url=url):
                self.saved.reset_mock()
                async def upstream(**kwargs):
                    yield event([{'text': ''}])
                    yield event([SIGNATURE])
                    yield event([{'text': 'answer'}], 'STOP')
                with patch.object(api, 'stream_post_async', upstream):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                        response = await client.post(url, json=body)
                self.assertEqual(response.status_code, 200)
                self.assertIn('answer', response.text)
                if keeps_signature:
                    self.assertIn(SIGNATURE['thoughtSignature'], response.text)
                self.assertTrue(self.saved.call_args.args[0]['success'])
