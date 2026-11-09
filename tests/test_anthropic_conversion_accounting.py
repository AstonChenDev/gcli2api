"""Local Claude adapter failures are not client disconnects; usage is optional."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
import test_custom_audit
from test_followup_repair import chunk
from src.api import antigravity as api
from src.converter import anthropic2gemini as converter
from src.router.antigravity import anthropic
from src.usage_log import extract_usage_tokens


class AnthropicConversionAccountingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    def app(self):
        app = FastAPI()
        app.include_router(anthropic.router)
        app.dependency_overrides[anthropic.authenticate_bearer] = lambda: 'test'
        return app

    async def post(self, app, model='gemini-4-argon-medium', stream=True):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            return await client.post('/v1/messages', json={'model': model, 'stream': stream,
                'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}]})

    async def test_invalid_usage_never_discards_valid_answer_in_normal_anti_or_nonstream(self):
        malformed = [{'promptTokenCount': 'invalid-number'}, {'candidatesTokenCount': {}},
                     {'thoughtsTokenCount': []}, {'cachedContentTokenCount': True},
                     {'promptTokenCount': float('inf')}, {'promptTokenCount': -10},
                     {'input_tokens_details': {'cached_tokens': float('inf')}}, ['invalid-shape']]
        for usage in malformed:
            for mode in ('normal', 'anti', 'nonstream'):
                with self.subTest(usage=usage, mode=mode):
                    self.saved.reset_mock()
                    async def upstream(**kwargs):
                        yield chunk('EXPECTED ANSWER', usage=usage)
                    payload = json.loads(chunk('EXPECTED ANSWER', usage=usage)[5:])
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                        stack.enter_context(patch.object(api, 'post_async', AsyncMock(return_value=httpx.Response(
                            200, content=json.dumps(payload).encode()))))
                        stack.enter_context(patch.object(anthropic, 'get_anti_truncation_max_attempts', AsyncMock(return_value=1)))
                        response = await self.post(self.app(), model=('抗截断-' if mode == 'anti' else '') + 'gemini-4-argon-medium', stream=mode != 'nonstream')
                    self.assertEqual(response.status_code, 200)
                    self.assertIn('EXPECTED ANSWER', response.text)
                    self.assertNotIn('RESPONSE_CONVERSION_FAILED', response.text)
                    self.assertEqual(self.saved.await_count, 1)
                    self.assertTrue(self.saved.call_args.args[0]['success'])
                    self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)

    async def test_adapter_exception_before_and_after_output_is_502_not_499(self):
        encode = json.dumps
        for anti in (False, True):
            for partial in (False, True):
                with self.subTest(anti=anti, partial=partial):
                    self.saved.reset_mock()
                    failed = False
                    async def upstream(**kwargs):
                        if partial:
                            yield chunk('partial answer ' + 'x' * 128, finish=None)
                        yield chunk('TRIGGER_FAILURE')
                    def encoding(value, *args, **kwargs):
                        nonlocal failed
                        if isinstance(value, dict) and not failed:
                            early = not partial and value.get('type') == 'message_start'
                            late = partial and value.get('type') == 'content_block_delta' and value.get('delta', {}).get('text') == 'TRIGGER_FAILURE'
                            if early or late:
                                failed = True
                                raise ValueError('synthetic adapter encoding failure')
                        return encode(value, *args, **kwargs)
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                        stack.enter_context(patch.object(anthropic, 'get_anti_truncation_max_attempts', AsyncMock(return_value=1)))
                        stack.enter_context(patch.object(converter.json, 'dumps', encoding))
                        response = await self.post(self.app(), model=('抗截断-' if anti else '') + 'gemini-4-argon-medium')
                    self.assertTrue(failed)
                    self.assertEqual(response.status_code, 200 if partial else 502)
                    self.assertIn('RESPONSE_CONVERSION_FAILED', response.text)
                    self.assertNotIn('message_stop', response.text)
                    self.assertNotIn('end_turn', response.text)
                    if partial:
                        self.assertIn('partial answer', response.text)
                    self.assertEqual(self.saved.await_count, 1)
                    row = self.saved.call_args.args[0]
                    self.assertEqual(row['status_code'], 502)
                    self.assertFalse(row['success'])
                    self.assertNotIn('CLIENT_DISCONNECTED', row['error_message'])

    async def test_split_prompt_and_cached_metadata_keep_input_total(self):
        async def source():
            yield chunk('answer', finish=None, usage={'promptTokenCount': 100}).encode()
            yield chunk('', finish=None, usage={'cachedContentTokenCount': 40}).encode()
            yield chunk('', finish='STOP', usage={'candidatesTokenCount': 5, 'thoughtsTokenCount': 2}).encode()
        events = [c async for c in converter.gemini_stream_to_anthropic_stream(source(), 'test')]
        messages = [json.loads(line[5:]) for c in events for line in c.decode().splitlines() if line.startswith('data:')]
        usage = next(v['usage'] for v in messages if v.get('type') == 'message_delta')
        self.assertEqual(usage['input_tokens'], 60)
        self.assertEqual(usage['cache_read_input_tokens'], 40)
        self.assertEqual(usage['output_tokens'], 7)


class TokenCounterValidationTests(unittest.TestCase):
    def test_invalid_or_negative_log_counters_do_not_interrupt_processing(self):
        for value in (float('inf'), float('-inf'), float('nan'), True, False, -9, {}, []):
            for field in ('promptTokenCount', 'candidatesTokenCount', 'thoughtsTokenCount', 'cachedContentTokenCount'):
                with self.subTest(value=value, field=field):
                    usage = extract_usage_tokens({'usageMetadata': {field: value}})
                    self.assertTrue(all(isinstance(v, int) and v >= 0 for v in usage.values()))
