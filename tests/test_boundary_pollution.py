"""Damaged private framing must never fall through as a successful answer."""
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response

from src.api import antigravity as api
from src.thought_boundary import (
    ThoughtBoundaryGuard, ThoughtBoundaryError, POLLUTION_ERROR_CODE,
)
import test_custom_audit
from test_thought_boundary import event, text_of, decode


# The two private framing strings visible in records 123977941/123980207.
# No user manuscript or input context is included in these fixtures.
MARKER = '<<<AGY_FINAL_6cd8614f927637e1>>>'
REPEATED_NONCE = '<<<AGY_FINAL_6cd86cd86cd86cd8>>>'
MISSING_DIGIT = '<<<AGY_FINAL_6cd864f927637e1>>>'
SAMPLES = (
    REPEATED_NONCE * 4 + MISSING_DIGIT + 'Wait, I need to output the exact marker. ',
    REPEATED_NONCE + ' ' + REPEATED_NONCE + 'Wait, emit exact marker: <<<AGY_FINAL_6cd86cd8...>>> ',
    MARKER + 'Wait, I need to output the exact marker. ',
    MARKER + 'Wait, emit exact marker: ',
    MARKER + 'Let me copy exact marker: ',
    MARKER + 'Thinking Process:\nAnalyze the request. ',
    MARKER + '正文。<<<AGY_FINAL_broken' + MARKER,
)


class BoundaryPollutionTests(unittest.TestCase):
    def test_observed_pollution_and_post_marker_analysis_at_all_chunk_widths(self):
        for source in SAMPLES:
            for width in (1, 2, 7, 29, 128, 4096):
                for explicit in (None, False):
                    for include in (True, False):
                        with self.subTest(source=source, width=width, explicit=explicit, include=include):
                            g = ThoughtBoundaryGuard(MARKER, include)
                            outputs = []
                            with self.assertRaises(ThoughtBoundaryError):
                                text = source + '第9章 示例章节\n后续正文。'
                                for i in range(0, len(text), width):
                                    part = {'text': text[i:i + width]}
                                    if explicit is not None:
                                        part['thought'] = explicit
                                    outputs.append(g.feed(event([part])))
                                outputs.append(g.feed(event([], 'STOP')))
                            self.assertNotIn('AGY_FINAL', text_of(outputs))
                            self.assertNotIn('Wait', text_of(outputs))
                            self.assertNotIn('第9章', text_of(outputs))
                            self.assertIn(text_of(outputs).strip(), ('', '正文。'))
                            self.assertTrue(g.failed)
                            self.assertFalse(g.complete)
                            self.assertIsNone(g.flush())
                            self.assertTrue(g.metadata()['boundary_failed'])

    def test_every_two_piece_split_of_corrupted_markers(self):
        for marker in (REPEATED_NONCE, '<<<AGY_FINAL_6cd84f927637e1>>>', '<<<AGY_FINAL_typo>>>'):
            for split in range(len(marker) + 1):
                with self.subTest(marker=marker, split=split):
                    g = ThoughtBoundaryGuard(MARKER)
                    outputs = []
                    with self.assertRaises(ThoughtBoundaryError):
                        for text in (marker[:split], marker[split:] + '正文'):
                            outputs.append(g.feed(event([{'text': text}])))
                        outputs.append(g.feed(event([], 'STOP')))
                    self.assertEqual(text_of(outputs), '')

    def test_unclosed_invalid_token_fails_at_eof_and_memory_is_bounded(self):
        for ending in ('STOP', 'EOF'):
            g = ThoughtBoundaryGuard(MARKER)
            output = [g.feed(event([{'text': '<<<AGY_FINAL_wrong...'}]))]
            with self.assertRaises(ThoughtBoundaryError):
                output.append(g.flush() if ending == 'EOF' else g.feed(event([], 'STOP')))
            self.assertEqual(text_of(output), '')
        g = ThoughtBoundaryGuard(MARKER)
        with self.assertRaises(ThoughtBoundaryError):
            for char in '<<<AGY_FINAL_' + 'a' * (g.RECOVERY_LIMIT + 1):
                g.feed(event([{'text': char}]))
        self.assertLessEqual(len(g.states[0]['pending']), g.RECOVERY_LIMIT + 1)

    def test_plain_english_chinese_code_and_short_openings_are_preserved(self):
        for body in ('普通正文。', 'English is allowed. Let me tell you a story.',
                     'Wait, the train is coming.', 'Let me explain the algorithm.',
                     'I need a cup of coffee.', 'The marker is blue.',
                     'Thinking Process: this is an ordinary sentence.',
                     '```python\nprint("hello")\n```', 'Wait'):
            for width in (1, 7, 4096):
                g = ThoughtBoundaryGuard(MARKER)
                source = MARKER + body
                out = [g.feed(event([{'text': source[i:i+width]}])) for i in range(0, len(source), width)]
                out.append(g.feed(event([], 'STOP')))
                self.assertEqual(text_of(out), body)
                self.assertFalse(g.failed)

    def test_valid_partial_answer_is_preserved_before_later_damage(self):
        g = ThoughtBoundaryGuard(MARKER)
        outputs = [g.feed(event([{'text': MARKER + '正文已经开始。'}]))]
        with self.assertRaises(ThoughtBoundaryError):
            for char in MISSING_DIGIT:
                outputs.append(g.feed(event([{'text': char}])))
        self.assertEqual(text_of(outputs), '正文已经开始。')
        self.assertNotIn('AGY_FINAL', text_of(outputs))

    def test_native_thought_with_quoted_markers_is_not_interpreted_as_framing(self):
        g = ThoughtBoundaryGuard(MARKER)
        thought = {'text': REPEATED_NONCE, 'thought': True, 'thoughtSignature': 'keep'}
        out = g.feed(event([thought, {'text': MARKER + '正文'}], 'STOP'))
        self.assertEqual(decode(out)['candidates'][0]['content']['parts'][0], thought)
        self.assertEqual(text_of([out]), '正文')


class BoundaryPollutionApiTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_failure_logs_once_without_ban_retry_or_successful_stop(self):
        for source in SAMPLES:
            for native_first in (True, False):
                self.saved.reset_mock()
                closed = []
                async def upstream(**kwargs):
                    try:
                        if native_first:
                            yield event([{'text': 'native analysis', 'thought': True}])
                        for char in source + '第9章 示例章节\n后续正文。':
                            yield event([{'text': char}])
                        yield event([], 'STOP')
                    finally:
                        closed.append(True)
                with ExitStack() as stack:
                    stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                    stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)))
                    stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={
                        'max_retries': 2, 'retry_interval': 0, 'retry_enabled': True})))
                    ban = stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                    out = [c async for c in api.stream_request({'model': 'gemini-4-argon-medium', 'request': {'contents': []}})]
                self.assertNotIn('AGY_FINAL', text_of(out))
                self.assertNotIn('第9章', text_of(out))
                self.assertIn(text_of(out).strip(), ('', '正文。'))
                self.assertEqual(self.saved.await_count, 1)
                row = self.saved.call_args.args[0]
                self.assertEqual((row['status_code'], row['success']), (502, False))
                self.assertIn(POLLUTION_ERROR_CODE, row['error_message'])
                self.assertIsInstance(out[-1], Response)
                self.assertEqual(json.loads(out[-1].body)['error']['status'], POLLUTION_ERROR_CODE)
                self.assertNotIn('"finishReason": "STOP"', ''.join(c for c in out if isinstance(c, str)))
                ban.assert_not_awaited()
                self.assertEqual(closed, [True])

    async def test_transport_error_while_opening_is_buffered_does_not_bypass_error_accounting(self):
        async def upstream(**kwargs):
            yield event([{'text': MARKER + 'Thinking Process:'}])
            raise httpx.ReadError('synthetic transport close')
        with patch.object(api, 'stream_post_async', upstream), patch.object(
                api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)):
            out = [c async for c in api.stream_request({
                'model': 'gemini-4-argon-medium', 'request': {'contents': []}})]
        self.assertEqual(text_of(out), '')
        self.assertEqual(out[-1].status_code, 502)
        self.assertIn(POLLUTION_ERROR_CODE, out[-1].body.decode())
        self.assertEqual(self.saved.await_count, 1)
        self.assertFalse(self.saved.call_args.args[0]['success'])

    async def test_three_protocols_and_anti_truncation_propagate_output_error(self):
        from src.router.antigravity import gemini, openai, anthropic
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'synthetic'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'synthetic'
        requests = [
            ('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents': [{'role': 'user', 'parts': [{'text': 'test'}]}]}),
            ('/v1/chat/completions', {'model': 'gemini-4-argon-medium', 'stream': True, 'messages': [{'role': 'user', 'content': 'test'}]}),
            ('/v1/messages', {'model': 'gemini-4-argon-medium', 'stream': True, 'max_tokens': 100, 'messages': [{'role': 'user', 'content': 'test'}]}),
        ]
        for anti in (False, True):
            for native_first in (False, True):
                for url, body in requests:
                    with self.subTest(anti=anti, native_first=native_first, url=url):
                        self.saved.reset_mock()
                        async def upstream(**kwargs):
                            if native_first:
                                yield event([{'text': 'native analysis', 'thought': True}])
                            yield event([{'text': SAMPLES[0] + '第9章 示例章节'}], 'STOP')
                        with ExitStack() as stack:
                            stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                            stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)))
                            for module in (gemini, openai, anthropic):
                                stack.enter_context(patch.object(module, 'is_anti_truncation_model', return_value=anti))
                                stack.enter_context(patch.object(module, 'get_anti_truncation_max_attempts', AsyncMock(return_value=2)))
                            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                                response = await client.post(url, json=body)
                        self.assertIn(POLLUTION_ERROR_CODE, response.text)
                        for unexpected in ('AGY_FINAL', '第9章', 'end_turn', '"finish_reason": "stop"', '"finishReason": "STOP"'):
                            self.assertNotIn(unexpected, response.text)
                        self.assertEqual(self.saved.await_count, 1)
                        self.assertEqual(self.saved.call_args.args[0]['status_code'], 502)
