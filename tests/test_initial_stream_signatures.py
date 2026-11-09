"""Initial signatures survive without committing headers or faking content."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from fastapi import Response
import test_custom_audit
from src.api import antigravity as api
from src.router.stream_passthrough import build_streaming_response_or_error
from test_thought_boundary import event, decode, text_of

BODY = {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}
MARKER = 'BOUNDARY'


def parts_of(chunks):
    return [p for c in chunks if isinstance(c, str) and c.startswith('data:') and '[DONE]' not in c
            for candidate in decode(c).get('candidates', []) for p in candidate.get('content', {}).get('parts', [])]


class InitialSignatureTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    def test_signature_for_another_candidate_does_not_renumber_implicit_index(self):
        chunk = api._prepend_signature_parts(event([{'text': 'candidate zero'}]), [
            (1, {'text': '', 'thoughtSignature': 'candidate-one-signature'})])
        candidates = decode(chunk)['candidates']
        by_index = {c['index']: c['content']['parts'] for c in candidates}
        self.assertEqual(by_index[0], [{'text': 'candidate zero'}])
        self.assertEqual(by_index[1], [{'text': '', 'thoughtSignature': 'candidate-one-signature'}])

    async def test_initial_signature_preserved_before_buffered_answer_with_and_without_guard(self):
        for guard_enabled in (False, True):
            self.saved.reset_mock()
            signature = {'text': '', 'thought': True, 'thoughtSignature': 'initial-signature'}
            async def upstream(**kwargs):
                yield event([signature])
                yield event([{'text': (MARKER if guard_enabled else '') + 'answer'}])
                yield event([], 'STOP')
            with patch.object(api, 'stream_post_async', upstream), patch.object(
                    api, 'prepare_thought_boundary', lambda m, r: (r, MARKER if guard_enabled else None)):
                out = [c async for c in api.stream_request(BODY)]
            self.assertEqual(parts_of(out), [signature, {'text': 'answer'}])
            self.assertEqual(text_of(out), 'answer')
            self.assertTrue(self.saved.call_args.args[0]['success'])

    async def test_signature_only_eof_is_still_empty_and_thought_optout_is_respected(self):
        for has_answer in (False, True):
            self.saved.reset_mock()
            async def upstream(**kwargs):
                yield event([{'text': '', 'thought': True, 'thoughtSignature': 'hidden-signature'}])
                if has_answer:
                    yield event([{'text': MARKER + 'answer'}], 'STOP')
            with patch.object(api, 'stream_post_async', upstream), patch.object(
                    api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)):
                out = [c async for c in api.stream_request({'model': BODY['model'], 'request': {
                    'contents': [], 'generationConfig': {'thinkingConfig': {'includeThoughts': False}}}})]
            self.assertNotIn('hidden-signature', str(parts_of(out)))
            self.assertEqual(text_of(out), 'answer' if has_answer else '')
            self.assertEqual(self.saved.call_args.args[0]['status_code'], 200 if has_answer else 502)
        # Visible signatures alone also cannot count as output or leak a 200.
        async def only_signature(**kwargs):
            yield event([{'text': '', 'thoughtSignature': 'only-signature'}], 'STOP')
        with patch.object(api, 'stream_post_async', only_signature):
            out = [c async for c in api.stream_request(BODY)]
        self.assertEqual(len(out), 1)
        self.assertIsInstance(out[0], Response)
        self.assertEqual(out[0].status_code, 502)

    async def test_initial_signature_does_not_finish_prefetch_before_real_output(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def upstream(**kwargs):
            yield event([{'text': '', 'thought': True, 'thoughtSignature': 'initial-signature'}])
            entered.set(); await release.wait()
            yield event([{'text': MARKER + 'answer'}], 'STOP')
        with patch.object(api, 'stream_post_async', upstream), patch.object(
                api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)):
            task = asyncio.create_task(build_streaming_response_or_error(api.stream_request(BODY)))
            await entered.wait(); await asyncio.sleep(0)
            self.assertFalse(task.done())
            release.set()
            response = await task
            try:
                chunks = [c async for c in response.body_iterator]
                self.assertIn('initial-signature', str(parts_of(chunks)))
                self.assertEqual(text_of(chunks), 'answer')
            finally:
                await response.body_iterator.aclose()
                await response.delivery.finalize()

    async def test_retry_discards_only_failed_attempt_signature(self):
        calls = 0
        async def upstream(**kwargs):
            nonlocal calls
            calls += 1
            yield event([{'text': '', 'thoughtSignature': 'failed' if calls == 1 else 'keep'}])
            if calls == 1:
                yield Response(status_code=503, content='{"error":{"code":503}}')
            else:
                yield event([{'text': MARKER + 'answer'}], 'STOP')
        with ExitStack() as stack:
            stack.enter_context(patch.object(api, 'stream_post_async', upstream))
            stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)))
            stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={
                'retry_enabled': True, 'max_retries': 1, 'retry_interval': 0})))
            stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
            stack.enter_context(patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=True)))
            stack.enter_context(patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))))
            out = [c async for c in api.stream_request(BODY)]
        self.assertEqual(calls, 2)
        self.assertNotIn('failed', str(parts_of(out)))
        self.assertIn('keep', str(parts_of(out)))
        self.assertEqual([c.args[0]['status_code'] for c in self.saved.call_args_list], [503, 200])

    async def test_initial_signature_buffer_is_bounded_and_reports_error(self):
        async def upstream(**kwargs):
            yield event([{'text': '', 'thoughtSignature': 'x' * (1024 * 1024 + 1)}])
        with patch.object(api, 'stream_post_async', upstream):
            out = [c async for c in api.stream_request(BODY)]
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].status_code, 500)
        self.assertEqual(json.loads(out[0].body)['error']['status'], 'UPSTREAM_INVALID_EVENT')
        self.assertFalse(self.saved.call_args.args[0]['success'])
