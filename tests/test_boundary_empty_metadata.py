"""Signature-only text parts must survive thought-boundary buffering."""
import unittest
from unittest.mock import patch
from fastapi import Response
from src.api import antigravity as api
import test_custom_audit
from src.thought_boundary import ThoughtBoundaryGuard
from test_thought_boundary import event, text_of
from test_boundary_part_metadata import MARKER, parts_of


class EmptyTextMetadataTests(unittest.TestCase):
    def test_metadata_only_eof_flushes_without_inventing_text(self):
        guard = ThoughtBoundaryGuard(MARKER)
        signature = {'text':'', 'thoughtSignature':'metadata-only'}
        chunks = [guard.feed(event([signature]))]
        self.assertFalse(guard.complete)
        chunks.append(guard.flush())
        self.assertEqual(parts_of(chunks), [signature])
        self.assertEqual(text_of(chunks), '')
        self.assertTrue(guard.complete)

    def test_empty_text_signature_after_buffered_answer_is_not_dropped(self):
        for ending in ('STOP', 'EOF'):
            with self.subTest(ending=ending):
                guard = ThoughtBoundaryGuard(MARKER)
                expected = [{'text':'answer'}, {'text':'', 'thoughtSignature':'final-signature'}]
                chunks = [guard.feed(event([part])) for part in expected]
                chunks.append(guard.feed(event([], 'STOP')) if ending == 'STOP' else guard.flush())
                self.assertEqual(parts_of(chunks), expected)
                self.assertTrue(guard.complete)

    def test_empty_metadata_inside_split_marker_is_preserved_without_marker_leak(self):
        for hidden in (False, True):
            with self.subTest(hidden=hidden):
                guard = ThoughtBoundaryGuard(MARKER, include_thoughts=not hidden)
                chunks = [guard.feed(event([{'text':'Thinking Process:\nplan' + MARKER[:15]}])),
                    guard.feed(event([{'text':'', 'thoughtSignature':'frame-signature'}])),
                    guard.feed(event([{'text':MARKER[15:]+'answer'}], 'STOP'))]
                self.assertEqual(text_of(chunks, True), '' if hidden else 'Thinking Process:\nplan')
                self.assertEqual(text_of(chunks), 'answer')
                self.assertIn({'text':'', 'thoughtSignature':'frame-signature'}, parts_of(chunks))
                self.assertTrue(all(not state['segments'] for state in guard.states.values()))


class EmptyMetadataStreamTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_final_signature_survives_but_signature_alone_is_not_an_answer(self):
        for has_answer in (True, False):
            with self.subTest(has_answer=has_answer):
                self.saved.reset_mock()
                signature = {'text':'', 'thoughtSignature':'terminal-signature'}
                async def upstream(**kwargs):
                    if has_answer:
                        yield event([{'text':MARKER + 'answer'}])
                    yield event([signature], 'STOP')
                with patch.object(api, 'stream_post_async', upstream), patch.object(
                    api, 'prepare_thought_boundary', lambda m, r: (r, MARKER),
                ):
                    output = [chunk async for chunk in api.stream_request({
                        'model':'gemini-4-argon-medium', 'request':{'contents':[]},
                    })]
                self.assertEqual(self.saved.await_count, 1)
                self.assertEqual(self.saved.call_args.args[0]['status_code'], 200 if has_answer else 502)
                if has_answer:
                    self.assertFalse(any(isinstance(item, Response) for item in output))
                    self.assertEqual(parts_of(output), [{'text':'answer'}, signature])
                else:
                    self.assertEqual(len(output), 1)
                    self.assertEqual(output[0].status_code, 502)
