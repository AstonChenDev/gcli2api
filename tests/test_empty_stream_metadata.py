"""Signature-only text parts survive ordinary streaming."""
import unittest
from unittest.mock import patch
from fastapi import Response
from src.api import antigravity as api
import test_custom_audit
from stream_fixtures import event, text_of
from test_initial_stream_signatures import parts_of


class EmptyMetadataStreamTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_final_signature_survives_but_signature_alone_is_not_an_answer(self):
        for has_answer in (True, False):
            with self.subTest(has_answer=has_answer):
                self.saved.reset_mock()
                signature = {'text':'', 'thoughtSignature':'terminal-signature'}
                async def upstream(**kwargs):
                    if has_answer:
                        yield event([{'text':'answer'}])
                    yield event([signature], 'STOP')
                with patch.object(api, 'stream_post_async', upstream):
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
