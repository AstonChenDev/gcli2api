import unittest
from unittest.mock import patch

import test_custom_audit
from src.api import antigravity as api
from src.thought_boundary import ThoughtBoundaryGuard
from test_thought_boundary import event, decode

OPAQUE = [{'thoughtSignature': 'standalone-signature'},
          {'inlineData': {'mimeType': 'image/png', 'data': 'aW1hZ2U='}},
          {'executableCode': {'language': 'PYTHON', 'code': 'print(1)'}}]

def parts(chunks):
    return [p for c in chunks if c for candidate in decode(c).get('candidates', [])
            for p in candidate.get('content', {}).get('parts', [])]


class OpaqueOrderingTests(unittest.TestCase):
    def test_nontext_parts_do_not_overtake_buffered_text_with_thoughts_on_or_off(self):
        for opaque in OPAQUE:
            for visible in (True, False):
                for terminal in ('STOP', 'EOF'):
                    with self.subTest(opaque=opaque, visible=visible, terminal=terminal):
                        guard = ThoughtBoundaryGuard('BOUNDARY', visible)
                        out = [guard.feed(event([{'text': 'prefix '}])), guard.feed(event([opaque])),
                               guard.feed(event([{'text': 'suffix'}], 'STOP' if terminal == 'STOP' else None))]
                        out.append(guard.flush())
                        self.assertEqual(parts(out), [{'text': 'prefix '}, opaque, {'text': 'suffix'}])
                        self.assertTrue(guard.complete)


class OpaqueAPIOrderingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_real_guard_keeps_text_before_following_nontext_output(self):
        for opaque in OPAQUE:
            with self.subTest(opaque=opaque):
                async def upstream(**kwargs):
                    yield event([{'text': 'prefix '}])
                    yield event([opaque])
                    yield event([{'text': 'suffix'}], 'STOP')
                self.saved.reset_mock()
                with patch.object(api, 'stream_post_async', upstream):
                    out = [c async for c in api.stream_request({'model': 'gemini-4-argon-medium',
                        'request': {'contents': [{'role': 'user', 'parts': [{'text': 'test'}]}]}})]
                self.assertEqual(parts(out), [{'text': 'prefix '}, opaque, {'text': 'suffix'}])
                self.assertTrue(self.saved.call_args.args[0]['success'])
