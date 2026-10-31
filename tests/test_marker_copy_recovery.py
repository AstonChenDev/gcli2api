import json
import unittest
from src.thought_boundary import ThoughtBoundaryGuard

MARKER = '<<<AGY_FINAL_22de2366556dbd79>>>'
WRONG_DIGIT = '<<<AGY_FINAL_22de3366556dbd79>>>'
DUPLICATED_RUN = '<<<AGY_FINAL_22de22de2366556dbd79>>>'

def event(parts):
    return 'data: ' + json.dumps({'candidates': [{'content': {'parts': parts}}]}) + '\n\n'

def collect(outputs):
    parts = [part for output in outputs if output for candidate in json.loads(output[5:])['candidates']
             for part in candidate.get('content', {}).get('parts', [])]
    return (''.join(p.get('text', '') for p in parts if not p.get('thought')),
            ''.join(p.get('text', '') for p in parts if p.get('thought')), parts)

class MarkerCopyRecoveryTests(unittest.TestCase):
    def test_native_thought_and_both_observed_nonce_errors_all_split_positions(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            for split in range(len(bad) + 1):
                with self.subTest(marker=bad, split=split):
                    g = ThoughtBoundaryGuard(MARKER)
                    native = {'text': 'native planning', 'thought': True, 'thoughtSignature': 'keep-exact'}
                    outputs = [g.feed(event([native]))]
                    for text in [bad[:split], bad[split:], '中文正文。']:
                        outputs.append(g.feed(event([{'text': text}])))
                    outputs.append(g.flush())
                    b, t, parts = collect(outputs)
                    self.assertEqual((b, t), ('中文正文。', 'native planning'))
                    self.assertEqual(parts[0], native)
                    self.assertEqual(g.metadata()['boundary_marker_corrections'], 1)

    def test_thinking_heading_still_separates_before_corrupted_marker(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            for width in [1, 2, 7, 24, 64]:
                with self.subTest(marker=bad, width=width):
                    g = ThoughtBoundaryGuard(MARKER)
                    before = 'Thinking Process:\n\n1. **Analyze:** Make a plan.\n'
                    source = before + bad + '实际的中文正文。'
                    outputs = [g.feed(event([{'text': source[i:i + width]}])) for i in range(0, len(source), width)]
                    outputs.append(g.flush())
                    self.assertEqual(collect(outputs)[:2], ('实际的中文正文。', before))

    def test_corrupted_marker_inside_answer_is_buffered_across_chunks(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            g = ThoughtBoundaryGuard(MARKER)
            outputs = [g.feed(event([{'text': 'first paragraph', 'thought': False}]))]
            outputs += [g.feed(event([{'text': char}])) for char in bad + ' second paragraph']
            outputs.append(g.flush())
            self.assertEqual(collect(outputs)[:2], ('first paragraph second paragraph', ''))

    def test_arbitrary_foreign_nonce_does_not_end_confirmed_thought(self):
        g = ThoughtBoundaryGuard(MARKER)
        source = 'Thinking Process:\nDiscuss the literal example <<<AGY_FINAL_0123456789abcdef>>> carefully.'
        outputs = [g.feed(event([{'text': char}])) for char in source]
        outputs.append(g.flush())
        self.assertEqual(collect(outputs)[:2], ('', source))
        self.assertTrue(g.metadata()['boundary_uncertain'])
        self.assertEqual(g.metadata()['boundary_marker_corrections'], 0)

    def test_unfinished_recoverable_marker_is_not_exposed_at_eof(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            for size in range(len('<<<AGY_FINAL_'), len(bad)):
                with self.subTest(marker=bad, size=size):
                    g = ThoughtBoundaryGuard(MARKER)
                    outputs = [g.feed(event([{'text': '完整正文。', 'thought': False}]))]
                    outputs += [g.feed(event([{'text': char}])) for char in bad[:size]]
                    outputs.append(g.flush())
                    self.assertEqual(collect(outputs)[:2], ('完整正文。', ''))

    def test_all_single_substitutions_and_short_duplicate_runs(self):
        nonce = '22de2366556dbd79'
        variants = {nonce[:i] + char + nonce[i + 1:] for i, old in enumerate(nonce)
                    for char in '0123456789abcdef' if char != old}
        variants |= {nonce[:i] + nonce[i:i + size] + nonce[i:]
                     for size in range(1, 5) for i in range(len(nonce) - size + 1)}
        for nonce_variant in variants:
            with self.subTest(nonce=nonce_variant):
                g = ThoughtBoundaryGuard(MARKER)
                outputs = [g.feed(event([{'text': '<<<AGY_FINAL_' + nonce_variant + '>>>正文'}])), g.flush()]
                self.assertEqual(collect(outputs)[:2], ('正文', ''))
                self.assertEqual(g.marker_corrections, 1)

class MarkerCopyApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_corrected_boundaries_return_success_in_all_three_protocols(self):
        from unittest.mock import patch
        from test_custom_audit import CustomAuditTests
        from src.api import antigravity as api
        from src.converter.openai2gemini import convert_gemini_to_openai_stream
        from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream
        from fastapi import Response
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            fixture = CustomAuditTests()
            await fixture.asyncSetUp()
            async def upstream(**kwargs):
                yield event([{'text': 'native planning', 'thought': True, 'thoughtSignature': 'keep-exact'}])
                for i in range(0, len(bad + '最终正文。'), 7):
                    yield event([{'text': (bad + '最终正文。')[i:i + 7]}])
                yield 'data: ' + json.dumps({'candidates': [{'finishReason': 'STOP'}]}) + '\n\n'
            try:
                with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)):
                    chunks = [c async for c in api.stream_request({'model': 'gemini-4-argon-medium', 'request': {'contents': []}})]
                self.assertFalse(any(isinstance(c, Response) for c in chunks))
                self.assertEqual(collect(chunks)[:2], ('最终正文。', 'native planning'))
                self.assertEqual(fixture.saved.call_args.args[0]['status_code'], 200)
                self.assertTrue(fixture.saved.call_args.args[0]['success'])
                converted = [convert_gemini_to_openai_stream(c, 'gemini-4-argon-medium', 'test') for c in chunks]
                deltas = [choice['delta'] for c in converted if c for choice in json.loads(c[5:]).get('choices', [])]
                self.assertEqual(''.join(d.get('content', '') for d in deltas), '最终正文。')
                self.assertEqual(''.join(d.get('reasoning_content', '') for d in deltas), 'native planning')
                async def stream():
                    for c in chunks:yield c.encode()
                converted = [c async for c in gemini_stream_to_anthropic_stream(stream(), 'gemini-4-argon-medium')]
                deltas = [json.loads(line[5:]).get('delta', {}) for c in converted for line in c.decode().splitlines() if line.startswith('data:')]
                self.assertEqual(''.join(d.get('text', '') for d in deltas), '最终正文。')
                self.assertEqual(''.join(d.get('thinking', '') for d in deltas), 'native planning')
            finally:
                fixture.stack.close()

class MarkerCopyOptionTests(unittest.TestCase):
    def test_thought_optout_preserves_answer_after_recovered_marker(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            g = ThoughtBoundaryGuard(MARKER, include_thoughts=False)
            outputs = [g.feed(event([{'text': 'native planning', 'thought': True}]))]
            source = 'Thinking Process:\nAdditional planning.\n' + bad + '最终正文。'
            outputs += [g.feed(event([{'text': char}])) for char in source]
            outputs.append(g.flush())
            self.assertEqual(collect(outputs)[:2], ('最终正文。', ''))

    def test_collision_suffix_remains_buffered_for_corrected_marker(self):
        for bad in [WRONG_DIGIT, DUPLICATED_RUN]:
            g = ThoughtBoundaryGuard(MARKER + '__')
            outputs = [g.feed(event([{'text': '正文', 'thought': False}]))]
            outputs += [g.feed(event([{'text': char}])) for char in bad + '__继续。']
            outputs.append(g.flush())
            self.assertEqual(collect(outputs)[:2], ('正文继续。', ''))
