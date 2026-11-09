"""Packet boundaries must not change classification or upstream thought order."""
import json
import unittest

from src.thought_boundary import ThoughtBoundaryGuard
from test_thought_boundary import event, text_of, decode
from src.converter.openai2gemini import convert_gemini_to_openai_stream
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream

MARKER = '<<<AGY_FINAL_0123456789abcdef>>>'


class BoundaryOrderingTests(unittest.TestCase):
    def test_word_prefixes_are_not_completed_words_at_packet_end(self):
        for word in ('markerboard', 'markerless', 'marker_name'):
            answer = 'Wait, I need to output the exact ' + word + ' for the map.'
            source = MARKER + answer
            for split in range(len(source) + 1):
                with self.subTest(word=word, split=split):
                    g = ThoughtBoundaryGuard(MARKER)
                    out = [g.feed(event([{'text': source[:split]}])),
                           g.feed(event([{'text': source[split:]}], 'STOP'))]
                    self.assertEqual(text_of(out), answer)
                    self.assertFalse(g.failed)
            g = ThoughtBoundaryGuard(MARKER)
            out = [g.feed(event([{'text': c}])) for c in source]
            out.append(g.flush())
            self.assertEqual(text_of(out), answer)

    def test_native_thought_waits_for_earlier_unclassified_text(self):
        for ending in ('STOP', 'EOF'):
            g = ThoughtBoundaryGuard(MARKER)
            native = {'text': 'second native. ', 'thought': True, 'thoughtSignature': 'keep-native'}
            out = [g.feed(event([{'text': 'first planning. '}])),
                   g.feed(event([native])),
                   g.feed(event([{'text': MARKER + 'answer'}]))]
            out.append(g.feed(event([], 'STOP')) if ending == 'STOP' else g.flush())
            self.assertEqual(text_of(out, True), 'first planning. second native. ')
            self.assertEqual(text_of(out), 'answer')
            parts = [p for c in out if c for candidate in decode(c)['candidates'] for p in candidate['content']['parts']]
            self.assertEqual(parts[1], native)
            self.assertTrue(g.complete)

    def test_interleaved_parts_keep_order_at_every_marker_split(self):
        for split in range(len(MARKER) + 1):
            g = ThoughtBoundaryGuard(MARKER)
            out = [g.feed(event([{'text': 'first. '}, {'text': 'native one. ', 'thought': True},
                                  {'text': 'third. ' + MARKER[:split]}])),
                   g.feed(event([{'text': 'native two. ', 'thought': True},
                                  {'text': MARKER[split:] + 'answer'}], 'STOP'))]
            self.assertEqual(text_of(out, True), 'first. native one. third. native two. ')
            self.assertEqual(text_of(out), 'answer')

    def test_buffered_plain_answer_stays_before_native_thought_at_eof(self):
        g = ThoughtBoundaryGuard(MARKER)
        native = {'text': 'native', 'thought': True, 'thoughtSignature': 'keep'}
        out = [g.feed(event([{'text': 'short body'}])), g.feed(event([native])), g.flush()]
        parts = [p for c in out if c for candidate in decode(c)['candidates'] for p in candidate['content']['parts']]
        self.assertEqual(parts, [{'text': 'short body'}, native])

    def test_thought_optout_does_not_expose_or_retain_queued_native_parts(self):
        g = ThoughtBoundaryGuard(MARKER, include_thoughts=False)
        out = [g.feed(event([{'text': 'first plan'}, {'text': 'native', 'thought': True},
                              {'text': MARKER + 'answer'}], 'STOP'))]
        self.assertEqual(text_of(out, True), '')
        self.assertEqual(text_of(out), 'answer')
        self.assertTrue(g.complete)


class BoundaryOrderingConverterTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_protocol_converters_preserve_interleaved_thought_order(self):
        g = ThoughtBoundaryGuard(MARKER)
        chunks = [g.feed(event([{'text': 'first. '}])),
                  g.feed(event([{'text': 'second. ', 'thought': True, 'thoughtSignature': 'signature'}])),
                  g.feed(event([{'text': MARKER + 'answer'}], 'STOP'))]
        converted = [convert_gemini_to_openai_stream(c, 'gemini-4-argon-medium', 'test') for c in chunks]
        deltas = [choice['delta'] for c in converted if c for choice in json.loads(c[5:]).get('choices', [])]
        self.assertEqual(''.join(d.get('reasoning_content', '') for d in deltas), 'first. second. ')
        self.assertEqual(''.join(d.get('content', '') for d in deltas), 'answer')
        async def stream():
            for c in chunks:
                yield c.encode()
        converted = [c async for c in gemini_stream_to_anthropic_stream(stream(), 'gemini-4-argon-medium')]
        deltas = [json.loads(line[5:]).get('delta', {}) for c in converted for line in c.decode().splitlines() if line.startswith('data:')]
        self.assertEqual(''.join(d.get('thinking', '') for d in deltas), 'first. second. ')
        self.assertEqual(''.join(d.get('text', '') for d in deltas), 'answer')
