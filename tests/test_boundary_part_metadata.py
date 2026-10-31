"""Buffered text must retain the metadata of its original upstream part."""
import copy
import unittest
from src.thought_boundary import ThoughtBoundaryGuard
from test_thought_boundary import event, decode, text_of

MARKER = '<<<AGY_FINAL_0123456789abcdef>>>'

def parts_of(events):
    return [part for value in events if value for candidate in decode(value)['candidates'] for part in candidate['content']['parts']]


class BoundaryPartMetadataTests(unittest.TestCase):
    def test_character_chunks_keep_metadata_and_drain_buffer_with_hidden_thoughts(self):
        planning = 'Thinking Process:\n1. **Plan:** first.\n2. **Write:** next.'
        answer = '正文：完整回答。'
        for include_thoughts in (True, False):
            for tail in ('', MARKER[:18]):
                with self.subTest(include_thoughts=include_thoughts, tail=tail):
                    guard = ThoughtBoundaryGuard(MARKER, include_thoughts)
                    text = planning + MARKER + answer + tail
                    output = []
                    for index, char in enumerate(text):
                        output.append(guard.feed(event([{'text':char, 'thoughtSignature':f'sig-{index}'}])))
                        for state in guard.states.values():
                            self.assertEqual(''.join(segment[0] for segment in state['segments']), state['pending'])
                    output.append(guard.flush())
                    self.assertEqual(text_of(output, True), planning if include_thoughts else '')
                    self.assertEqual(text_of(output), answer)
                    for part in parts_of(output):
                        index = int(part['thoughtSignature'].removeprefix('sig-'))
                        self.assertEqual(part['text'], text[index])
                    self.assertTrue(all(not state['segments'] for state in guard.states.values()))

    def test_buffered_parts_keep_their_own_signatures_at_stop_and_eof(self):
        original = [{'text':'first ', 'thoughtSignature':'signature-A'}, {'text':'second', 'thoughtSignature':'signature-B'}]
        for end in ('STOP','EOF'):
            with self.subTest(end=end):
                guard = ThoughtBoundaryGuard(MARKER)
                before = copy.deepcopy(original)
                output = [guard.feed(event([part])) for part in original]
                output.append(guard.feed(event([], 'STOP')) if end == 'STOP' else guard.flush())
                self.assertEqual(parts_of(output), original)
                self.assertEqual(original, before)

    def test_marker_split_does_not_attach_next_parts_signature_to_previous_text(self):
        for split in range(len(MARKER)+1):
            with self.subTest(split=split):
                guard = ThoughtBoundaryGuard(MARKER)
                output = [guard.feed(event([{'text':'Thinking Process:\nplan' + MARKER[:split], 'thoughtSignature':'plan-signature'}])),
                    guard.feed(event([{'text':MARKER[split:] + 'answer', 'thoughtSignature':'answer-signature'}], 'STOP'))]
                self.assertEqual(text_of(output, True), 'Thinking Process:\nplan')
                self.assertEqual(text_of(output), 'answer')
                for part in parts_of(output):
                    self.assertEqual(part.get('thoughtSignature'), 'plan-signature' if part.get('thought') else 'answer-signature')
