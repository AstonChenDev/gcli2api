import json
import unittest
from unittest.mock import AsyncMock, patch
import httpx
from fastapi import FastAPI
import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import openai
from src.converter import openai2gemini as converter


def candidate(index=None, tool=None, text=None, finish=None):
    parts = []
    if tool:
        parts.append({'functionCall': {'name': tool, 'args': {'name': tool}}})
    if text:
        parts.append({'text': text})
    value = {'content': {'role': 'model', 'parts': parts}}
    if index is not None:
        value['index'] = index
    if finish:
        value['finishReason'] = finish
    return value


class CandidateToolTests(unittest.TestCase):
    def setUp(self):
        converter._STREAM_TOOL_INDEX.clear()

    def stream(self, candidates, response_id='test'):
        wire = 'data: ' + json.dumps({'candidates': candidates})
        return json.loads(converter.convert_gemini_to_openai_stream(wire, 'test', response_id)[5:])['choices']

    def test_each_candidate_has_independent_tool_indices_and_finish_reason(self):
        first = self.stream([candidate(0, tool='a'), candidate(1, tool='b')])
        self.assertEqual([v['delta']['tool_calls'][0]['index'] for v in first], [0, 0])
        second = self.stream([candidate(0, finish='STOP'), candidate(1, tool='c')])
        self.assertEqual(second[0]['finish_reason'], 'tool_calls')
        self.assertEqual(second[1]['delta']['tool_calls'][0]['index'], 1)
        third = self.stream([candidate(1, tool='d')])
        self.assertEqual(third[0]['delta']['tool_calls'][0]['index'], 2)
        self.assertEqual(self.stream([candidate(1, finish='STOP')])[0]['finish_reason'], 'tool_calls')
        self.assertNotIn('test', converter._STREAM_TOOL_INDEX)

    def test_text_candidate_does_not_inherit_another_candidates_tool_finish(self):
        self.stream([candidate(0, tool='a')])
        text = self.stream([candidate(1, text='answer', finish='STOP')])
        self.assertEqual(text[0]['finish_reason'], 'stop')
        self.assertEqual(self.stream([candidate(0, finish='STOP')])[0]['finish_reason'], 'tool_calls')

    def test_implicit_candidate_indices_are_preserved_in_both_conversion_modes(self):
        candidates = [candidate(text='one', finish='STOP'), candidate(text='two', finish='STOP')]
        stream = self.stream(candidates)
        normal = converter.convert_gemini_to_openai_response({'candidates': candidates}, 'test')['choices']
        for choices in (stream, normal):
            self.assertEqual([v['index'] for v in choices], [0, 1])

    def test_explicit_indices_and_truncation_stay_intact(self):
        result = self.stream([candidate(3, tool='a', finish='MAX_TOKENS'), candidate(7, text='ok', finish='STOP')])
        self.assertEqual([(v['index'], v['finish_reason']) for v in result], [(3, 'length'), (7, 'stop')])

    def test_unspecified_finish_is_not_terminal_and_does_not_reset_tools(self):
        first = self.stream([candidate(0, tool='a', finish='FINISH_REASON_UNSPECIFIED')])
        self.assertIsNone(first[0]['finish_reason'])
        second = self.stream([candidate(0, tool='b')])
        self.assertEqual(second[0]['delta']['tool_calls'][0]['index'], 1)
        self.assertEqual(self.stream([candidate(0, finish='STOP')])[0]['finish_reason'], 'tool_calls')

    def test_owned_stream_states_survive_cache_pressure_and_identical_response_ids(self):
        left, right = {}, {}
        def convert(state, tool):
            wire = 'data: ' + json.dumps({'candidates': [candidate(0, tool=tool)]})
            result = converter.convert_gemini_to_openai_stream(wire, 'test', 'same-id', tool_call_state=state)
            return json.loads(result[5:])['choices'][0]['delta']['tool_calls'][0]['index']
        self.assertEqual(convert(left, 'first'), 0)
        self.assertEqual(convert(right, 'other'), 0)
        for index in range(converter._STREAM_TOOL_INDEX_MAX_ENTRIES + 1):
            self.stream([candidate(0, tool='noise')], response_id=str(index))
        self.assertEqual(convert(left, 'second'), 1)
        self.assertEqual(convert(right, 'other-second'), 1)


class CandidateToolRouteTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_normal_and_anti_routes_keep_tool_state_out_of_global_cache(self):
        app = FastAPI()
        app.include_router(openai.router)
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        for prefix in ('', '抗截断-'):
            with self.subTest(prefix=prefix):
                converter._STREAM_TOOL_INDEX.clear()
                self.saved.reset_mock()
                async def upstream(**kwargs):
                    for candidates in ([candidate(0, tool='a')], [candidate(0, tool='b')], [candidate(0, finish='STOP')]):
                        yield 'data: ' + json.dumps({'response': {'candidates': candidates}}) + '\n\n'
                        self.assertEqual(len(converter._STREAM_TOOL_INDEX), 0)
                with patch.object(api, 'stream_post_async', upstream), patch.object(
                        openai, 'get_anti_truncation_max_attempts', AsyncMock(return_value=1)):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                        result = await client.post('/v1/chat/completions', json={
                            'model': prefix + 'gemini-4-argon-medium', 'stream': True,
                            'messages': [{'role': 'user', 'content': 'test'}],
                            'tools': [{'type': 'function', 'function': {'name': name, 'parameters': {'type': 'object'}}} for name in ('a', 'b')]})
                self.assertEqual(result.status_code, 200)
                events = [json.loads(line[5:]) for line in result.text.splitlines() if line.startswith('data:') and '[DONE]' not in line]
                choices = [v for event in events for v in event.get('choices', [])]
                tools = [t for v in choices for t in v['delta'].get('tool_calls', [])]
                self.assertEqual([t['index'] for t in tools], [0, 1])
                self.assertEqual(choices[-1]['finish_reason'], 'tool_calls')
                self.assertEqual(self.saved.await_count, 1)
                self.assertTrue(self.saved.call_args.args[0]['success'])
