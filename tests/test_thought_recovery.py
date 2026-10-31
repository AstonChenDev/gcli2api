"""Regression contracts for leaked headings, ambiguous boundaries and wire errors."""
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
from test_thought_boundary import ThoughtBoundaryGuard, event, text_of, MARKER
import test_stream_event_recovery as recovery
from test_stream_event_recovery import parts_of

PLAN = ('Thinking Process:\n\n1.  **Analyze Request**:\n'
        '    *   Check names (人物) and constraints.\n\n'
        '2.  **Writing and Polishing**:\n'
        '    *   Keep the prose grounded and ensure smooth transitions.')
STORY = '老人把那盏已经熄灭的油灯轻轻放在窗台上，抬头望着院子里的梧桐树，半晌没有说话。'


class FramingRecoveryTests(unittest.TestCase):
    def split_stream(self, source, sizes):
        guard = ThoughtBoundaryGuard(MARKER)
        chunks = []; offset = 0
        for size in sizes:
            chunks.append(guard.feed(event([{'text': source[offset:offset+size]}])))
            offset += size
        chunks.append(guard.feed(event([{'text': source[offset:]}], 'STOP')))
        return guard, chunks

    def test_every_heading_and_transition_split(self):
        source = PLAN + STORY
        for i in list(range(25)) + list(range(len(PLAN)-10, len(source)+1)):
            with self.subTest(split=i):
                guard, chunks = self.split_stream(source, [i])
                self.assertEqual(text_of(chunks, True), PLAN)
                self.assertEqual(text_of(chunks), STORY)
                self.assertFalse(guard.metadata()['boundary_uncertain'])

    def test_character_chunks_and_bounded_memory(self):
        source = PLAN + STORY
        guard, chunks = self.split_stream(source, [1]*len(source))
        self.assertEqual(text_of(chunks, True), PLAN)
        self.assertEqual(text_of(chunks), STORY)
        guard = ThoughtBoundaryGuard(MARKER)
        guard.feed(event([{'text': 'Thinking Process:\n'}]))
        for _ in range(100):
            guard.feed(event([{'text': 'analysis ' * 100}]))
            self.assertLessEqual(len(guard.states[0]['pending']), guard.LOOKBEHIND)

    def test_markdown_header_and_ordinary_mentions(self):
        for prefix in ['**Thinking Process:**\n', '## Thinking Process:\n', '__Thinking Process:__\n']:
            _, chunks = self.split_stream(prefix + 'analysis' + MARKER + 'answer', [1]*len(prefix))
            self.assertEqual(text_of(chunks, True), prefix+'analysis')
            self.assertEqual(text_of(chunks), 'answer')
        for body in ['Thinking Process is the title of this document.',
                     'Thinking Process: this is an ordinary sentence, not a heading.',
                     'The phrase Thinking Process: is quoted here.',
                     '这是正文中引用的 Thinking Process: 标题。', 'Thinking about the problem is useful.']:
            _, chunks = self.split_stream(body, [1]*len(body))
            self.assertEqual(text_of(chunks), body)
            self.assertEqual(text_of(chunks, True), '')

    def test_chinese_planning_and_quoted_examples_are_not_body(self):
        for suffix in ['需要先检查人物的行为是否符合设定，再确认场景与前文之间的关系是否合理。',
                       '\n    *   '+STORY, '\nExample: "'+STORY+'"',
                       '\n例如：'+STORY]:
            guard, chunks = self.split_stream(PLAN+suffix, [1]*len(PLAN+suffix))
            self.assertEqual(text_of(chunks), '')
            self.assertEqual(text_of(chunks, True), PLAN+suffix)
            self.assertTrue(guard.metadata()['boundary_uncertain'])

    def test_partial_private_marker_never_leaks_on_finish(self):
        guard = ThoughtBoundaryGuard(MARKER)
        chunks = [guard.feed(event([{'text': MARKER + 'answer' + MARKER[:-2]}], 'STOP'))]
        self.assertEqual(text_of(chunks), 'answer')

    def test_explicit_native_body_without_marker(self):
        guard = ThoughtBoundaryGuard(MARKER)
        thought = {'text': 'native thinking', 'thought': True, 'thoughtSignature': 'synthetic'}
        chunks = [guard.feed(event([thought, {'text': 'final answer', 'thought': False}], 'STOP'))]
        self.assertEqual(text_of(chunks, True), thought['text'])
        self.assertEqual(text_of(chunks), 'final answer')
        self.assertIn(thought, json.loads(chunks[0][5:])['response']['candidates'][0]['content']['parts'])


class OutcomeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = recovery.EventRecoveryTests.asyncSetUp
    run_attempts = recovery.EventRecoveryTests.run_attempts
    async def test_marker_only_and_signature_only_are_empty(self):
        for parts in [[{'text': 'BOUNDARY'}], [{'thoughtSignature': 'synthetic'}]]:
            self.saved.reset_mock()
            chunks, rows = await self.run_attempts([[event(parts, 'STOP')]], enabled=False)
            self.assertEqual(rows[-1]['status_code'], 502)
            self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_EMPTY_RESPONSE')

    async def test_thought_only_success_and_missing_finish_error(self):
        for finish, expected in [('STOP', 200), (None, 500)]:
            self.saved.reset_mock()
            chunks, rows = await self.run_attempts([[event([{'text': 'Thinking Process:\nanalysis'}], finish)]], enabled=False)
            self.assertEqual(rows[-1]['status_code'], expected)
            self.assertEqual(''.join(p.get('text','') for p in parts_of(chunks) if p.get('thought')), 'Thinking Process:\nanalysis')

    async def test_done_flushes_buffer_without_losing_text(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'normal answer'}]), 'data: [DONE]']])
        self.assertTrue(rows[-1]['success'])
        self.assertEqual(parts_of(chunks), [{'text': 'normal answer'}])

    async def test_hidden_thought_allows_retry_before_visible_content(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'native', 'thought': True}]),
            'data: {"error":{"code":503,"status":"UNAVAILABLE"}}'],
            [event([{'text':'BOUNDARYanswer'}], 'STOP')]], include=False)
        self.assertEqual(self.calls, 2)
        self.assertEqual(parts_of(chunks), [{'text':'answer'}])
        self.assertEqual([r['status_code'] for r in rows], [503,200])

    async def test_blocked_same_chunk_preserves_partial_answer(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'BOUNDARYpartial answer'}], 'PROHIBITED_CONTENT')]])
        self.assertEqual(parts_of(chunks), [{'text': 'partial answer'}])
        self.assertEqual(rows[-1]['status_code'], 400)
        self.assertEqual(json.loads(chunks[-1].body)['error']['blockReason'], 'PROHIBITED_CONTENT')

    async def test_hidden_reclassified_thought_only_is_not_empty(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'Thinking Process:\nanalysis'}], 'STOP')]], include=False)
        self.assertEqual(parts_of(chunks), [])
        self.assertTrue(rows[-1]['success'])

    async def test_protocol_converters_preserve_whitespace_and_ellipsis(self):
        from src.converter.openai2gemini import convert_gemini_to_openai_stream
        from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream
        chunks = [event([{'text': c}]) for c in '甲\n\n…… 乙']
        chunks.append(event([{'text':'first', 'thought':True}, {'text':'second', 'thought':True}], 'STOP'))
        ds = [json.loads(convert_gemini_to_openai_stream(c,'gemini-4-argon-medium','test')[5:])['choices'][0]['delta'] for c in chunks]
        self.assertEqual(''.join(d.get('content','') for d in ds), '甲\n\n…… 乙')
        self.assertEqual(''.join(d.get('reasoning_content','') for d in ds), 'firstsecond')
        async def source():
            for c in chunks: yield c.encode()
        output = [c async for c in gemini_stream_to_anthropic_stream(source(),'gemini-4-argon-medium')]
        ds = [json.loads(l[5:]).get('delta',{}) for c in output for l in c.decode().splitlines() if l.startswith('data:')]
        self.assertEqual(''.join(d.get('text','') for d in ds), '甲\n\n…… 乙')
        self.assertEqual(''.join(d.get('thinking','') for d in ds), 'firstsecond')

    async def test_three_protocols_normal_and_anti_preserve_midstream_error(self):
        from src.router.antigravity import gemini, openai, anthropic
        from src.api import antigravity as api
        app = FastAPI()
        for module in (gemini, openai, anthropic): app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'synthetic'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'synthetic'
        requests = [('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
                    ('/v1/chat/completions', {'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
                    ('/v1/messages', {'model':'gemini-4-argon-medium','stream':True,'max_tokens':100,'messages':[{'role':'user','content':'test'}]})]
        for anti in [False, True]:
            for url, body in requests:
                for blocked in [False, True]:
                    with self.subTest(anti=anti,url=url,blocked=blocked):
                        async def upstream(**kwargs):
                            yield event([{'text':'native-thought','thought':True}, {'text':'BOUNDARYanswer'}])
                            yield (event([], finish='PROHIBITED_CONTENT') if blocked else
                                   'data: {"error":{"code":400,"status":"INVALID_ARGUMENT","message":"synthetic detail"}}')
                        with patch.object(api,'stream_post_async',upstream), patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')), patch.object(api,'record_api_call_error',AsyncMock()):
                            from contextlib import ExitStack
                            with ExitStack() as stack:
                                for module in (gemini, openai, anthropic):
                                    stack.enter_context(patch.object(module,'is_anti_truncation_model',return_value=anti))
                                    stack.enter_context(patch.object(module,'get_anti_truncation_max_attempts',AsyncMock(return_value=1)))
                                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                                    response = await client.post(url,json=body)
                        self.assertEqual(response.status_code,200)
                        self.assertIn('PROHIBITED_CONTENT' if blocked else 'INVALID_ARGUMENT',response.text)
                        self.assertNotIn('end_turn',response.text)
                        self.assertNotIn('"finish_reason": "stop"',response.text)
                        if url == '/v1/messages': self.assertIn('event: error\n',response.text)
                        self.assertEqual(self.saved.call_args.args[0]['status_code'],400)
