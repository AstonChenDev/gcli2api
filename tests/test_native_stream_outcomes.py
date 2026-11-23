"""Native thought output and stream-error regression contracts."""
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
from stream_fixtures import event, text_of
import test_stream_event_recovery as recovery
from test_stream_event_recovery import parts_of

class OutcomeRecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = recovery.EventRecoveryTests.asyncSetUp
    run_attempts = recovery.EventRecoveryTests.run_attempts
    async def test_empty_text_and_signature_only_are_empty(self):
        for parts in [[{'text': ''}], [{'thoughtSignature': 'synthetic'}]]:
            self.saved.reset_mock()
            chunks, rows = await self.run_attempts([[event(parts, 'STOP')]], enabled=False)
            self.assertEqual(rows[-1]['status_code'], 502)
            self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_EMPTY_RESPONSE')

    async def test_thought_only_success_and_missing_finish_error(self):
        for finish, expected in [('STOP', 200), (None, 500)]:
            self.saved.reset_mock()
            chunks, rows = await self.run_attempts([[event([{'text': 'Thinking Process:\nanalysis', 'thought': True}], finish)]], enabled=False)
            self.assertEqual(rows[-1]['status_code'], expected)
            self.assertEqual(''.join(p.get('text','') for p in parts_of(chunks) if p.get('thought')), 'Thinking Process:\nanalysis')

    async def test_done_preserves_text(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'normal answer'}]), 'data: [DONE]']])
        self.assertTrue(rows[-1]['success'])
        self.assertEqual(parts_of(chunks), [{'text': 'normal answer'}])

    async def test_returned_native_thought_prevents_retry_after_output(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'native', 'thought': True}]),
            'data: {"error":{"code":503,"status":"UNAVAILABLE"}}'],
            [event([{'text':'answer'}], 'STOP')]], include=False)
        self.assertEqual(self.calls, 1)
        self.assertEqual(parts_of(chunks), [{'text':'native', 'thought': True}])
        self.assertEqual([r['status_code'] for r in rows], [503])

    async def test_blocked_same_chunk_preserves_partial_answer(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'partial answer'}], 'PROHIBITED_CONTENT')]])
        self.assertEqual(parts_of(chunks), [{'text': 'partial answer'}])
        self.assertEqual(rows[-1]['status_code'], 400)
        self.assertEqual(json.loads(chunks[-1].body)['error']['blockReason'], 'PROHIBITED_CONTENT')


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
                            yield event([{'text':'native-thought','thought':True}, {'text':'answer'}])
                            yield (event([], finish='PROHIBITED_CONTENT') if blocked else
                                   'data: {"error":{"code":400,"status":"INVALID_ARGUMENT","message":"synthetic detail"}}')
                        with patch.object(api,'stream_post_async',upstream), patch.object(api,'record_api_call_error',AsyncMock()):
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
