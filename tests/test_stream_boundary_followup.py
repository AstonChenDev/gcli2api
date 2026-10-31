import json
import unittest
from src.thought_boundary import ThoughtBoundaryGuard
import test_stream_event_recovery as recovery
from test_stream_event_recovery import event, parts_of
from test_marker_copy_recovery import MARKER


class ExplicitTextHeadingTests(unittest.TestCase):
    def test_false_flag_on_every_chunk_does_not_expose_confirmed_planning(self):
        plan = 'Thinking Process:\n\n1. **Analyze:** Read constraints.\n2. **Plan:** Build the answer.\n'
        for heading in ['Thinking Process:\n', '**Thinking Process:**\n', '## Thinking Process:\n',
                        '# **Thinking Process:**\n', '__Thinking Process:__\r\n']:
            text = heading + plan.split('\n', 1)[1] + MARKER + '最后的正文。'
            for width in [1, 2, 7, 24, 128, 4096]:
                with self.subTest(heading=heading, width=width):
                    guard = ThoughtBoundaryGuard(MARKER)
                    out = [guard.feed(event([{'text': text[i:i+width], 'thought': False}])) for i in range(0, len(text), width)]
                    out.append(guard.flush())
                    # event() uses Google's wrapped response format.
                    ps = parts_of([x for x in out if x])
                    self.assertEqual(''.join(p.get('text','') for p in ps if not p.get('thought')), '最后的正文。')
                    self.assertEqual(''.join(p.get('text','') for p in ps if p.get('thought')), text.split(MARKER)[0])

    def test_unmarked_false_flag_continuation_stays_thought(self):
        g = ThoughtBoundaryGuard(MARKER)
        out = [g.feed(event([{'text': text, 'thought': False}])) for text in ['Thinking Process:\n', 'Still analyzing the request.']]
        out.append(g.flush())
        ps = parts_of([x for x in out if x])
        self.assertTrue(all(p.get('thought') is True for p in ps))
        self.assertTrue(g.metadata()['boundary_uncertain'])

    def test_new_explicit_body_flag_after_unflagged_planning_still_works(self):
        g = ThoughtBoundaryGuard(MARKER)
        out = [g.feed(event([{'text': 'Thinking Process:\nAnalysis.'}])),
               g.feed(event([{'text': '真正正文。', 'thought': False}], 'STOP'))]
        ps = parts_of(out)
        self.assertEqual(''.join(p['text'] for p in ps if not p.get('thought')), '真正正文。')

    def test_ordinary_false_flag_text_is_not_delayed_or_reclassified(self):
        for text in ['普通正文。', 'Thinking Process: this is an ordinary sentence.', 'The answer is 42.']:
            g = ThoughtBoundaryGuard(MARKER)
            out = g.feed(event([{'text': text, 'thought': False}]))
            self.assertEqual(parts_of([out]), [{'text': text}])

    def test_empty_false_placeholder_is_not_evidence_about_the_heading(self):
        g = ThoughtBoundaryGuard(MARKER)
        out = [g.feed(event([{'text':'', 'thought':False}])),
               g.feed(event([{'text':'Thinking Process:\nAnalysis.'}])),
               g.feed(event([{'text':'真正正文。', 'thought':False}], 'STOP'))]
        ps = parts_of(out)
        self.assertEqual(''.join(p.get('text','') for p in ps if not p.get('thought')), '真正正文。')


class ErrorWithContentTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = recovery.EventRecoveryTests.asyncSetUp
    run_attempts = recovery.EventRecoveryTests.run_attempts

    async def test_explicit_error_preserves_same_event_thought_and_body(self):
        for status in [400, 429, 503]:
            self.saved.reset_mock()
            payload = json.loads(event([{'text':'native thinking','thought':True,'thoughtSignature':'keep'},
                                        {'text':'BOUNDARYpartial answer'}], 'STOP')[5:])
            error = {'code':status,'status':'UPSTREAM_REPORTED_ERROR','message':'original detail'}
            payload['response']['error'] = error
            chunks, rows = await self.run_attempts([['data: '+json.dumps(payload)]])
            self.assertEqual(parts_of(chunks), [{'text':'native thinking','thought':True,'thoughtSignature':'keep'}, {'text':'partial answer'}])
            self.assertEqual(self.calls,1)
            self.assertEqual(json.loads(chunks[-1].body)['error'],error)
            self.assertEqual(rows[-1]['status_code'],status)
            self.assertFalse(rows[-1]['success'])
            self.assertNotIn('"finishReason": "STOP"',''.join(c for c in chunks if isinstance(c,str)))

    async def test_short_buffered_content_is_flushed_before_error_without_retry(self):
        payload = json.loads(event([{'text':'short answer'}])[5:])
        payload['response']['error']={'code':503,'status':'UNAVAILABLE','message':'original'}
        chunks, rows = await self.run_attempts([['data: '+json.dumps(payload)]])
        self.assertEqual(parts_of(chunks),[{'text':'short answer'}])
        self.assertEqual(self.calls,1)
        self.assertEqual(rows[-1]['status_code'],503)

    async def test_same_event_error_through_all_http_formats_and_anti_truncation(self):
        from contextlib import ExitStack
        from unittest.mock import AsyncMock, patch
        import httpx
        from fastapi import FastAPI
        from src.api import antigravity as api
        from src.router.antigravity import gemini, openai, anthropic
        app = FastAPI()
        for module in (gemini, openai, anthropic):app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'synthetic'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'synthetic'
        requests = [('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
                    ('/v1/chat/completions', {'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
                    ('/v1/messages', {'model':'gemini-4-argon-medium','stream':True,'max_tokens':100,'messages':[{'role':'user','content':'test'}]})]
        for anti in [False, True]:
            for url, body in requests:
                with self.subTest(anti=anti, url=url):
                    self.saved.reset_mock()
                    async def upstream(**kwargs):
                        d = json.loads(event([{'text':'native-thought','thought':True}, {'text':'BOUNDARYpartial-answer'}], 'STOP')[5:])
                        d['response']['error'] = {'code':400,'status':'INVALID_ARGUMENT','message':'original-error-detail'}
                        yield 'data: '+json.dumps(d)
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api,'stream_post_async',upstream))
                        stack.enter_context(patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')))
                        stack.enter_context(patch.object(api,'record_api_call_error',AsyncMock()))
                        for module in (gemini, openai, anthropic):
                            stack.enter_context(patch.object(module,'is_anti_truncation_model',return_value=anti))
                            stack.enter_context(patch.object(module,'get_anti_truncation_max_attempts',AsyncMock(return_value=1)))
                        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                            response = await client.post(url,json=body)
                    self.assertEqual(response.status_code,200)
                    self.assertIn('partial-answer',response.text)
                    self.assertIn('native-thought',response.text)
                    self.assertIn('original-error-detail',response.text)
                    self.assertNotIn('end_turn',response.text)
                    self.assertNotIn('"finish_reason": "stop"',response.text)
                    self.assertNotIn('"finishReason": "STOP"',response.text)
                    self.assertEqual(self.saved.call_args.args[0]['status_code'],400)
