"""Recover only on explicit framing evidence, on the original upstream call."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response

from src.api import antigravity as api
from src.thought_boundary import ThoughtBoundaryGuard, ThoughtBoundaryError
from test_thought_boundary import event, text_of, decode
from test_boundary_pollution import MARKER, MISSING_DIGIT, REPEATED_NONCE
import test_custom_audit

ANSWER = '第9章 正文\nEnglish dialogue is preserved. 他推开了门。'
BAD = '<<<AGY_FINAL_unrecognizable>>>'
SELF_TALK = 'Wait, I need to output the exact marker. '


class RecoveryTests(unittest.TestCase):
    def replay(self, source, width=1, include=True):
        g = ThoughtBoundaryGuard(MARKER, include)
        out = [g.feed(event([{'text': source[i:i+width]}])) for i in range(0, len(source), width)]
        out.append(g.feed(event([], 'STOP')))
        return g, out

    def test_one_missing_nonce_digit_is_recovered_for_every_position(self):
        nonce = MARKER[13:-3]
        for i in range(len(nonce)):
            broken = '<<<AGY_FINAL_' + nonce[:i] + nonce[i+1:] + '>>>'
            for width in (1, 7, 4096):
                with self.subTest(i=i, width=width):
                    g, out = self.replay(broken + ANSWER, width)
                    self.assertEqual(text_of(out), ANSWER)
                    self.assertTrue(g.complete)
                    self.assertEqual(g.marker_corrections, 1)

    def test_observed_five_character_nonce_duplication_recovers_prefix(self):
        # Framing only, copied from the saved real upstream capture. Its STOP
        # was never captured, so this fixture tests text recovery, not completion.
        bad = '<<<AGY_FINAL_6cd866cd8614f927637e1>>>'
        for width in (1, 7, 4096):
            g = ThoughtBoundaryGuard(MARKER)
            source = bad + ANSWER
            out = [g.feed(event([{'text':source[i:i+width]}])) for i in range(0,len(source),width)]
            self.assertEqual(text_of(out),ANSWER)
            self.assertEqual(g.marker_corrections,1)
            self.assertNotIn('finishReason',decode(out[-1])['candidates'][0])

    def test_earlier_answer_in_same_packet_is_not_discarded_to_recover_later_text(self):
        source = MARKER + ANSWER + BAD + '\n' + MARKER + '\nreplacement'
        for width in (1,7,4096):
            g = ThoughtBoundaryGuard(MARKER);out=[]
            with self.assertRaises(ThoughtBoundaryError):
                for i in range(0,len(source),width):out.append(g.feed(event([{'text':source[i:i+width]}])))
                g.flush()
            self.assertNotIn('replacement',text_of(out))

    def test_repeated_marker_limit_prevents_recursive_overflow(self):
        g = ThoughtBoundaryGuard(MARKER)
        with self.assertRaises(ThoughtBoundaryError):g.feed(event([{'text':MARKER*1000+ANSWER}]))
        self.assertEqual(g.failure_reason,'boundary_recovery_limit')

    def test_repeated_marker_runs_recover_without_duplicate_answer(self):
        for source in (MARKER*5, MARKER+'\n  '+MARKER, MISSING_DIGIT+MARKER):
            for width in (1, 7, 29, 4096):
                g, out = self.replay(source + ANSWER, width)
                self.assertEqual(text_of(out).strip(), ANSWER)
                self.assertGreater(g.repeated_markers, 0)
                self.assertNotIn('AGY_FINAL', text_of(out, True))

    def test_unknown_marker_or_self_talk_waits_for_exact_standalone_marker(self):
        for prefix in (BAD, REPEATED_NONCE, MARKER+SELF_TALK, MARKER+'Thinking Process:\ninternal planning'):
            for width in (1, 2, 7, 29, 4096):
                for include in (False, True):
                    with self.subTest(prefix=prefix,width=width,include=include):
                        g, out = self.replay(prefix + '\n' + MARKER + '\n' + ANSWER, width, include)
                        self.assertEqual(text_of(out), ANSWER)
                        self.assertEqual(text_of(out, True), '')
                        self.assertEqual(g.recovery_events, 1)
                        self.assertTrue(g.complete)

    def test_ambiguous_text_and_quoted_or_wrong_markers_still_fail(self):
        endings = [ANSWER, '\n'+MISSING_DIGIT+'\n'+ANSWER, '\n"'+MARKER+'"\n'+ANSWER,
                   '\n```text\n'+MARKER+'\n```\n'+ANSWER, ' '+MARKER+ANSWER,
                   '\n"quoted example\n'+MARKER+'\nquoted"',
                   '\n````text\n```\n'+MARKER+'\n````\n'+ANSWER,
                   '\n```text\n~~~\n'+MARKER+'\n```\n'+ANSWER]
        for ending in endings:
            for width in (1, 4096):
                with self.subTest(ending=ending,width=width):
                    g = ThoughtBoundaryGuard(MARKER);out=[]
                    with self.assertRaises(ThoughtBoundaryError):
                        source=BAD+SELF_TALK+ending
                        for i in range(0,len(source),width):out.append(g.feed(event([{'text':source[i:i+width]}])))
                        out.append(g.feed(event([], 'STOP')))
                    self.assertEqual(text_of(out),'')
                    self.assertTrue(g.failed)

    def test_resynchronization_does_not_invent_a_missing_answer_or_finish(self):
        for source in (MARKER+MARKER, BAD+'\n'+MARKER+'\n', BAD+'\n'+MARKER+'\n'+MARKER[:20]):
            g=ThoughtBoundaryGuard(MARKER)
            g.feed(event([{'text':source}]))
            with self.assertRaises(ThoughtBoundaryError):g.flush()
        g=ThoughtBoundaryGuard(MARKER)
        output=g.feed(event([{'text':BAD+'\n'+MARKER+'\n'+ANSWER}]))
        self.assertEqual(text_of([output]),ANSWER)
        self.assertNotIn('finishReason',decode(output)['candidates'][0])

    def test_native_thought_and_signatures_keep_order_across_recovery(self):
        for include in (True,False):
            g=ThoughtBoundaryGuard(MARKER,include)
            native={'thought':True,'text':'genuine thinking','thoughtSignature':'sig'}
            signature={'thoughtSignature':'opaque'}
            out=[g.feed(event([{'text':BAD},native,signature]))]
            out.append(g.feed(event([{'text':'\n'+MARKER+'\n'+ANSWER}], 'STOP')))
            parts=[p for o in out for c in decode(o)['candidates'] for p in c['content']['parts']]
            self.assertEqual(text_of(out),ANSWER)
            self.assertEqual(text_of(out,True),'genuine thinking' if include else '')
            self.assertIn(signature,parts)
            if include:self.assertLess(parts.index(native),parts.index(signature))
            self.assertLess(parts.index(signature),next(i for i,p in enumerate(parts) if p.get('text')==ANSWER))

    def test_recovery_is_bounded_even_with_native_metadata_only_packets(self):
        g=ThoughtBoundaryGuard(MARKER)
        g.feed(event([{'text':BAD}]))
        with self.assertRaises(ThoughtBoundaryError):
            for _ in range(g.RECOVERY_PART_LIMIT+1):g.feed(event([{'thoughtSignature':'s'}]))
        self.assertEqual(g.failure_reason,'boundary_recovery_limit')
        g=ThoughtBoundaryGuard(MARKER)
        with self.assertRaises(ThoughtBoundaryError):g.feed(event([{'text':BAD+'x'*g.RECOVERY_LIMIT}]))
        self.assertEqual(g.failure_reason,'boundary_recovery_limit')

    def test_large_valid_body_after_early_recovery_marker_is_not_recovery_buffer(self):
        body='正常正文。'*10000
        g,out=self.replay(BAD+'\n'+MARKER+'\n'+body,100000)
        self.assertEqual(text_of(out),body)

    def test_post_body_corruption_does_not_splice_or_drop_existing_answer(self):
        g=ThoughtBoundaryGuard(MARKER)
        out=[g.feed(event([{'text':MARKER+ANSWER}]))]
        with self.assertRaises(ThoughtBoundaryError):g.feed(event([{'text':BAD+'\n'+MARKER+'\nreplacement'}]))
        self.assertEqual(text_of(out),ANSWER)


class RecoveryApiTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_same_upstream_call_recovers_in_all_three_http_protocols(self):
        from src.router.antigravity import gemini, openai, anthropic
        app=FastAPI()
        for module in (gemini,openai,anthropic):app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible]=lambda:'synthetic'
        app.dependency_overrides[openai.authenticate_bearer]=lambda:'synthetic'
        cases=[('/v1beta/models/gemini-4-argon-medium:streamGenerateContent',{'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
               ('/v1/chat/completions',{'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
               ('/v1/messages',{'model':'gemini-4-argon-medium','stream':True,'max_tokens':1000,'messages':[{'role':'user','content':'test'}]})]
        for anti in (False,True):
            for url,body in cases:
                with self.subTest(url=url,anti=anti):
                    self.saved.reset_mock();calls=[];closed=[]
                    async def upstream(**kwargs):
                        calls.append(1)
                        try:
                            for chunk in (BAD,SELF_TALK,'\n'+MARKER+'\n',ANSWER):yield event([{'text':chunk}])
                            yield event([], 'STOP')
                        finally:closed.append(1)
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api,'stream_post_async',upstream))
                        stack.enter_context(patch.object(api,'prepare_thought_boundary',lambda m,r:(r,MARKER)))
                        stack.enter_context(patch.object(api,'get_retry_config',AsyncMock(return_value={'max_retries':3,'retry_interval':0,'retry_enabled':True})))
                        for module in (gemini,openai,anthropic):
                            stack.enter_context(patch.object(module,'is_anti_truncation_model',return_value=anti))
                        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:r=await c.post(url,json=body)
                    self.assertEqual(r.status_code,200)
                    self.assertIn('English dialogue is preserved.',r.text)
                    self.assertNotIn('AGY_FINAL',r.text);self.assertNotIn('Wait,',r.text)
                    self.assertNotIn('UPSTREAM_OUTPUT_BOUNDARY_INVALID',r.text)
                    self.assertEqual(calls,[1]);self.assertEqual(closed,[1])
                    self.assertEqual(self.saved.await_count,1)
                    self.assertTrue(self.saved.call_args.args[0]['success'])

    async def test_unresolved_eof_and_explicit_upstream_error_remain_failures(self):
        for terminal,expected in [('EOF',502),('STOP',502),('429',429)]:
            self.saved.reset_mock();calls=[]
            async def upstream(**kwargs):
                calls.append(1);yield event([{'text':BAD+SELF_TALK}])
                if terminal=='STOP':yield event([], 'STOP')
                if terminal=='429':yield Response(status_code=429,content='{"error":{"code":429}}')
            with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,MARKER)),patch.object(api,'record_api_call_error',AsyncMock()):
                out=[x async for x in api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})]
            self.assertEqual(out[-1].status_code,expected)
            self.assertEqual(calls,[1]);self.assertEqual(self.saved.await_count,1)
            self.assertFalse(self.saved.call_args.args[0]['success'])
            self.assertEqual(text_of(out),'')

    async def test_cancellation_during_recovery_closes_original_upstream(self):
        entered,closed=asyncio.Event(),asyncio.Event()
        async def upstream(**kwargs):
            try:
                yield event([{'text':BAD}])
                entered.set()
                await asyncio.Event().wait()
            finally:closed.set()
        with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,MARKER)):
            stream=api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
            task=asyncio.create_task(anext(stream))
            await asyncio.wait_for(entered.wait(),2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            await stream.aclose()
        self.assertTrue(closed.is_set())
        self.assertEqual(self.saved.await_count,1)
        self.assertFalse(self.saved.call_args.args[0]['success'])
