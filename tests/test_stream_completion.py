import asyncio
import json
import unittest
from unittest.mock import patch
import test_custom_audit
from src.api import antigravity as api
from src.stream_metadata import StreamCompletion


def event(text='OK', reason=None, index=0):
    c={'index':index,'content':{'parts':[{'text':text}]}}
    if reason:c['finishReason']=reason
    return 'data: '+json.dumps({'response':{'responseId':'synthetic-completion','candidates':[c]}})+'\n\n'


class CompletionTests(unittest.TestCase):
    def test_candidate_metadata_and_signature_are_not_another_answer(self):
        state = StreamCompletion(2)
        state.observe(event(reason="STOP"))
        state.observe('data: {"candidates":[{"index":1,"groundingMetadata":{},'
                      '"content":{"parts":[{"thoughtSignature":"only-signature"}]}}]}')
        state.observe("data: [DONE]")
        self.assertFalse(state.complete)
        self.assertEqual(state.candidates, {0})

    def test_multiple_candidates_must_all_finish(self):
        state=StreamCompletion(2)
        state.observe(event(reason='STOP'));self.assertFalse(state.complete)
        state.observe(event(index=1));self.assertFalse(state.complete)
        state.observe(event(reason='MAX_TOKENS',index=1));self.assertTrue(state.complete)

    def test_done_requires_response_and_error_never_completes(self):
        state=StreamCompletion();state.observe('data: [DONE]');self.assertFalse(state.complete)
        state.observe(event());self.assertTrue(state.complete)
        state.observe('data: {"error":{"code":500}}');self.assertFalse(state.complete)

    def test_unspecified_is_not_a_finish_reason(self):
        state=StreamCompletion();state.observe(event(reason='FINISH_REASON_UNSPECIFIED'))
        self.assertFalse(state.complete)
        self.assertNotIn('OK',json.dumps(state.as_dict()))


class StreamAccountingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def collect_pair(self, chunks):
        async def upstream(**kwargs):
            for chunk in chunks:
                yield chunk

        body = {'model': 'gemini-4-argon-medium', 'request': {
            'contents': [], 'generationConfig': {'candidateCount': 2}}}
        with patch.object(api, 'stream_post_async', upstream):
            return [chunk async for chunk in api.stream_request(body)]

    async def test_empty_candidate_finishes_before_sibling_text(self):
        for early_index in (0, 1):
            with self.subTest(early_index=early_index):
                candidate = {'index': early_index, 'finishReason': 'MAX_TOKENS',
                             'finishMessage': 'token limit reached'}
                early = 'data: ' + json.dumps({'candidates': [candidate]})
                chunks = await self.collect_pair([early, event('answer', 'STOP', 1 - early_index)])
                first, last = [json.loads(chunk[5:]) for chunk in chunks]
                self.assertEqual(first['candidates'], [candidate])
                self.assertEqual(last['response']['candidates'][0]['index'], 1 - early_index)
                self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)

    async def test_refused_candidate_keeps_its_reason_and_completed_sibling(self):
        answer = event('answer', 'STOP')
        for text in ('', 'partial'):
            refused = event(text, 'RECITATION', 1)
            for upstream in ([refused, answer], [answer, refused]):
                chunks = await self.collect_pair(upstream)
                self.assertEqual([json.loads(chunk[5:]) for chunk in chunks],
                                 [json.loads(chunk[5:]) for chunk in upstream])
                self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)

    async def test_partial_refusal_without_sibling_keeps_error_at_eof_or_done(self):
        refused = event('partial', 'RECITATION', 1)
        for ending in ([], ['data: [DONE]']):
            partial, error = await self.collect_pair([refused, *ending])
            self.assertIn('partial', partial)
            self.assertEqual(json.loads(error.body)['error']['blockReason'], 'RECITATION')
            self.assertEqual(self.saved.call_args.args[0]['status_code'], 400)

    async def test_refusal_without_an_answer_remains_a_client_error(self):
        refused = event('', 'RECITATION', 1)
        prompt_block = 'data: {"promptFeedback":{"blockReason":"SAFETY"}}'
        for upstream in ([refused], [refused, event('', 'SAFETY')],
                         [refused, prompt_block, event('answer', 'STOP')]):
            chunks = await self.collect_pair(upstream)
            self.assertEqual(len(chunks), 1)
            self.assertEqual(chunks[0].status_code, 400)
            self.assertEqual(self.saved.call_args.args[0]['status_code'], 400)

    async def test_finished_candidate_alone_still_has_no_output(self):
        chunks = await self.collect_pair([event('', 'MAX_TOKENS'), event('', 'STOP', index=1)])
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].status_code, 502)

    async def test_done_with_a_missing_candidate_reports_error_before_client_can_stop(self):
        for index in (0, 1):
            with self.subTest(index=index):
                chunks = await self.collect_pair([event('answer', 'STOP', index), 'data: [DONE]'])
                answer, error = chunks
                self.assertEqual(json.loads(answer[5:])['response']['candidates'][0]['index'], index)
                self.assertEqual(json.loads(error.body)['error']['status'], 'UPSTREAM_INCOMPLETE_STREAM')
                self.assertEqual(self.saved.call_args.args[0]['status_code'], 500)

    async def test_done_can_finish_two_observed_candidates_without_separate_stops(self):
        chunks = await self.collect_pair([event('second', index=1), event('first'), 'data: [DONE]'])
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[-1].strip(), 'data: [DONE]')
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)

    async def test_finished_candidate_does_not_finish_an_interrupted_sibling(self):
        chunks = await self.collect_pair([event('', 'MAX_TOKENS'), event('partial', index=1)])
        self.assertEqual(len(chunks), 3)
        self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_INCOMPLETE_STREAM')

    async def run_close(self, chunks, expected, *, close=False):
        blocked=asyncio.Event()
        async def upstream(**kwargs):
            for chunk in chunks:yield chunk
            blocked.set();await asyncio.Event().wait()
        with patch.object(api,'stream_post_async',upstream):
            stream=api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
            for _ in chunks:await anext(stream)
            if close:await stream.aclose()
            else:
                task=asyncio.create_task(anext(stream));await blocked.wait();task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
                await stream.aclose()
        self.assertEqual(self.saved.await_count,1)
        row=self.saved.call_args.args[0]
        self.assertEqual((row['status_code'],row['success']),expected)

    async def test_stop_then_cancel_is_success(self):
        await self.run_close([event('OK','STOP')],(200,True))

    async def test_stop_then_aclose_is_success(self):
        await self.run_close([event('OK','STOP')],(200,True),close=True)

    async def test_internal_cancel_is_unknown_interruption(self):
        await self.run_close([event('partial')],(500,False))

    async def test_internal_generator_close_is_unknown_interruption(self):
        await self.run_close([event('partial')],(500,False),close=True)

    async def test_short_internal_close_is_unknown_interruption(self):
        await self.run_close([event('partial')],(500,False),close=True)

    async def test_done_then_cancel_is_success(self):
        await self.run_close([event(),'data: [DONE]\n\n'],(200,True))

    async def test_unmarked_finished_answer_is_success_on_close(self):
        await self.run_close([event('ordinary answer','STOP')],(200,True),close=True)

    async def test_asyncio_cancel_during_success_log_preserves_outcome(self):
        entered=asyncio.Event();calls=0
        async def detail(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==1:entered.set();await asyncio.Event().wait()
            return ''
        async def upstream(**kwargs):yield event('OK', 'STOP')
        with patch.object(api,'stream_post_async',upstream), patch.object(api,'_record_request_body_detail',detail):
            stream=api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
            await anext(stream)
            task=asyncio.create_task(anext(stream));await entered.wait();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            await stream.aclose()
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200)

    async def test_anyio_cancel_during_success_log_finishes_once(self):
        import anyio
        entered=asyncio.Event();release=asyncio.Event()
        async def detail(*args,**kwargs):
            entered.set();await release.wait();return ''
        async def upstream(**kwargs):yield event(reason="STOP")
        async def consume():
            async for _ in api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}}):pass
        with patch.object(api,'stream_post_async',upstream), patch.object(api,'_record_request_body_detail',detail):
            async with anyio.create_task_group() as group:
                group.start_soon(consume);await entered.wait();group.cancel_scope.cancel();release.set()
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200)

    async def asgi_disconnect(self, finished):
        from starlette.responses import StreamingResponse
        disconnected=asyncio.Event();sent=[]
        async def upstream(**kwargs):
            yield event('OK', 'STOP' if finished else None)
            await asyncio.Event().wait()
        async def receive():
            await disconnected.wait();return {'type':'http.disconnect'}
        async def send(message):
            if message['type']=='http.response.body' and message.get('body'):
                sent.append(message['body']);disconnected.set()
        with patch.object(api,'stream_post_async',upstream):
            from src.router.stream_passthrough import build_streaming_response_or_error
            response=await build_streaming_response_or_error(api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}}))
            await asyncio.wait_for(response({'type':'http','asgi':{'spec_version':'2.3'}},receive,send),5)
        self.assertEqual(len(sent),1)
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200 if finished else 499)

    async def test_starlette_disconnect_after_stop_is_success(self):
        await self.asgi_disconnect(True)

    async def test_starlette_disconnect_mid_answer_is_499(self):
        await self.asgi_disconnect(False)
