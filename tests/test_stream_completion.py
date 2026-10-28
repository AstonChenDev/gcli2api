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

    async def run_close(self, chunks, expected, *, close=False, guard=False):
        blocked=asyncio.Event()
        async def upstream(**kwargs):
            for chunk in chunks:yield chunk
            blocked.set();await asyncio.Event().wait()
        def prepare(model,request):return request, ('BOUNDARY' if guard else None)
        with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',prepare):
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
        await self.run_close([event('BOUNDARYOK','STOP')],(200,True),guard=True)

    async def test_stop_then_aclose_is_success(self):
        await self.run_close([event('BOUNDARYOK','STOP')],(200,True),guard=True,close=True)

    async def test_body_started_but_unfinished_cancel_is_499(self):
        await self.run_close([event('BOUNDARYpartial')],(499,False),guard=True)

    async def test_body_started_but_unfinished_aclose_is_499(self):
        await self.run_close([event('BOUNDARYpartial')],(499,False),guard=True,close=True)

    async def test_no_guard_partial_aclose_is_499(self):
        await self.run_close([event('partial')],(499,False),close=True)

    async def test_done_then_cancel_is_success(self):
        await self.run_close([event(),'data: [DONE]\n\n'],(200,True))

    async def test_boundary_failure_does_not_become_success_on_close(self):
        await self.run_close([event('missing boundary','STOP')],(502,False),guard=True,close=True)

    async def test_asyncio_cancel_during_success_log_preserves_outcome(self):
        entered=asyncio.Event();calls=0
        async def detail(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==1:entered.set();await asyncio.Event().wait()
            return ''
        async def upstream(**kwargs):yield event('OK')
        with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,None)),patch.object(api,'_record_request_body_detail',detail):
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
        async def upstream(**kwargs):yield event()
        async def consume():
            async for _ in api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}}):pass
        with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,None)),patch.object(api,'_record_request_body_detail',detail):
            async with anyio.create_task_group() as group:
                group.start_soon(consume);await entered.wait();group.cancel_scope.cancel();release.set()
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200)

    async def asgi_disconnect(self, finished):
        from starlette.responses import StreamingResponse
        disconnected=asyncio.Event();sent=[]
        async def upstream(**kwargs):
            yield event('BOUNDARYOK', 'STOP' if finished else None)
            await asyncio.Event().wait()
        async def receive():
            await disconnected.wait();return {'type':'http.disconnect'}
        async def send(message):
            if message['type']=='http.response.body' and message.get('body'):
                sent.append(message['body']);disconnected.set()
        with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')):
            response=StreamingResponse(api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}}))
            await asyncio.wait_for(response({'type':'http','asgi':{'spec_version':'2.3'}},receive,send),5)
        self.assertEqual(len(sent),1)
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200 if finished else 499)

    async def test_starlette_disconnect_after_stop_is_success(self):
        await self.asgi_disconnect(True)

    async def test_starlette_disconnect_mid_answer_is_499(self):
        await self.asgi_disconnect(False)
