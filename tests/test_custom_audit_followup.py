import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from contextlib import ExitStack
import httpx
from fastapi import FastAPI, Response
import test_custom_audit
from test_stream_completion import event
from src.api import antigravity as api
from src.router.antigravity import gemini, openai, anthropic
from src.router.stream_passthrough import build_streaming_response_or_error

class ContinuationAccountingTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_completed_segment_then_continuation_error_preserves_order_and_status(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic): app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        cases = [('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
                 ('/v1/chat/completions', {'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
                 ('/v1/messages', {'model':'gemini-4-argon-medium','stream':True,'max_tokens':4096,'messages':[{'role':'user','content':'test'}]})]
        for url,body in cases:
            with self.subTest(url=url):
                self.saved.reset_mock();calls=0
                async def upstream(**kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        yield event('BOUNDARYfirst segment','MAX_TOKENS')
                    else:
                        yield Response(status_code=400,content=json.dumps({'error':{'code':400,'status':'INVALID_ARGUMENT','message':'continuation rejected'}}))
                with ExitStack() as stack:
                    for module in (gemini,openai,anthropic):
                        stack.enter_context(patch.object(module,'is_anti_truncation_model',return_value=True))
                        stack.enter_context(patch.object(module,'get_anti_truncation_max_attempts',AsyncMock(return_value=2)))
                    stack.enter_context(patch.object(api,'stream_post_async',upstream))
                    stack.enter_context(patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')))
                    stack.enter_context(patch.object(api,'record_api_call_error',AsyncMock()))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                        response=await client.post(url,json=body)
                self.assertEqual(calls,2)
                self.assertIn('first segment',response.text)
                self.assertIn('continuation rejected',response.text)
                rows=[call.args[0] for call in self.saved.await_args_list]
                self.assertEqual([r['status_code'] for r in rows],[200,400])
                self.assertEqual(len({r['request_id'] for r in rows}),1)

    async def test_next_segment_send_failure_does_not_inherit_previous_completion(self):
        for version in ('2.3', '2.4'):
            for fail_second_send in (False, True):
                with self.subTest(version=version, fail_second_send=fail_second_send):
                    self.saved.reset_mock()
                    calls = 0

                    async def upstream(**kwargs):
                        nonlocal calls
                        calls += 1
                        yield event('BOUNDARY' + ('first' if calls == 1 else 'second'), 'STOP')

                    async def segments():
                        for _ in range(2):
                            async for chunk in api.stream_request({
                                'model': 'gemini-4-argon-medium', 'request': {'contents': []},
                            }):
                                yield chunk

                    async def send(message):
                        if b'second' in message.get('body', b''):
                            self.assertEqual(self.saved.await_count, 1)
                            self.assertEqual(self.saved.call_args.args[0]['status_code'], 200)
                            if fail_second_send:
                                raise BrokenPipeError('synthetic second segment send failure')

                    async def receive():
                        await asyncio.Event().wait()

                    with patch.object(api, 'stream_post_async', upstream), patch.object(
                        api, 'prepare_thought_boundary', lambda m, r: (r, 'BOUNDARY'),
                    ):
                        response = await build_streaming_response_or_error(segments())
                        run = response({'type': 'http', 'asgi': {'spec_version': version}}, receive, send)
                        if fail_second_send:
                            with self.assertRaises(Exception):
                                await asyncio.wait_for(run, 2)
                        else:
                            await asyncio.wait_for(run, 2)
                    rows = [call.args[0] for call in self.saved.await_args_list]
                    self.assertEqual([r['status_code'] for r in rows], [200, 499 if fail_second_send else 200])
                    self.assertEqual(len({r['request_id'] for r in rows}), 1)

class NonStreamFailureUsageTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_upstream_reported_usage_survives_failed_nonstream_responses(self):
        for status in ('blocked','embedded_error','empty'):
            with self.subTest(status=status):
                self.saved.reset_mock()
                value={'responseId':'usage-failure-id','usageMetadata':{'promptTokenCount':123,'candidatesTokenCount':17,'thoughtsTokenCount':4,'totalTokenCount':144,'cachedContentTokenCount':20}}
                if status=='blocked':
                    value['candidates']=[{'content':{'parts':[{'text':'partial answer'}]},'finishReason':'PROHIBITED_CONTENT'}]
                elif status=='embedded_error':
                    value['error']={'code':400,'message':'explicit error'}
                with patch.object(api,'post_async',AsyncMock(return_value=httpx.Response(200,json={'response':value}))),patch.object(api,'record_api_call_error',AsyncMock()):
                    response=await api.non_stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
                self.assertEqual(response.status_code,502 if status=='empty' else 400)
                self.assertEqual(self.saved.await_count,1)
                row=self.saved.call_args.args[0]
                self.assertFalse(row['success'])
                self.assertEqual(row.get('prompt_tokens'),123)
                self.assertEqual(row.get('completion_tokens'),21)
                self.assertEqual(row.get('cached_tokens'),20)
                self.assertEqual(row.get('response_id'),'usage-failure-id')
