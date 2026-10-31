import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from src.api import antigravity as api
from src.stream_events import inspect_line, PROMPT_REJECTION_NOTICE
from src.router.stream_passthrough import build_streaming_response_or_error
import test_custom_audit
from test_stream_completion import event


def notice(text=PROMPT_REJECTION_NOTICE):
    return {'response': {'candidates': [{'content': {'parts': [{'text': text}]}}]}}


class RejectionShapeTests(unittest.TestCase):
    def test_exact_observed_wire_rejection_is_explicit_error(self):
        kind, error = inspect_line('data: ' + json.dumps(notice()), allow_text_notice=True)
        self.assertEqual(kind, 'blocked')
        self.assertEqual(error.status_code, 400)
        payload = json.loads(error.body)['error']
        self.assertEqual(payload['message'], PROMPT_REJECTION_NOTICE)
        self.assertEqual(payload['reasonSource'], 'upstream_text_notice')
        self.assertNotIn('blockReason', payload)  # not an upstream enum
        self.assertFalse(hasattr(error, 'upstream_content'))

    def test_prose_native_thought_and_model_metadata_are_not_rejections(self):
        variants = [notice('The policy says: ' + PROMPT_REJECTION_NOTICE), notice()]
        variants[1]['response']['responseId'] = 'real-model-response'
        thought = notice(); thought['response']['candidates'][0]['content']['parts'][0]['thought'] = True
        variants.append(thought)
        finished = notice(); finished['response']['candidates'][0]['finishReason'] = 'STOP'
        variants.append(finished)
        for value in variants:
            original = copy.deepcopy(value)
            kind, _ = inspect_line('data: '+json.dumps(value), allow_text_notice=True)
            self.assertEqual(kind, 'content')
            self.assertEqual(value, original)
        self.assertEqual(inspect_line('data: '+json.dumps(notice()))[0], 'content')


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def respond(self, chunks, send, receive=None, version='2.4'):
        async def upstream(**kwargs):
            for chunk in chunks:
                yield chunk
            if not any('STOP' in c for c in chunks):
                await asyncio.Event().wait()
        async def waiting():
            await asyncio.Event().wait()
        with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'prepare_thought_boundary', lambda m,r:(r,'BOUNDARY')):
            response = await build_streaming_response_or_error(api.stream_request(
                {'model':'gemini-4-argon-medium','request':{'contents':[]}}))
            await asyncio.wait_for(response({'type':'http','asgi':{'spec_version':version}}, receive or waiting, send), 2)
            return response

    async def test_stop_before_send_failure_is_not_success(self):
        for version in ('2.3', '2.4'):
            self.saved.reset_mock()
            async def send(message):
                if message['type']=='http.response.body':
                    raise BrokenPipeError('synthetic failed send')
            with self.assertRaises(Exception):
                await self.respond([event('BOUNDARYanswer','STOP')],send,version=version)
            self.assertEqual(self.saved.await_count,1)
            self.assertEqual(self.saved.call_args.args[0]['status_code'],499)

    async def test_success_waits_for_real_send_and_request_id_matches(self):
        headers = []
        async def send(message):
            self.assertEqual(self.saved.await_count,0)
            if message['type']=='http.response.start':headers.extend(message['headers'])
        await self.respond([event('BOUNDARYanswer','STOP')],send)
        self.assertEqual(self.saved.await_count,1)
        row=self.saved.call_args.args[0]
        self.assertTrue(row['success'])
        self.assertEqual(dict(headers)[b'x-request-id'].decode(),row['request_id'])

    async def test_observed_disconnect_after_successful_stop_send_is_success(self):
        disconnected=asyncio.Event()
        async def send(message):
            if message['type']=='http.response.body' and message.get('body'):disconnected.set()
        async def receive():
            await disconnected.wait();return {'type':'http.disconnect'}
        await self.respond([event('BOUNDARYanswer','STOP')],send,receive,version='2.3')
        self.assertEqual(self.saved.call_args.args[0]['status_code'],200)

    async def test_internal_task_cancel_is_not_claimed_as_client_disconnect(self):
        sent=asyncio.Event()
        async def send(message):
            if message['type']=='http.response.body':sent.set()
        task=asyncio.create_task(self.respond([event('BOUNDARYpartial')],send))
        await sent.wait();task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],500)
        self.assertIn('STREAM_INTERRUPTED_UNKNOWN',self.saved.call_args.args[0]['error_message'])

    async def test_rejection_is_http400_before_body_and_not_retried(self):
        async def upstream(**kwargs):yield 'data: '+json.dumps(notice())
        with patch.object(api,'stream_post_async',upstream):
            response=await build_streaming_response_or_error(api.stream_request(
                {'model':'gemini-4-argon-medium','request':{'contents':[]}}))
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.saved.await_count,1)
        self.assertIn(PROMPT_REJECTION_NOTICE,json.loads(response.body)['error']['message'])
        api.record_api_call_success.assert_not_awaited()

class OutputLimitTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_explicit_limits_and_thought_preferences_survive(self):
        from src.converter.antigravity_fix import normalize_antigravity_request
        for limit in (1024,4096,8192,32768,64000):
            result=await normalize_antigravity_request({'model':'gemini-4-argon-medium',
                'contents':[],'generationConfig':{'maxOutputTokens':limit,'thinkingConfig':{'includeThoughts':True}}})
            self.assertEqual(result['generationConfig']['maxOutputTokens'],limit)
            self.assertTrue(result['generationConfig']['thinkingConfig']['includeThoughts'])

    async def test_default_and_invalid_limits(self):
        from src.converter.antigravity_fix import normalize_antigravity_request
        from fastapi import HTTPException
        result=await normalize_antigravity_request({'model':'gemini-4-argon-medium','contents':[]})
        self.assertEqual(result['generationConfig']['maxOutputTokens'],64000)
        capped=await normalize_antigravity_request({'model':'gemini-4-argon-medium','contents':[],
            'generationConfig':{'maxOutputTokens':65536}})
        self.assertEqual(capped['generationConfig']['maxOutputTokens'],64000)
        for limit in (True,0,-1,1.5,'4096'):
            with self.assertRaises(HTTPException):
                await normalize_antigravity_request({'model':'gemini-4-argon-medium','contents':[],
                    'generationConfig':{'maxOutputTokens':limit}})

class RejectionRoutesTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_all_three_protocols_and_anti_return_notice_as_http400(self):
        import httpx
        from fastapi import FastAPI
        from contextlib import ExitStack
        from unittest.mock import AsyncMock
        from src.router.antigravity import gemini,openai,anthropic
        app=FastAPI()
        for module in (gemini,openai,anthropic):app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible]=lambda:'test'
        app.dependency_overrides[openai.authenticate_bearer]=lambda:'test'
        cases=[('/v1beta/models/gemini-4-argon-medium:streamGenerateContent',{'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
            ('/v1/chat/completions',{'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
            ('/v1/messages',{'model':'gemini-4-argon-medium','stream':True,'max_tokens':4096,'messages':[{'role':'user','content':'test'}]})]
        for anti in (False,True):
            for url,body in cases:
                self.saved.reset_mock()
                async def upstream(**kw):yield 'data: '+json.dumps(notice())
                with ExitStack() as stack:
                    stack.enter_context(patch.object(api,'stream_post_async',upstream))
                    record_error=stack.enter_context(patch.object(api,'record_api_call_error',AsyncMock()))
                    for module in (gemini,openai,anthropic):
                        stack.enter_context(patch.object(module,'is_anti_truncation_model',return_value=anti))
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
                        response=await c.post(url,json=body)
                    record_error.assert_not_awaited()
                self.assertEqual(response.status_code,400,(url,anti,response.text))
                self.assertEqual(response.json()['error']['message'],PROMPT_REJECTION_NOTICE)
                self.assertEqual(self.saved.await_count,1)
                self.assertEqual(response.headers['x-request-id'],self.saved.call_args.args[0]['request_id'])

class ExplicitErrorCodes(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_explicit_codes_survive_all_stream_protocols_before_and_after_text(self):
        import httpx
        from fastapi import FastAPI
        from unittest.mock import AsyncMock
        from src.router.antigravity import gemini,openai,anthropic
        app=FastAPI()
        for module in (gemini,openai,anthropic):app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible]=lambda:'test'
        app.dependency_overrides[openai.authenticate_bearer]=lambda:'test'
        cases=[('/v1beta/models/gemini-4-argon-medium:streamGenerateContent',{'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
            ('/v1/chat/completions',{'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
            ('/v1/messages',{'model':'gemini-4-argon-medium','stream':True,'max_tokens':4096,'messages':[{'role':'user','content':'test'}]})]
        for code in (400,401,403,404,429,500,502,503):
            for partial in (False,True):
                for url,body in cases:
                    with self.subTest(code=code,partial=partial,url=url):
                        self.saved.reset_mock()
                        async def upstream(**kw):
                            if partial:yield event('BOUNDARYpartial answer')
                            yield 'data: '+json.dumps({'error':{'code':code,'status':'SYNTHETIC_UPSTREAM_ERROR','message':'original error'}})
                        with patch.object(api,'stream_post_async',upstream),patch.object(api,'record_api_call_error',AsyncMock()),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')):
                            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
                                response=await c.post(url,json=body)
                        self.assertEqual(response.status_code,200 if partial else code)
                        self.assertEqual(self.saved.await_count,1)
                        self.assertEqual(self.saved.call_args.args[0]['status_code'],code)
                        self.assertFalse(self.saved.call_args.args[0]['success'])
                        self.assertIn('original error',response.text)
                        if partial:self.assertIn('partial answer',response.text)
                        self.assertNotIn('end_turn',response.text)

class CompletedTransportTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_transport_close_error_after_complete_answer_does_not_relabel_success(self):
        import httpx
        from fastapi import FastAPI
        from src.router.antigravity import gemini
        app=FastAPI();app.include_router(gemini.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible]=lambda:'test'
        for finish,expected in ((None,500),('STOP',200)):
            with self.subTest(finish=finish):
                self.saved.reset_mock()
                async def upstream(**kwargs):
                    yield event('BOUNDARYanswer',finish)
                    raise httpx.RemoteProtocolError('synthetic transport close')
                with patch.object(api,'stream_post_async',upstream),patch.object(api,'prepare_thought_boundary',lambda m,r:(r,'BOUNDARY')):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                        response=await client.post('/v1beta/models/gemini-4-argon-medium:streamGenerateContent',json={'contents':[{'role':'user','parts':[{'text':'test'}]}]})
                self.assertEqual(self.saved.await_count,1)
                self.assertEqual(self.saved.call_args.args[0]['status_code'],expected)
                self.assertIn('answer',response.text)
                self.assertEqual('"error"' in response.text,expected==500)
