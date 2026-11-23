"""Regressions from the isolated November follow-up audit."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
import httpx
from fastapi import FastAPI, Request, Response

import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import openai
from src.router.stream_passthrough import build_streaming_response_or_error
from src.converter import openai2gemini

BODY = {'model':'gemini-4-argon-medium','request':{'contents':[]}}
CRED = ('synthetic@example.invalid', {'access_token':'synthetic','project_id':'synthetic'})

def chunk(text='answer', finish='STOP', usage=None):
    candidate = {'index':0,'content':{'parts':[{'text':text}]}}
    if finish:
        candidate['finishReason'] = finish
    response = {'candidates':[candidate]}
    if usage is not None:
        response['usageMetadata'] = usage
    return 'data: ' + json.dumps({'response':response}) + '\n\n'


class FollowupRepairTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def request(self, upstream, anti=False):
        app = FastAPI()
        app.include_router(openai.router)
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        with patch.object(api,'stream_post_async',upstream), patch.object(openai,'is_anti_truncation_model',return_value=anti), patch.object(openai,'get_anti_truncation_max_attempts',AsyncMock(return_value=2)):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                return await client.post('/v1/chat/completions',json={
                    'model':'gemini-4-argon-medium','stream':True,
                    'messages':[{'role':'user','content':'test'}]})

    async def test_invalid_token_metadata_preserves_answer_and_success(self):
        for anti in (False, True):
            for bad_usage in ({'promptTokenCount':'not-a-number'}, {'totalTokenCount':[]}, ['invalid-shape']):
                with self.subTest(anti=anti, usage=bad_usage):
                    self.saved.reset_mock()
                    async def upstream(**kwargs):
                        yield chunk('EXPECTED ANSWER', usage=bad_usage)
                    response = await self.request(upstream, anti)
                    self.assertEqual(response.status_code, 200)
                    self.assertIn('EXPECTED ANSWER', response.text)
                    self.assertIn('[DONE]', response.text)
                    self.assertEqual(self.saved.await_count, 1)
                    self.assertTrue(self.saved.call_args.args[0]['success'])

    async def test_unrecoverable_conversion_failure_is_explicit_and_never_success(self):
        original = openai2gemini.convert_gemini_to_openai_stream
        for anti in (False, True):
            for partial in (False, True):
                with self.subTest(anti=anti, partial=partial):
                    self.saved.reset_mock()
                    async def upstream(**kwargs):
                        if partial:
                            yield chunk('partial text', finish=None)
                        yield chunk('TRIGGER_FAILURE')
                    def convert(event, *args, **kwargs):
                        if 'TRIGGER_FAILURE' in event:
                            raise ValueError('synthetic adapter failure')
                        return original(event, *args, **kwargs)
                    with patch.object(openai2gemini, 'convert_gemini_to_openai_stream', convert):
                        response = await self.request(upstream, anti)
                    self.assertEqual(response.status_code, 200 if partial else 502)
                    self.assertIn('RESPONSE_CONVERSION_FAILED', response.text)
                    self.assertNotIn('[DONE]', response.text)
                    if partial:
                        self.assertIn('partial text', response.text)
                    self.assertEqual(self.saved.await_count, 1)
                    self.assertEqual(self.saved.call_args.args[0]['status_code'], 502)
                    self.assertFalse(self.saved.call_args.args[0]['success'])

    async def test_nonstream_invalid_usage_also_preserves_answer(self):
        app = FastAPI()
        app.include_router(openai.router)
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        payload = json.loads(chunk('NONSTREAM ANSWER', usage={'promptTokenCount':'bad'})[5:])
        with patch.object(api,'post_async',AsyncMock(return_value=httpx.Response(200,json=payload))):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                response = await client.post('/v1/chat/completions',json={
                    'model':'gemini-4-argon-medium','stream':False,
                    'messages':[{'role':'user','content':'test'}]})
        self.assertEqual(response.status_code,200)
        self.assertIn('NONSTREAM ANSWER',response.text)
        self.assertEqual(self.saved.await_count,1)
        self.assertTrue(self.saved.call_args.args[0]['success'])

    async def test_disconnect_during_initial_credential_lookup_logs_once(self):
        started, closed = asyncio.Event(), asyncio.Event()
        async def credential(**kwargs):
            try:
                started.set()
                await asyncio.Event().wait()
            finally:
                closed.set()
        async def receive():
            await started.wait()
            return {'type':'http.disconnect'}
        with patch.object(api,'get_valid_credential',credential):
            response = await asyncio.wait_for(build_streaming_response_or_error(
                api.stream_request(BODY), request=Request({'type':'http'},receive)), 1)
        self.assertEqual(response.status_code, 499)
        self.assertTrue(closed.is_set())
        self.assertEqual(self.saved.await_count, 1)
        row = self.saved.call_args.args[0]
        self.assertEqual(row['status_code'], 499)
        self.assertEqual(row['request_id'], response.headers['x-request-id'])
        self.assertEqual(row['credential_email'], '')
        self.assertIn('BEFORE_UPSTREAM', row['error_message'])

    async def test_internal_cancel_before_credential_is_not_client_disconnect(self):
        started = asyncio.Event()
        async def credential(**kwargs):
            started.set()
            await asyncio.Event().wait()
        async def receive():
            await asyncio.Event().wait()
        with patch.object(api,'get_valid_credential',credential):
            task = asyncio.create_task(build_streaming_response_or_error(
                api.stream_request(BODY), request=Request({'type':'http'},receive)))
            await asyncio.wait_for(started.wait(),1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],500)

    async def test_missing_credential_and_missing_token_are_not_double_logged(self):
        for cred in (None, ('synthetic@example.invalid', {})):
            self.saved.reset_mock()
            with patch.object(api,'get_valid_credential',AsyncMock(return_value=cred)):
                response = await build_streaming_response_or_error(api.stream_request(BODY))
            self.assertEqual(response.status_code,500)
            self.assertEqual(self.saved.await_count,1)

    async def test_prefetch_is_joined_on_stream_disconnect_and_nonstream_cancellation(self):
        for streaming in (True, False):
            with self.subTest(streaming=streaming):
                ready, closed = asyncio.Event(), asyncio.Event()
                prefetch = []
                count = 0
                async def credential(**kwargs):
                    nonlocal count
                    count += 1
                    if count == 1:
                        return CRED
                    prefetch.append(asyncio.current_task())
                    try:
                        ready.set()
                        await asyncio.Event().wait()
                    finally:
                        closed.set()
                async def bookkeeping(*args, **kwargs):
                    await ready.wait()
                    await asyncio.Event().wait()
                async def source(**kwargs):
                    yield Response(status_code=429,content='{"error":{"message":"retry"}}')
                async def receive():
                    await ready.wait()
                    return {'type':'http.disconnect'}
                with ExitStack() as stack:
                    stack.enter_context(patch.object(api,'get_valid_credential',credential))
                    stack.enter_context(patch.object(api,'record_api_call_error',bookkeeping))
                    stack.enter_context(patch.object(api,'get_retry_config',AsyncMock(return_value={'max_retries':1,'retry_interval':0,'retry_enabled':True})))
                    try:
                        if streaming:
                            stack.enter_context(patch.object(api,'stream_post_async',source))
                            response = await asyncio.wait_for(build_streaming_response_or_error(
                                api.stream_request(BODY),request=Request({'type':'http'},receive)),1)
                            self.assertEqual(response.status_code,499)
                        else:
                            stack.enter_context(patch.object(api,'post_async',AsyncMock(return_value=httpx.Response(429,json={'error':{'message':'retry'}}))))
                            task = asyncio.create_task(api.non_stream_request(BODY))
                            await asyncio.wait_for(ready.wait(),1)
                            task.cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await task
                        self.assertEqual(len(prefetch),1)
                        self.assertTrue(prefetch[0].done())
                        self.assertTrue(closed.is_set())
                    finally:
                        for task in prefetch:
                            task.cancel()
                        await asyncio.gather(*prefetch,return_exceptions=True)
