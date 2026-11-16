"""Offline regressions for pre-first-byte disconnects and transport errors."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Request, Response

import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import openai, gemini, anthropic
from src.router.stream_passthrough import build_streaming_response_or_error


class PrefetchDisconnectTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_disconnect_before_first_item_closes_all_protocols_and_logs_499_once(self):
        app = FastAPI()
        for module in (openai, gemini, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        cases = [('/v1/chat/completions', {'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
                 ('/v1/messages', {'model':'gemini-4-argon-medium','stream':True,'max_tokens':100,'messages':[{'role':'user','content':'test'}]}),
                 ('/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents':[{'role':'user','parts':[{'text':'test'}]}]})]
        for path, payload in cases:
            for continuation in (False, True):
                with self.subTest(path=path, continuation=continuation):
                    self.saved.reset_mock()
                    started, closed = asyncio.Event(), asyncio.Event()
                    async def source(**kwargs):
                        try:
                            started.set()
                            await asyncio.Event().wait()
                            yield ''
                        finally:
                            closed.set()
                    body = json.dumps(payload).encode()
                    body_sent = False
                    async def receive():
                        nonlocal body_sent
                        if not body_sent:
                            body_sent = True
                            return {'type':'http.request','body':body,'more_body':False}
                        await started.wait()
                        return {'type':'http.disconnect'}
                    sent = []
                    async def send(message):
                        sent.append(message)
                    scope = {'type':'http','asgi':{'version':'3.0','spec_version':'2.4'},'http_version':'1.1',
                             'method':'POST','path':path,'raw_path':path.encode(),'root_path':'','query_string':b'',
                             'headers':[(b'content-type',b'application/json')],'scheme':'http',
                             'server':('test',80),'client':('test',1)}
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'stream_post_async', source))
                        for module in (openai, gemini, anthropic):
                            stack.enter_context(patch.object(module, 'is_anti_truncation_model', return_value=continuation))
                        await asyncio.wait_for(app(scope, receive, send), 2)
                    self.assertTrue(closed.is_set())
                    self.assertEqual(self.saved.await_count, 1)
                    row = self.saved.call_args.args[0]
                    self.assertEqual(row['status_code'], 499)
                    self.assertFalse(row['success'])
                    self.assertIn('CLIENT_DISCONNECTED', row['error_message'])

    async def test_transport_timeout_uses_upstream_500_not_499(self):
        async def source(**kwargs):
            raise httpx.ConnectTimeout('synthetic transport timeout')
            yield ''
        with patch.object(api, 'stream_post_async', source):
            response = await build_streaming_response_or_error(api.stream_request(
                {'model':'gemini-4-argon-medium','request':{'contents':[]}}))
        self.assertEqual(response.status_code, 500)
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 500)
        self.assertIn('ConnectTimeout', response.body.decode())

    async def test_disconnect_while_waiting_for_next_chunk_closes_stream(self):
        for version in ('2.3', '2.4'):
            sent, closed = asyncio.Event(), asyncio.Event()
            async def source():
                try:
                    yield b'data: {"choices":[]}\n\n'
                    await asyncio.Event().wait()
                finally:
                    closed.set()
            async def send(message):
                if message['type'] == 'http.response.body':
                    sent.set()
            async def receive():
                await sent.wait()
                return {'type':'http.disconnect'}
            response = await build_streaming_response_or_error(source())
            await asyncio.wait_for(response({'type':'http','asgi':{'spec_version':version}}, receive, send), 1)
            self.assertTrue(closed.is_set())
            self.assertEqual(response.delivery.disconnect_source, 'http.disconnect')

    async def test_external_cancel_during_prefetch_is_not_mislabeled_client_disconnect(self):
        self.saved.reset_mock()
        started, closed = asyncio.Event(), asyncio.Event()
        async def source(**kwargs):
            try:
                started.set()
                await asyncio.Event().wait()
                yield ''
            finally:
                closed.set()
        async def receive():
            await asyncio.Event().wait()
        with patch.object(api, 'stream_post_async', source):
            task = asyncio.create_task(build_streaming_response_or_error(api.stream_request(
                {'model':'gemini-4-argon-medium','request':{'contents':[]}}), request=Request({'type':'http'}, receive)))
            await asyncio.wait_for(started.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(closed.is_set())
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 500)

    async def test_empty_and_error_prefetch_close_outer_generator(self):
        async def receive():
            await asyncio.Event().wait()
        request = Request({'type':'http'}, receive)
        for empty in (False, True):
            closed = asyncio.Event()
            async def source():
                try:
                    if not empty:
                        yield Response(status_code=403)
                finally:
                    closed.set()
            response = await build_streaming_response_or_error(source(), request=request)
            self.assertEqual(response.status_code, 204 if empty else 403)
            self.assertTrue(closed.is_set())
