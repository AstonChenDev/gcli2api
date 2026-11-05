"""Offline regressions for handshake deadlines and pre-first-byte disconnects."""
import asyncio
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Request, Response

import test_custom_audit
from src import httpx_client
from src.api import antigravity as api
from src.router.antigravity import openai, gemini, anthropic
from src.router.stream_passthrough import build_streaming_response_or_error


class HandshakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.writers = []
        self.tasks = set()
        self.servers = []
        self.stack = ExitStack()
        self.stack.enter_context(patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value='')))
        self.stack.enter_context(patch.object(httpx_client, 'get_stream_handshake_timeout_enabled', AsyncMock(return_value=True)))
        self.stack.enter_context(patch.object(httpx_client, 'get_stream_handshake_timeout_seconds', AsyncMock(return_value=.15)))

    async def asyncTearDown(self):
        for writer in self.writers:
            writer.close()
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.stack.close()

    async def serve(self, handler):
        async def tracked(reader, writer):
            self.tasks.add(asyncio.current_task())
            self.writers.append(writer)
            try:
                await handler(reader, writer)
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(tracked, '127.0.0.1', 0)
        self.servers.append(server)
        return server.sockets[0].getsockname()[1]

    async def test_production_default_is_40_with_unlimited_model_reads(self):
        with patch.object(httpx_client, 'get_stream_handshake_timeout_seconds', AsyncMock(return_value=40)):
            async with httpx_client.get_streaming_client(trust_env=False) as client:
                self.assertEqual(client.timeout.connect, 40)
                self.assertIsNone(client.timeout.read)

    async def test_http_proxy_connect_handshake_times_out_and_closes_socket(self):
        closed = asyncio.Event()
        async def proxy(reader, writer):
            headers = await reader.readuntil(b'\r\n\r\n')
            self.assertTrue(headers.startswith(b'CONNECT '))
            await reader.read()
            closed.set()
        port = await self.serve(proxy)
        with self.assertRaises(httpx.ConnectTimeout):
            async with httpx_client.get_streaming_client(proxy=f'http://127.0.0.1:{port}', trust_env=False) as client:
                await client.post('https://example.invalid/test')
        await asyncio.wait_for(closed.wait(), 1)

    async def test_disabled_timeout_can_wait_longer_but_cancellation_still_closes(self):
        ready, closed = asyncio.Event(), asyncio.Event()
        async def proxy(reader, writer):
            await reader.readuntil(b'\r\n\r\n')
            ready.set()
            await reader.read()
            closed.set()
        port = await self.serve(proxy)
        with patch.object(httpx_client, 'get_stream_handshake_timeout_enabled', AsyncMock(return_value=False)):
            async with httpx_client.get_streaming_client(proxy=f'http://127.0.0.1:{port}', trust_env=False) as client:
                self.assertIsNone(client.timeout.connect)
                self.assertIsNone(client.handshake_timeout)
                task = asyncio.create_task(client.post('https://example.invalid/test'))
                try:
                    await asyncio.wait_for(ready.wait(), 1)
                    await asyncio.sleep(.35)
                    self.assertFalse(task.done())
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
        await asyncio.wait_for(closed.wait(), 1)

    async def test_socks_handshake_times_out_and_closes_socket(self):
        closed = asyncio.Event()
        async def proxy(reader, writer):
            await reader.read(1024)
            await reader.read()
            closed.set()
        port = await self.serve(proxy)
        with self.assertRaises(httpx.ConnectTimeout):
            async with httpx_client.get_streaming_client(proxy=f'socks5://127.0.0.1:{port}', trust_env=False) as client:
                await client.post('https://example.invalid/test')
        await asyncio.wait_for(closed.wait(), 1)

    async def test_tls_handshake_times_out(self):
        async def stalled_tls(reader, writer):
            await reader.read()
        port = await self.serve(stalled_tls)
        with self.assertRaises(httpx.ConnectTimeout):
            async with httpx_client.get_streaming_client(trust_env=False) as client:
                await client.post(f'https://127.0.0.1:{port}/')

    async def test_client_cancellation_during_socks_handshake_closes_socket(self):
        ready, closed = asyncio.Event(), asyncio.Event()
        async def proxy(reader, writer):
            await reader.read(1024)
            ready.set()
            await reader.read()
            closed.set()
        port = await self.serve(proxy)
        async with httpx_client.get_streaming_client(proxy=f'socks5://127.0.0.1:{port}', trust_env=False) as client:
            task = asyncio.create_task(client.post('https://example.invalid/test'))
            await asyncio.wait_for(ready.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        await asyncio.wait_for(closed.wait(), 1)

    async def test_slow_generation_headers_and_body_are_not_handshake_timeout(self):
        async def origin(reader, writer):
            await reader.readuntil(b'\r\n\r\n')
            await asyncio.sleep(.3)
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nO')
            await writer.drain()
            await asyncio.sleep(.3)
            writer.write(b'K!!')
            await writer.drain()
        port = await self.serve(origin)
        events = []
        async def trace(event, info):
            events.append(event)
        async with httpx_client.get_streaming_client(trust_env=False) as client:
            response = await client.post(f'http://127.0.0.1:{port}/', extensions={'trace': trace})
        self.assertEqual(response.text, 'OK!!')
        self.assertIn('http11.send_request_headers.started', events)
        self.assertIn('http11.receive_response_body.complete', events)

    async def test_proxy_rejection_is_not_timeout(self):
        async def proxy(reader, writer):
            await reader.readuntil(b'\r\n\r\n')
            writer.write(b'HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n')
            await writer.drain()
        port = await self.serve(proxy)
        with self.assertRaises(httpx.ProxyError):
            async with httpx_client.get_streaming_client(proxy=f'http://127.0.0.1:{port}', trust_env=False) as client:
                await client.post('https://example.invalid/test')


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

    async def test_connection_timeout_is_504_not_499(self):
        async def source(**kwargs):
            raise httpx.ConnectTimeout('handshake exceeded 40s')
            yield ''
        with patch.object(api, 'stream_post_async', source):
            response = await build_streaming_response_or_error(api.stream_request(
                {'model':'gemini-4-argon-medium','request':{'contents':[]}}))
        self.assertEqual(response.status_code, 504)
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 504)
        self.assertIn('UPSTREAM_CONNECT_TIMEOUT', response.body.decode())

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
