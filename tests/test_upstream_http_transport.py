"""Exercise the restored upstream transport using local sockets only."""
import asyncio
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx

import config
from src import httpx_client


class UpstreamTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(httpx_client, "_shared_clients", []))
        self.stack.enter_context(patch.object(httpx_client, "_shared_clients_lock", asyncio.Lock()))
        self.stack.enter_context(patch.object(httpx_client, "get_proxy_config", AsyncMock(return_value="")))
        self.stack.enter_context(patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "no_proxy": "127.0.0.1"}))
        self.addAsyncCleanup(httpx_client.close_shared_clients)
        self.tasks = set()

    async def server(self, handler):
        async def serve(reader, writer):
            self.tasks.add(asyncio.current_task())
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                for line in headers.split(b"\r\n"):
                    if line.lower().startswith(b"content-length:"):
                        await reader.readexactly(int(line.split(b":", 1)[1]))
                await handler(reader, writer)
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        async def cleanup():
            server.close()
            await server.wait_closed()
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.addAsyncCleanup(cleanup)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/"

    async def test_legacy_settings_do_not_restore_deadline_and_shutdown_closes_client(self):
        with patch.dict(os.environ, {"STREAM_HANDSHAKE_TIMEOUT_ENABLED": "true", "STREAM_HANDSHAKE_TIMEOUT_SECONDS": "40"}), patch.object(config, "_config_cache", {"stream_handshake_timeout_enabled": True, "stream_handshake_timeout_seconds": 40}):
            async with httpx_client.get_streaming_client() as first:
                self.assertEqual(first.timeout.as_dict(), dict(connect=None, read=None, write=None, pool=None))
                async with httpx_client.get_streaming_client() as second:
                    self.assertIs(first, second)
            self.assertFalse(first.is_closed)
            await httpx_client.close_shared_clients()
            self.assertTrue(first.is_closed)

    async def test_101_streams_queue_then_complete_without_synthetic_timeout(self):
        accepted = asyncio.Queue()
        release = asyncio.Event()
        async def hold(reader, writer):
            accepted.put_nowait(True)
            await release.wait()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            await writer.drain()
        url = await self.server(hold)
        async def request():
            return [chunk async for chunk in httpx_client.stream_post_async(url, {}, native=True)]
        requests = []
        try:
            for _ in range(100):
                requests.append(asyncio.create_task(request()))
                await asyncio.wait_for(accepted.get(), 3)
            requests.append(asyncio.create_task(request()))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(accepted.get(), .2)
            self.assertFalse(requests[-1].done())
            release.set()
            results = await asyncio.wait_for(asyncio.gather(*requests), 10)
            self.assertEqual(results, [[b"OK"]] * 101)
        finally:
            release.set()
            for task in requests:
                task.cancel()
            await asyncio.gather(*requests, return_exceptions=True)

    async def test_explicit_nonstream_timeout_still_applies(self):
        async def hold(reader, writer):
            await reader.read()
        url = await self.server(hold)
        with self.assertRaises(httpx.ReadTimeout):
            await httpx_client.post_async(url, json={}, timeout=.1)

    async def test_cancelled_stream_releases_connection(self):
        started, closed = asyncio.Event(), asyncio.Event()
        async def hold(reader, writer):
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
            await writer.drain()
            started.set()
            self.assertEqual(await reader.read(), b"")
            closed.set()
        url = await self.server(hold)
        async def request():
            return [chunk async for chunk in httpx_client.stream_post_async(url, {}, native=True)]
        task = asyncio.create_task(request())
        try:
            await asyncio.wait_for(started.wait(), 3)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await asyncio.wait_for(closed.wait(), 3)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_real_upstream_504_is_preserved(self):
        async def reject(reader, writer):
            writer.write(b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 7\r\nConnection: close\r\n\r\ntimeout")
            await writer.drain()
        url = await self.server(reject)
        results = [chunk async for chunk in httpx_client.stream_post_async(url, {})]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status_code, 504)
        self.assertEqual(results[0].body, b"timeout")
