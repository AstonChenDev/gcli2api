"""Exercise the restored upstream transport using local sockets only."""
import asyncio
import gzip
import json
import os
import unittest
from contextlib import ExitStack, suppress
from unittest.mock import AsyncMock, patch

import httpx

import config
from src import httpx_client
from src.router.stream_passthrough import ObservedStreamingResponse
from src.stream_delivery import StreamDelivery


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
                with suppress(ConnectionResetError):
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

    async def test_sse_keeps_unicode_text_across_byte_and_line_boundaries(self):
        line = 'data: ' + json.dumps({"text": "甲\u0085乙\u2028丙\u2029丁"}, ensure_ascii=False)
        wire = (line + "\r\n\r\n" + line + "\r\r" + line + "\n\n" + line).encode()

        async def fragmented():
            for byte in wire:
                yield bytes([byte])

        response = httpx.Response(200, content=fragmented())
        try:
            lines = [item async for item in httpx_client._sse_lines(response)]
        finally:
            await response.aclose()
        self.assertEqual(lines, [line, "", line, "", line, "", line])

    async def test_sse_skips_only_one_initial_bom_across_reads(self):
        line = 'data: ' + json.dumps({"text": "甲\ufeff乙"}, ensure_ascii=False)
        wire = (line + "\r\n\r\n" + line + "\n\n").encode("utf-8-sig")

        async def fragmented():
            for byte in wire:
                yield bytes([byte])

        response = httpx.Response(200, content=fragmented())
        try:
            self.assertEqual([item async for item in httpx_client._sse_lines(response)],
                             [line, "", line, ""])
        finally:
            await response.aclose()

    async def test_sse_extension_fields_do_not_displace_data_or_record_boundaries(self):
        lines = ["x-trace-id: abc", ": heartbeat", "data: {", "vendorFlag", 'data: "ok":true}',
                 "", "data", "data: invalid-json", ""]
        wire = ("\r\n".join(lines) + "\r\n").encode()
        for media_type, expected in [
            ("text/event-stream; charset=utf-8", [": heartbeat", "data: {", 'data: "ok":true}', "",
                                                  "data", "data: invalid-json", ""]),
            ("text/plain", lines),
        ]:
            async def send_response(reader, writer):
                headers = f"Content-Type: {media_type}\r\nContent-Length: {len(wire)}\r\n"
                writer.write(b"HTTP/1.1 200 OK\r\nConnection: close\r\n" + headers.encode() + b"\r\n" + wire)
                await writer.drain()

            url = await self.server(send_response)
            self.assertEqual([line async for line in httpx_client.stream_post_async(url, {})], expected)
            self.assertEqual(b"".join([chunk async for chunk in httpx_client.stream_post_async(url, {}, native=True)]), wire)

    def test_delivery_recognizes_terminal_and_error_events_containing_unicode(self):
        for event, terminal, error in [
            ({"candidates": [{"content": {"parts": [{"text": "甲\u2028乙"}]}, "finishReason": "STOP"}]}, True, False),
            ({"error": {"message": "甲\u2029乙"}}, False, True),
        ]:
            with self.subTest(event=event):
                delivery = StreamDelivery()
                record = "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"
                delivery.sent({"body": record.encode(), "more_body": True})
                self.assertEqual(delivery.terminal_sent, terminal)
                self.assertEqual(delivery.error_sent, error)

    async def test_line_based_clients_can_read_outgoing_unicode_sse(self):
        text = "中文\\\u0085乙\u2028丙\u2029丁"
        data = json.dumps({"text": text}, ensure_ascii=False)
        wire = []
        async def records():
            yield ": heartbeat\n\n"
            yield "event: content\ndata: " + data + "\n\n"
            yield "data: " + data + "\n\n"

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            if message.get("body"):
                wire.append(message["body"])

        response = ObservedStreamingResponse(records(), StreamDelivery(), media_type="text/event-stream")
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        self.assertEqual(wire[0], b": heartbeat\n\n")
        lines = b"".join(wire).decode().splitlines()
        self.assertEqual([json.loads(line[5:]) for line in lines if line.startswith("data:")],
                         [{"text": text}, {"text": text}])
        self.assertIn("中文".encode(), wire[1])

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
            # A cancelled connection can end with either EOF or a TCP reset.
            with suppress(ConnectionResetError):
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

    async def test_compressed_stream_rejection_has_decoded_headers(self):
        body = b'{"error":"busy"}'
        packed = gzip.compress(body)
        async def reject(reader, writer):
            writer.write(b"HTTP/1.1 429 Too Many Requests\r\nContent-Encoding: gzip\r\nRetry-After: 8\r\n"
                         + f"Content-Length: {len(packed)}\r\nConnection: close\r\n\r\n".encode() + packed)
            await writer.drain()
        url = await self.server(reject)
        response, = [chunk async for chunk in httpx_client.stream_post_async(url, {})]
        self.assertEqual(response.body, body)
        self.assertNotIn("content-encoding", response.headers)
        self.assertEqual(int(response.headers["content-length"]), len(body))
        self.assertEqual(response.headers["retry-after"], "8")

    async def test_real_upstream_504_is_preserved(self):
        async def reject(reader, writer):
            writer.write(b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 7\r\nConnection: close\r\n\r\ntimeout")
            await writer.drain()
        url = await self.server(reject)
        results = [chunk async for chunk in httpx_client.stream_post_async(url, {})]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status_code, 504)
        self.assertEqual(results[0].body, b"timeout")
