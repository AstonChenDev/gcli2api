"""A failed attempt releases its generator before waiting for the next one."""
import asyncio
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from fastapi import Response
import test_custom_audit
from test_thought_boundary import event
from src.api import antigravity as api
from src.stream_delivery import StreamDelivery, current_delivery

BODY = {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}


class RetryStreamCleanupTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_failed_stream_closed_before_credential_switch_with_or_without_delivery(self):
        for observed in (False, True):
            with self.subTest(observed=observed):
                closed = []; calls = 0
                delivery = StreamDelivery() if observed else None
                token = current_delivery.set(delivery)
                async def upstream(**kwargs):
                    nonlocal calls
                    calls += 1; attempt = calls
                    try:
                        if attempt == 1:
                            yield Response(status_code=503, content='{"error":{"code":503}}')
                        else:
                            self.assertIn(1, closed)
                            yield event([{'text': 'answer'}], 'STOP')
                    finally:
                        closed.append(attempt)
                async def switch(**kwargs):
                    self.assertIn(1, closed)
                    if delivery:
                        self.assertEqual(len(delivery.closers), 1)  # credential prefetch owner only
                    return True, None
                try:
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                        stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, None)))
                        stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={
                            'retry_enabled': True, 'max_retries': 1, 'retry_interval': 0})))
                        stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                        stack.enter_context(patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=True)))
                        stack.enter_context(patch.object(api, '_switch_credential_for_retry', switch))
                        out = [c async for c in api.stream_request(BODY)]
                        self.assertEqual(calls, 2)
                        self.assertEqual(closed, [1, 2])
                        if delivery:
                            self.assertEqual(len(delivery.closers), 1)
                            await delivery.finalize()
                finally:
                    current_delivery.reset(token)

    async def test_nonretry_error_and_caller_close_release_upstream_immediately(self):
        closed = asyncio.Event()
        async def upstream(**kwargs):
            try:
                yield Response(status_code=400, content='{"error":{"code":400}}')
            finally:
                closed.set()
        with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'record_api_call_error', AsyncMock()):
            stream = api.stream_request(BODY)
            self.assertEqual((await anext(stream)).status_code, 400)
            await stream.aclose()
            self.assertTrue(closed.is_set())

    async def test_internal_cancellation_closes_active_generator(self):
        entered, closed = asyncio.Event(), asyncio.Event()
        async def upstream(**kwargs):
            try:
                entered.set()
                await asyncio.Event().wait()
                yield ''
            finally:
                closed.set()
        with patch.object(api, 'stream_post_async', upstream):
            stream = api.stream_request(BODY)
            task = asyncio.create_task(anext(stream))
            await entered.wait(); task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(closed.is_set())
            await stream.aclose()
