"""The final response and final usage row must describe the same retry outcome."""
import unittest
from unittest.mock import AsyncMock, patch
from contextlib import ExitStack
import httpx
from fastapi import Response
from src.api import antigravity as api
import test_custom_audit


class RetryTerminalErrorTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_transport_failure_after_retry_does_not_return_stale_http_error(self):
        for streaming in (False, True):
            for first_status in (429, 503):
                with self.subTest(streaming=streaming, first_status=first_status):
                    self.saved.reset_mock()
                    calls = 0
                    async def stream(**kwargs):
                        nonlocal calls
                        calls += 1
                        if calls == 1:
                            yield Response(status_code=first_status, content='{"error":{"message":"first attempt only"}}')
                        else:
                            raise httpx.ConnectError('synthetic final connection failure')
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(api, 'get_retry_config', AsyncMock(return_value={'max_retries':1,'retry_enabled':True,'retry_interval':0})))
                        stack.enter_context(patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=True)))
                        stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                        body={'model':'gemini-4-argon-medium','request':{'contents':[]}}
                        if streaming:
                            stack.enter_context(patch.object(api, 'stream_post_async', stream))
                            results=[item async for item in api.stream_request(body)]
                            self.assertEqual(len(results),1)
                            response=results[0]
                            self.assertEqual(calls,2)
                        else:
                            post=stack.enter_context(patch.object(api, 'post_async', AsyncMock(side_effect=[
                                httpx.Response(first_status, json={'error':{'message':'first attempt only'}}),
                                httpx.ConnectError('synthetic final connection failure'),
                            ])))
                            response=await api.non_stream_request(body)
                            self.assertEqual(post.await_count,2)
                    rows=[call.args[0] for call in self.saved.await_args_list]
                    self.assertEqual([row['status_code'] for row in rows], [first_status,500])
                    self.assertEqual(response.status_code,rows[-1]['status_code'])
                    self.assertNotIn(b'first attempt only',response.body)
