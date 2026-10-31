import asyncio
import json
import unittest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
import test_custom_audit
from src.api import antigravity as api
from src.stream_events import inspect_line
from src.thought_boundary import ERROR_CODE
from src.router.stream_passthrough import build_streaming_response_or_error
from src.converter.openai2gemini import convert_gemini_to_openai_stream
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream


MARKER = 'BOUNDARY'


def event(parts=None, finish=None, **extra):
    c = {'content': {'role': 'model', 'parts': parts or []}}
    if finish:
        c['finishReason'] = finish
    return 'data: ' + json.dumps({'response': {'responseId': 'synthetic-recovery',
        'candidates': [c], **extra}}) + '\n\n'


def parts_of(chunks):
    parts = []
    for chunk in chunks:
        if isinstance(chunk, Response) or not chunk.startswith('data:') or '[DONE]' in chunk:
            continue
        obj = json.loads(chunk[5:])
        for c in obj.get('response', obj).get('candidates', []):
            parts.extend(c.get('content', {}).get('parts', []))
    return parts


class EventRecoveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def run_attempts(self, attempts, enabled=True, maximum=3, include=True):
        self.calls = 0
        async def upstream(**kwargs):
            index = self.calls
            self.calls += 1
            for item in attempts[min(index, len(attempts) - 1)]:
                if isinstance(item, Exception):
                    raise item
                yield item
        self.record_error = AsyncMock()
        self.record_success = AsyncMock()
        with patch.object(api, 'stream_post_async', upstream), \
             patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)), \
             patch.object(api, 'get_retry_config', AsyncMock(return_value={
                 'retry_enabled': enabled, 'max_retries': maximum, 'retry_interval': 0})), \
             patch.object(api, 'record_api_call_error', self.record_error), \
             patch.object(api, 'record_api_call_success', self.record_success), \
             patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))), \
             patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=enabled)), \
             patch.object(api, 'parse_and_log_cooldown', AsyncMock(return_value=None)):
            chunks = [x async for x in api.stream_request({'model': 'gemini-4-argon-medium',
                'request': {'contents': [], 'generationConfig': {'thinkingConfig': {'includeThoughts': include}}}},
                log_ctx={'request_id': 'synthetic-same-request'})]
        return chunks, [c.args[0] for c in self.saved.call_args_list]

    async def test_empty_heartbeat_metadata_done_retry_then_native_thought_and_answer(self):
        thought = {'text': '原生思考', 'thought': True, 'thoughtSignature': 'synthetic-signature'}
        chunks, rows = await self.run_attempts([
            ['', ': keepalive', 'data: {"candidates":[],"usageMetadata":{"totalTokenCount":3}}', 'data: [DONE]'],
            [event([thought]), event([{'text': MARKER + '正文'}], 'STOP',
                usageMetadata={'promptTokenCount': 10, 'candidatesTokenCount': 2, 'totalTokenCount': 12})]])
        self.assertEqual(self.calls, 2)
        self.assertEqual([r['status_code'] for r in rows], [502, 200])
        self.assertEqual(rows[0]['error_message'], 'UPSTREAM_EMPTY_RESPONSE')
        self.assertEqual({r['request_id'] for r in rows}, {'synthetic-same-request'})
        self.assertEqual(parts_of(chunks), [thought, {'text': '正文'}])
        self.assertEqual(rows[-1]['prompt_tokens'], 10)
        self.assertEqual(rows[-1]['total_tokens'], 12)
        self.assertEqual(self.record_success.await_count, 1)
        self.record_error.assert_not_awaited()
        self.assertFalse(any('[DONE]' in c for c in chunks if isinstance(c, str)))

    async def test_empty_retry_capped_even_if_configuration_allows_many(self):
        chunks, rows = await self.run_attempts([['', 'data: {"candidates":[]}']], maximum=20)
        self.assertEqual(self.calls, 2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_EMPTY_RESPONSE')
        self.record_success.assert_not_awaited()

    async def test_retry_switch_and_zero_limit_are_respected(self):
        for enabled, maximum in [(False, 3), (True, 0)]:
            self.saved.reset_mock()
            _, rows = await self.run_attempts([['']], enabled=enabled, maximum=maximum)
            self.assertEqual(self.calls, 1)
            self.assertEqual(len(rows), 1)

    async def test_explicit_sse_error_preserved_without_boundary_error(self):
        error = {'code': 429, 'status': 'RESOURCE_EXHAUSTED', 'message': 'synthetic quota'}
        chunks, rows = await self.run_attempts([['data: ' + json.dumps({'response': {'error': error}})]], enabled=False)
        self.assertEqual(self.calls, 1)
        self.assertEqual(json.loads(chunks[-1].body)['error'], error)
        self.assertEqual(rows[0]['status_code'], 429)
        self.assertNotIn(ERROR_CODE, rows[0]['error_message'])

    async def test_sse_error_can_recover_before_content(self):
        chunks, rows = await self.run_attempts([
            ['data: {"error":{"code":503,"status":"UNAVAILABLE"}}'],
            [event([{'text': MARKER + '恢复'}], 'STOP')]])
        self.assertEqual(self.calls, 2)
        self.assertEqual([r['status_code'] for r in rows], [503, 200])
        self.assertEqual(parts_of(chunks), [{'text': '恢复'}])

    async def test_blocked_events_are_not_boundary_errors_or_retried(self):
        for chunk in ['data: {"promptFeedback":{"blockReason":"SAFETY"}}',
                      event(finish='RECITATION')]:
            self.saved.reset_mock()
            chunks, rows = await self.run_attempts([[chunk]])
            self.assertEqual(self.calls, 1)
            self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_CONTENT_BLOCKED')
            self.assertEqual(len(rows), 1)
            self.record_error.assert_not_awaited()

    async def test_malformed_event_is_not_passed_to_client_as_text(self):
        chunks, rows = await self.run_attempts([['data: not json']])
        self.assertEqual(self.calls, 1)
        self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_INVALID_EVENT')
        self.assertEqual(parts_of(chunks), [])

    async def test_confirmed_thinking_without_boundary_remains_thought_without_error(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'Thinking Process:\n不确定是思考还是正文'}], 'STOP')]])
        self.assertEqual(self.calls, 1)
        self.assertTrue(all(p.get('thought') is True for p in parts_of(chunks)))
        self.assertTrue(rows[0]['success'])

    async def test_native_thought_followed_by_unmarked_body_succeeds(self):
        chunks, rows = await self.run_attempts([[event([{'text': '思考', 'thought': True}]),
                                               event([{'text': '仍不确定'}], 'STOP')]])
        self.assertEqual(self.calls, 1)
        self.assertEqual(parts_of(chunks), [{'text': '思考', 'thought': True}, {'text': '仍不确定'}])
        self.assertTrue(rows[0]['success'])

    async def test_no_retry_after_thought_or_body_on_transport_or_upstream_error(self):
        for part in [{'text': '思考', 'thought': True}, {'text': MARKER + '正文'}]:
            for failure in [RuntimeError('synthetic disconnect'),
                            'data: {"error":{"code":503,"status":"UNAVAILABLE"}}',
                            Response(status_code=503, content='synthetic HTTP error')]:
                self.saved.reset_mock()
                chunks, rows = await self.run_attempts([[event([part]), failure]])
                self.assertEqual(self.calls, 1)
                self.assertEqual(len(rows), 1)
                self.assertFalse(rows[0]['success'])
                self.assertEqual(len(parts_of(chunks)), 1)

    async def test_transport_error_before_content_retries(self):
        chunks, rows = await self.run_attempts([[RuntimeError('synthetic disconnect')],
                                               [event([{'text': MARKER + 'OK'}], 'STOP')]])
        self.assertEqual(self.calls, 2)
        self.assertEqual([r['status_code'] for r in rows], [500, 200])
        self.assertEqual(parts_of(chunks), [{'text': 'OK'}])

    async def test_optout_still_suppresses_native_and_reclassified_thought(self):
        chunks, rows = await self.run_attempts([[event([{'text': 'native', 'thought': True},
            {'text': 'reclassified' + MARKER + 'answer'}], 'STOP')]], include=False)
        self.assertEqual(parts_of(chunks), [{'text': 'answer'}])
        self.assertTrue(rows[0]['success'])

    async def test_both_converters_keep_recovered_thought_out_of_body(self):
        chunks, _ = await self.run_attempts([[''], [event([
            {'text': '思考', 'thought': True, 'thoughtSignature': 'synthetic'},
            {'text': MARKER + '答案'}], 'STOP')]])
        converted = convert_gemini_to_openai_stream(chunks[0], 'gemini-4-argon-medium', 'synthetic')
        delta = json.loads(converted[6:])['choices'][0]['delta']
        self.assertEqual(delta['reasoning_content'], '思考')
        self.assertEqual(delta['content'], '答案')
        async def source():
            for c in chunks:
                yield c.encode()
        output = [x async for x in gemini_stream_to_anthropic_stream(source(), 'gemini-4-argon-medium')]
        deltas = [json.loads(line[6:]).get('delta') for x in output for line in x.decode().splitlines()
                  if line.startswith('data: ')]
        self.assertIn({'type': 'thinking_delta', 'thinking': '思考'}, deltas)
        self.assertIn({'type': 'text_delta', 'text': '答案'}, deltas)

    async def test_real_httpx_line_iteration_and_response_prefetch(self):
        # Exercise the actual HTTP decoder and first-event prefetch, offline.
        from src import httpx_client
        payload = '\n' + event([{'text': MARKER + 'OK'}], 'STOP').replace('data: ', 'data:')
        @asynccontextmanager
        async def client(**kwargs):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, text=payload))) as session:
                yield session
        with patch.object(httpx_client, 'get_streaming_client', client), \
             patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)):
            response = await build_streaming_response_or_error(api.stream_request(
                {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}))
            chunks = [x async for x in response.body_iterator]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(parts_of(chunks), [{'text': 'OK'}])
        self.assertEqual(self.saved.call_args.args[0]['response_id'], 'synthetic-recovery')

    async def test_http_routes_recover_and_preserve_thought_channels(self):
        from src.router.antigravity import gemini, openai, anthropic
        app = FastAPI()
        for router in (gemini, openai, anthropic):
            app.include_router(router.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'synthetic'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'synthetic'
        model = 'gemini-4-argon-medium'
        requests = [
            ('/v1beta/models/' + model + ':streamGenerateContent',
             {'contents': [{'role': 'user', 'parts': [{'text': 'synthetic request'}]}]}, 'gemini'),
            ('/v1/chat/completions', {'model': model, 'stream': True,
             'messages': [{'role': 'user', 'content': 'synthetic request'}]}, 'openai'),
            ('/v1/messages', {'model': model, 'stream': True, 'max_tokens': 100,
             'messages': [{'role': 'user', 'content': 'synthetic request'}]}, 'anthropic'),
        ]
        for url, body, format_name in requests:
            calls = 0
            async def upstream(**kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    yield ''
                    return
                yield event([{'text': 'synthetic-thought', 'thought': True, 'thoughtSignature': 'synthetic'},
                             {'text': MARKER + 'synthetic-answer'}], 'STOP')
            self.saved.reset_mock()
            with patch.object(api, 'stream_post_async', upstream), \
                 patch.object(api, 'prepare_thought_boundary', lambda m, r: (r, MARKER)), \
                 patch.object(api, 'get_retry_config', AsyncMock(return_value={
                     'retry_enabled': True, 'max_retries': 2, 'retry_interval': 0})), \
                 patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                    response = await client.post(url, json=body)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(calls, 2)
            self.assertNotIn(MARKER, response.text)
            self.assertNotIn(ERROR_CODE, response.text)
            events = [json.loads(line[5:]) for line in response.text.splitlines()
                      if line.startswith('data:') and line[5:].strip() != '[DONE]']
            if format_name == 'gemini':
                parts = [p for e in events for c in e.get('candidates', []) for p in c.get('content', {}).get('parts', [])]
                self.assertEqual(''.join(p.get('text', '') for p in parts if p.get('thought')), 'synthetic-thought')
                self.assertEqual(''.join(p.get('text', '') for p in parts if not p.get('thought')), 'synthetic-answer')
            elif format_name == 'openai':
                deltas = [c.get('delta', {}) for e in events for c in e.get('choices', [])]
                self.assertEqual(''.join(d.get('reasoning_content', '') for d in deltas), 'synthetic-thought')
                self.assertEqual(''.join(d.get('content', '') for d in deltas), 'synthetic-answer')
            else:
                deltas = [e.get('delta', {}) for e in events]
                self.assertEqual(''.join(d.get('thinking', '') for d in deltas), 'synthetic-thought')
                self.assertEqual(''.join(d.get('text', '') for d in deltas), 'synthetic-answer')

    async def test_initial_empty_lines_do_not_hide_http_error_status(self):
        from src.router.antigravity import gemini
        app = FastAPI()
        app.include_router(gemini.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'synthetic'
        async def upstream(**kwargs):
            yield ''
            yield 'data: {"error":{"code":400,"status":"INVALID_ARGUMENT","message":"synthetic"}}'
        with patch.object(api, 'stream_post_async', upstream), \
             patch.object(api, 'record_api_call_error', AsyncMock()):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                response = await client.post('/v1beta/models/gemini-4-argon-medium:streamGenerateContent',
                    json={'contents': [{'role': 'user', 'parts': [{'text': 'synthetic request'}]}]})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['error']['status'], 'INVALID_ARGUMENT')

    async def test_cancel_while_waiting_after_heartbeat_is_still_499(self):
        entered = asyncio.Event()
        async def upstream(**kwargs):
            yield ': keepalive'
            entered.set()
            await asyncio.Event().wait()
        with patch.object(api, 'stream_post_async', upstream):
            stream = api.stream_request({'model': 'gemini-4-argon-medium', 'request': {'contents': []}})
            self.assertTrue((await anext(stream)).startswith(':'))
            task = asyncio.create_task(anext(stream))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            await stream.aclose()
        self.assertEqual(self.saved.await_count, 1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 499)


class LineInspectionTests(unittest.TestCase):
    def test_noncontent_events_and_sse_spacing(self):
        for line in ['', ' ', 'event: message', 'id: 1', 'retry: 1000']:
            self.assertEqual(inspect_line(line), ('empty', None))
        self.assertEqual(inspect_line(': keepalive'), ('heartbeat', ': keepalive\n\n'))
        self.assertEqual(inspect_line('data:{"candidates":[]}')[0], 'metadata')
        self.assertEqual(inspect_line('data: [DONE]')[0], 'done')

    def test_invalid_payload_shapes_are_rejected(self):
        for payload in ['null', '[]', '{"candidates":1}', '{"candidates":[1]}',
                        '{"candidates":[{"content":{"parts":[1]}}]}']:
            kind, response = inspect_line('data: ' + payload)
            self.assertEqual(kind, 'invalid')
            self.assertEqual(response.status_code, 500)
