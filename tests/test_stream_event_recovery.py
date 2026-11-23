import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
import test_custom_audit
from src import httpx_client
from src.api import antigravity as api
from src.stream_events import inspect_line
ERROR_CODE = "UPSTREAM_OUTPUT_BOUNDARY_MISSING"
from src.router.stream_passthrough import build_streaming_response_or_error
from src.converter.openai2gemini import convert_gemini_to_openai_stream
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream


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
        with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'get_retry_config', AsyncMock(return_value={
                 'retry_enabled': enabled, 'max_retries': maximum, 'retry_interval': 0})), patch.object(api, 'record_api_call_error', self.record_error), patch.object(api, 'record_api_call_success', self.record_success), patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))), patch.object(api, 'handle_error_with_retry', AsyncMock(return_value=enabled)), patch.object(api, 'parse_and_log_cooldown', AsyncMock(return_value=None)):
            chunks = [x async for x in api.stream_request({'model': 'gemini-4-argon-medium',
                'request': {'contents': [], 'generationConfig': {'thinkingConfig': {'includeThoughts': include}}}},
                log_ctx={'request_id': 'synthetic-same-request'})]
        return chunks, [c.args[0] for c in self.saved.call_args_list]

    async def test_empty_heartbeat_metadata_done_retry_then_native_thought_and_answer(self):
        thought = {'text': '原生思考', 'thought': True, 'thoughtSignature': 'synthetic-signature'}
        chunks, rows = await self.run_attempts([
            ['', ': keepalive', 'data: {"candidates":[],"usageMetadata":{"totalTokenCount":3}}', 'data: [DONE]'],
            [event([thought]), event([{'text': '正文'}], 'STOP',
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

    async def test_empty_done_retries_without_consuming_trailing_content(self):
        first = [event(usageMetadata={'promptTokenCount': 13}), 'data: [DONE]',
                 event([{'text': 'after terminal'}], 'STOP')]
        chunks, rows = await self.run_attempts([first, [event([{'text': 'recovered'}], 'STOP')]])
        self.assertEqual(parts_of(chunks), [{'text': 'recovered'}])
        self.assertEqual([row['status_code'] for row in rows], [502, 200])
        self.assertEqual(rows[0]['prompt_tokens'], 13)

    async def test_failed_attempt_releases_transport_and_slot_before_yielding_error(self):
        from src.credential_cooldown import Lease

        for frame, status in [('data: [DONE]', 502), ('data: {"error":{"code":400,"message":"invalid"}}', 400)]:
            closed = asyncio.Event()

            async def upstream(**kwargs):
                try:
                    yield frame
                finally:
                    closed.set()

            store = AsyncMock()
            lease = Lease(store, 'test@example.invalid', 'synthetic-slot')

            async def admit(candidate, *args):
                return candidate, lease

            with patch.object(api, 'acquire_dispatch', admit), patch.object(api, 'stream_post_async', upstream), \
                 patch.object(api, 'record_api_call_error', AsyncMock()):
                stream = api.stream_request({'model': 'gemini-4-argon-medium', 'request': {'contents': []}})
                try:
                    error = await anext(stream)
                    self.assertEqual(error.status_code, status)
                    self.assertTrue(closed.is_set())
                    store.finish.assert_awaited_once()
                    self.assertFalse(store.finish.await_args.kwargs['success'])
                finally:
                    await stream.aclose()
            store.finish.assert_awaited_once()

    async def test_empty_retry_capped_even_if_configuration_allows_many(self):
        chunks, rows = await self.run_attempts([['', 'data: {"candidates":[]}']], maximum=20)
        self.assertEqual(self.calls, 2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_EMPTY_RESPONSE')
        self.record_success.assert_not_awaited()

    async def test_split_usage_is_preserved_for_clients_and_log(self):
        initial = {'promptTokenCount': 100, 'cachedContentTokenCount': 40, 'totalTokenCount': 100}
        chunks, rows = await self.run_attempts([[
            event(usageMetadata=initial),
            event([{'text': 'first'}]),
            event([{'text': 'last'}], usageMetadata={'candidatesTokenCount': 5}),
            event(finish='STOP', usageMetadata={'thoughtsTokenCount': 2, 'cachedContentTokenCount': 0}),
        ]])
        events = [json.loads(chunk[5:])['response'] for chunk in chunks]
        self.assertEqual(len(events), 3)
        self.assertEqual(events[0]['usageMetadata'], initial)
        self.assertEqual(events[-1]['usageMetadata'], {
            'promptTokenCount': 100, 'cachedContentTokenCount': 0, 'candidatesTokenCount': 5, 'thoughtsTokenCount': 2,
        })
        self.assertEqual(parts_of(chunks), [{'text': 'first'}, {'text': 'last'}])
        self.assertEqual((rows[0]['prompt_tokens'], rows[0]['completion_tokens'],
                          rows[0]['cached_tokens'], rows[0]['total_tokens']), (100, 7, 0, 107))

    async def test_null_counter_does_not_erase_previous_count_in_log_or_response(self):
        chunks, rows = await self.run_attempts([[
            event([{"text": "answer"}], usageMetadata={"promptTokenCount": 14}),
            event(finish="STOP", usageMetadata={"promptTokenCount": None, "candidatesTokenCount": 8}),
        ]])
        usage = json.loads(chunks[-1][5:])["response"]["usageMetadata"]
        self.assertEqual(usage, {"promptTokenCount": 14, "candidatesTokenCount": 8})
        self.assertEqual((rows[0]["prompt_tokens"], rows[0]["total_tokens"]), (14, 22))

    async def test_rejection_log_merges_usage_from_the_entire_attempt(self):
        prefix = event(usageMetadata={'promptTokenCount': 120, 'cachedContentTokenCount': 40,
                                      'totalTokenCount': 120})
        for rejection in ({'error': {'code': 400, 'message': 'fixture error'}},
                          {'promptFeedback': {'blockReason': 'SAFETY'}}):
            for explicit_zero in (False, True):
                with self.subTest(rejection=rejection, explicit_zero=explicit_zero):
                    self.saved.reset_mock()
                    wrapper = {'usageMetadata': {'thoughtsTokenCount': 2},
                               'response': {**rejection, 'usageMetadata': {'candidatesTokenCount': 7}}}
                    if explicit_zero:
                        wrapper['usageMetadata'].update(promptTokenCount=0, cachedContentTokenCount=0)
                    chunks, rows = await self.run_attempts([[prefix, 'data: ' + json.dumps(wrapper)]], enabled=False)
                    self.assertEqual(chunks[-1].status_code, 400)
                    self.assertEqual(len(rows), 1)
                    row = rows[0]
                    self.assertEqual((row['prompt_tokens'], row['cached_tokens'], row['completion_tokens'], row['total_tokens']),
                                     (0, 0, 9, 9) if explicit_zero else (120, 40, 9, 129))

    async def test_http_rejection_keeps_reported_usage_and_original_body(self):
        body = json.dumps({'error': {'code': 400}, 'usageMetadata': {
            'promptTokenCount': 120, 'cachedContentTokenCount': 40, 'candidatesTokenCount': 7, 'thoughtsTokenCount': 2,
        }}).encode('utf-8-sig')
        chunks, rows = await self.run_attempts([[Response(body, status_code=400)]], enabled=False)
        self.assertEqual((chunks[-1].status_code, chunks[-1].body), (400, body))
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]['prompt_tokens'], rows[0]['cached_tokens'], rows[0]['total_tokens']), (120, 40, 129))

    async def test_initial_usage_of_empty_attempt_stays_with_its_log(self):
        chunks, rows = await self.run_attempts([
            [event(usageMetadata={'promptTokenCount': 100})],
            [event([{'text': 'retry'}], 'STOP', usageMetadata={'candidatesTokenCount': 5})],
        ])
        self.assertEqual(parts_of(chunks), [{'text': 'retry'}])
        self.assertEqual(json.loads(chunks[0][5:])['response']['usageMetadata'], {'candidatesTokenCount': 5})
        self.assertEqual([(row['status_code'], row['prompt_tokens']) for row in rows], [(502, 100), (200, 0)])

    async def test_split_usage_accepts_legacy_counter_names_and_numeric_strings(self):
        chunks, rows = await self.run_attempts([[
            event(usageMetadata={'total_input_tokens': '100', 'input_tokens_details': {'cached_tokens': '40'}}),
            event([{'text': 'answer'}], 'STOP', usageMetadata={'candidatesTokenCount': 5}),
        ]])
        usage = json.loads(chunks[0][5:])['response']['usageMetadata']
        self.assertEqual(usage, {'promptTokenCount': 100, 'cachedContentTokenCount': 40, 'candidatesTokenCount': 5})
        self.assertEqual((rows[0]['prompt_tokens'], rows[0]['cached_tokens'], rows[0]['total_tokens']), (100, 40, 105))

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
            [event([{'text': '恢复'}], 'STOP')]])
        self.assertEqual(self.calls, 2)
        self.assertEqual([r['status_code'] for r in rows], [503, 200])
        self.assertEqual(parts_of(chunks), [{'text': '恢复'}])

    async def test_embedded_stream_error_uses_this_attempts_retry_header(self):
        for status, expected in ((429, '17'), (503, '17'), (400, None)):
            async def upstream(**kwargs):
                kwargs['retry_headers']['retry-after'] = '17'
                yield 'data: ' + json.dumps({'error': {'code': status, 'message': 'test'}})

            with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'record_api_call_error', AsyncMock()):
                chunks = [chunk async for chunk in api.stream_request({
                    'model': 'gemini-4-argon-medium', 'request': {'contents': []}})]
            self.assertEqual(len(chunks), 1)
            self.assertEqual(chunks[0].status_code, status)
            self.assertEqual(chunks[0].headers.get('retry-after'), expected)

    async def test_model_404_switches_before_delivery_only(self):
        model = 'gemini-4-argon-medium'
        a = ('first@example.invalid', {'access_token': 'first', 'project_id': 'project-a'})
        b = ('second@example.invalid', {'access_token': 'second', 'project_id': 'project-b'})
        request = {'model': model, 'request': {'contents': []}}

        for partial, has_alternative in [(False, True), (False, False), (True, True)]:
            with self.subTest(partial=partial, has_alternative=has_alternative):
                self.saved.reset_mock()
                calls = []

                async def select(**kwargs):
                    if not kwargs.get('exclude_emails'):
                        return a
                    self.assertEqual(kwargs['exclude_emails'], {a[0]})
                    return b if has_alternative else None

                async def upstream(**kwargs):
                    calls.append(kwargs['credential_email'])
                    if kwargs['credential_email'] == a[0]:
                        if partial:
                            yield event([{'text': 'delivered'}])
                        yield Response(status_code=404, content='model unavailable')
                    else:
                        self.assertEqual(kwargs['body']['project'], 'project-b')
                        yield event([{'text': 'recovered'}], 'STOP')

                with patch.object(api, 'get_valid_credential', AsyncMock(side_effect=select)), \
                     patch.object(api, 'stream_post_async', upstream), \
                     patch.object(api, 'get_retry_config', AsyncMock(return_value={
                         'retry_enabled': True, 'max_retries': 3, 'retry_interval': 0})), \
                     patch.object(api, '_record_model_not_found', AsyncMock()) as marked:
                    chunks = [chunk async for chunk in api.stream_request(request)]

                marked.assert_awaited_once_with(a[0], model, 'first', 'model unavailable', expected_revision=None)
                statuses = [call.args[0]['status_code'] for call in self.saved.call_args_list]
                if not partial and has_alternative:
                    self.assertEqual(calls, [a[0], b[0]])
                    self.assertEqual(statuses, [404, 200])
                    self.assertEqual(parts_of(chunks), [{'text': 'recovered'}])
                else:
                    self.assertEqual(calls, [a[0]])
                    self.assertEqual(statuses, [404])
                    self.assertEqual(chunks[-1].status_code, 404)

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

    async def test_empty_control_fields_preserve_the_first_and_following_data(self):
        fields = ['id', 'event', 'retry']
        chunks, rows = await self.run_attempts([[
            *fields, event([{'text': 'first'}]), *fields, event([{'text': 'last'}], 'STOP')
        ]])
        self.assertEqual(parts_of(chunks), [{'text': 'first'}, {'text': 'last'}])
        self.assertEqual([row['status_code'] for row in rows], [200])
        self.assertEqual(self.calls, 1)

    async def test_multiline_record_preserves_output_usage_and_following_events(self):
        parts = [{'text': '分析', 'thought': True}, {'text': '正文\u2028继续'}]
        usage = {'promptTokenCount': 7, 'candidatesTokenCount': 3, 'totalTokenCount': 10}
        source = json.loads(event(parts, usageMetadata=usage)[5:])
        fields = ['data: ' + line for line in json.dumps(source, indent=2, ensure_ascii=False).split('\n')]
        fields[1:1] = [': keepalive', 'id: 7', 'event: message', 'data']
        chunks, rows = await self.run_attempts([[*fields, '', event([{'text': '结束'}], 'STOP')]])
        self.assertEqual(parts_of(chunks), [*parts, {'text': '结束'}])
        self.assertEqual(self.calls, 1)
        self.assertEqual(rows[0]['status_code'], 200)
        self.assertEqual(rows[0]['total_tokens'], 10)
        self.assertEqual(rows[0]['response_id'], 'synthetic-recovery')

    async def test_incomplete_multiline_record_does_not_consume_next_event(self):
        for tail in ([], ['', event([{'text': 'unrelated'}], 'STOP')]):
            with self.subTest(tail=tail):
                chunks, _ = await self.run_attempts([['data: {', 'data: "response":', *tail]])
                self.assertEqual(self.calls, 1)
                self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_INVALID_EVENT')
                self.assertEqual(parts_of(chunks), [])

    async def test_empty_data_fields_at_record_edges_and_empty_record_errors(self):
        line = event([{'text': 'OK'}], 'STOP').rstrip('\n')
        for fields in (['data', line, 'data:', ''], [line, 'data:', '']):
            with self.subTest(fields=fields):
                chunks, _ = await self.run_attempts([fields])
                self.assertEqual(parts_of(chunks), [{'text': 'OK'}])
                self.assertFalse(any(isinstance(chunk, Response) for chunk in chunks))
        chunks, _ = await self.run_attempts([['data:', '', line]])
        self.assertEqual(json.loads(chunks[-1].body)['error']['status'], 'UPSTREAM_INVALID_EVENT')
        self.assertEqual(self.calls, 1)


    async def test_native_thought_followed_by_unmarked_body_succeeds(self):
        chunks, rows = await self.run_attempts([[event([{'text': '思考', 'thought': True}]),
                                               event([{'text': '仍不确定'}], 'STOP')]])
        self.assertEqual(self.calls, 1)
        self.assertEqual(parts_of(chunks), [{'text': '思考', 'thought': True}, {'text': '仍不确定'}])
        self.assertTrue(rows[0]['success'])

    async def test_no_retry_after_thought_or_body_on_transport_or_upstream_error(self):
        for part in [{'text': '思考', 'thought': True}, {'text': '正文'}]:
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
                                               [event([{'text': 'OK'}], 'STOP')]])
        self.assertEqual(self.calls, 2)
        self.assertEqual([r['status_code'] for r in rows], [500, 200])
        self.assertEqual(parts_of(chunks), [{'text': 'OK'}])


    async def test_real_httpx_line_iteration_and_response_prefetch(self):
        # Exercise the actual HTTP decoder and first-event prefetch, offline.
        payload = '\n' + event([{'text': 'OK'}], 'STOP').replace('data: ', 'data:')
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, text=payload))) as session:
            with patch.object(httpx_client, '_get_shared_client', AsyncMock(return_value=session)):
                response = await build_streaming_response_or_error(api.stream_request(
                    {'model': 'gemini-4-argon-medium', 'request': {'contents': []}}))
                chunks = []
                async def send(message):
                    if message['type']=='http.response.body' and message.get('body'):
                        chunks.append(message['body'].decode())
                async def receive():
                    await asyncio.Event().wait()
                await response({'type':'http','asgi':{'spec_version':'2.4'}}, receive, send)
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
                             {'text': 'synthetic-answer'}], 'STOP')
            self.saved.reset_mock()
            with patch.object(api, 'stream_post_async', upstream), patch.object(api, 'get_retry_config', AsyncMock(return_value={
                     'retry_enabled': True, 'max_retries': 2, 'retry_interval': 0})), patch.object(api, '_switch_credential_for_retry', AsyncMock(return_value=(True, None))):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                    response = await client.post(url, json=body)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(calls, 2)
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

    async def test_internal_cancel_after_heartbeat_is_unknown_interruption(self):
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
        self.assertEqual(self.saved.call_args.args[0]['status_code'], 500)
        self.assertIn('STREAM_INTERRUPTED_UNKNOWN', self.saved.call_args.args[0]['error_message'])


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
