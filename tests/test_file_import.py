"""ZIP import stays responsive and never needs real Google credentials."""
import asyncio
import io
import json
import unittest
import zipfile
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, UploadFile, HTTPException
from src.panel import creds
from src.google_oauth_api import TokenError


def archive(entries):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as z:
        for name, value in entries:
            z.writestr(name, value)
    return output.getvalue()


def credential(i):
    return {'filename': f'{i}.json', 'content': json.dumps({'refresh_token': 'synthetic', 'email': f'{i}@example.invalid', 'tier': 'pro', 'project_id': 'test'})}


class FileImportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(creds, '_import_slots', asyncio.Semaphore(3)))
        self.app = FastAPI(); self.app.include_router(creds.router)
        self.app.dependency_overrides[creds.verify_panel_token] = lambda: 'test'
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test')

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_zip_reports_bad_entries_and_supports_bom_uppercase(self):
        raw = archive([('good.JSON', '\ufeff' + credential(1)['content']), ('bad.json', b'\xff'), ('__MACOSX/hidden.json', '{}'), ('nested/broken.json', '{')])
        with patch.object(creds, '_import_single_credential', AsyncMock(return_value={'status':'success','message':'导入成功'})) as importer:
            response = await self.client.post('/creds/upload-stream', files=[('files', ('test.ZIP', raw, 'application/zip'))])
        self.assertEqual(response.status_code, 200)
        events = [json.loads(line) for line in response.text.splitlines()]
        self.assertEqual(events[0]['total_count'], 3)
        self.assertEqual(events[-1]['uploaded_count'], 1)
        self.assertEqual(len(events[-1]['results']), 3)
        self.assertEqual(importer.await_count, 1)
        self.assertEqual(response.headers['x-accel-buffering'], 'no')
        self.assertNotIn('synthetic', response.text)

    async def test_slow_item_does_not_block_progress_and_times_out(self):
        async def process(data):
            if data['email'].startswith('slow'):
                await asyncio.sleep(10)
            return {'status': 'success', 'message': 'saved'}
        entries = [credential('slow'), credential('fast')]
        with patch.object(creds, '_import_single_credential', process), patch.object(creds, 'IMPORT_ITEM_TIMEOUT', .06), patch.object(creds, 'IMPORT_HEARTBEAT_SECONDS', .01):
            stream = creds._credential_import_events(entries)
            self.assertEqual((await anext(stream))['type'], 'start')
            event = await anext(stream)
            self.assertEqual(event['result']['filename'], 'fast.json')
            remaining = [item async for item in stream]
        self.assertTrue(any(item['type'] == 'heartbeat' for item in remaining))
        self.assertEqual(remaining[-1]['uploaded_count'], 1)
        self.assertEqual(remaining[-1]['completed_count'], 2)
        self.assertEqual(remaining[-1]['results'][0]['code'], 'import_timeout')

    async def test_concurrency_bounded_across_simultaneous_imports(self):
        active = maximum = 0
        async def process(data):
            nonlocal active, maximum
            active += 1; maximum = max(maximum, active)
            await asyncio.sleep(.005)
            active -= 1
            return {'status': 'success', 'message': 'saved'}
        async def run():
            return [item async for item in creds._credential_import_events([credential(i) for i in range(10)])]
        with patch.object(creds, '_import_single_credential', process):
            a, b = await asyncio.gather(run(), run())
        self.assertEqual(maximum, 3)
        self.assertEqual((a[-1]['uploaded_count'], b[-1]['uploaded_count']), (10, 10))

    async def test_disconnect_cancels_unfinished_workers(self):
        active = 0
        began = asyncio.Event()
        async def process(data):
            nonlocal active
            active += 1; began.set()
            try:
                await asyncio.sleep(10)
            finally:
                active -= 1
        with patch.object(creds, '_import_single_credential', process):
            stream = creds._credential_import_events([credential(i) for i in range(15)])
            await anext(stream); await began.wait()
            await stream.aclose()
        self.assertEqual(active, 0)
        self.assertEqual(creds._import_slots._value, 3)

    async def test_refresh_timeout_retries_and_does_not_mark_token_invalid(self):
        async def slow_refresh(instance):
            await asyncio.sleep(10)
        with patch.object(creds.Credentials, 'refresh', slow_refresh), patch.object(creds, 'IMPORT_REFRESH_TIMEOUT', .01), patch.object(creds, 'add_credential', AsyncMock()) as save:
            result = await creds._import_single_credential(json.loads(credential(1)['content']))
        self.assertEqual(result['code'], 'network_error')
        self.assertIn('不代表凭证无效', result['message'])
        save.assert_not_awaited()

    async def test_transient_refresh_retries_then_saves_once(self):
        calls = 0
        async def refresh(instance):
            nonlocal calls
            calls += 1
            if calls == 1:
                try:
                    raise httpx.ConnectTimeout('synthetic')
                except httpx.ConnectTimeout:
                    raise TokenError('refresh failed')
            instance.access_token = 'synthetic-access'
        with patch.object(creds.Credentials, 'refresh', refresh), patch.object(creds, 'add_credential', AsyncMock()) as save:
            result = await creds._import_single_credential(json.loads(credential(1)['content']))
        self.assertEqual(result['status'], 'success'); self.assertEqual(calls, 2)
        save.assert_awaited_once()

    async def test_permanent_oauth_error_no_retry_no_secrets(self):
        error = TokenError('sensitive-response-must-not-leak'); error.status_code = 400
        with patch.object(creds.Credentials, 'refresh', AsyncMock(side_effect=error)) as refresh, patch.object(creds, 'add_credential', AsyncMock()) as save:
            result = await creds._import_single_credential(json.loads(credential(1)['content']))
        self.assertEqual(result['code'], 'oauth_rejected')
        self.assertNotIn('sensitive-response', result['message'])
        self.assertEqual(refresh.await_count, 1); save.assert_not_awaited()

    async def test_invalid_files_and_auth_do_not_start_validation(self):
        with patch.object(creds, '_import_single_credential', AsyncMock()) as importer:
            self.assertEqual((await self.client.post('/creds/upload-stream', files={'files':('bad.zip', b'not a zip')})).status_code,400)
            self.assertEqual((await self.client.post('/creds/upload-stream', files={'files':('empty.zip', archive([('readme.txt','none')]))})).status_code,400)
            self.app.dependency_overrides.clear()
            self.assertIn((await self.client.post('/creds/upload-stream', files={'files':('test.json',credential(1)['content'])})).status_code,(401,403))
            importer.assert_not_awaited()

    async def test_legacy_json_endpoint_and_file_validation(self):
        with patch.object(creds, '_import_single_credential', AsyncMock(return_value={'status':'success','message':'saved'})):
            result=await self.client.post('/creds/upload',files=[('files',('ok.json',credential(1)['content'])),('files',('bad.json','{"refresh_token":123}'))])
        self.assertEqual(result.json()['uploaded_count'],1)
        self.assertEqual(result.json()['total_count'],2)
        self.assertIn('必须是字符串',result.json()['results'][1]['message'])

    async def test_archive_limits_rejected_before_import(self):
        with patch.object(creds, 'MAX_IMPORT_FILES', 1):
            with self.assertRaises(HTTPException):
                creds._extract_zip_content(archive([('1.json','{}'),('2.json','{}')]))
