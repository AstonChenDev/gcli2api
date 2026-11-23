"""Closing the HTTP import response must join workers before returning."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from src.panel import creds


class ImportHTTPDisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_receive_disconnect_and_failed_send_cancel_workers_on_both_asgi_versions(self):
        for version in ('2.0', '2.4'):
            for failed_send in (False, True):
                with self.subTest(version=version, failed_send=failed_send):
                    active = 0
                    started = []
                    cancelled = []
                    began = asyncio.Event()
                    disconnected = asyncio.Event()
                    release = asyncio.Event()
                    saved = []

                    async def process(entry):
                        nonlocal active
                        active += 1
                        started.append(entry['filename'])
                        if active == creds.IMPORT_CONCURRENCY:
                            began.set()
                        try:
                            await release.wait()
                            saved.append(entry['filename'])
                            return {'status': 'success'}
                        finally:
                            active -= 1
                            cancelled.append(entry['filename'])

                    async def send(message):
                        if message['type'] == 'http.response.body' and message.get('body'):
                            await began.wait()
                            if failed_send:
                                raise OSError('synthetic disconnected socket')
                            disconnected.set()

                    async def receive():
                        await disconnected.wait()
                        return {'type': 'http.disconnect'}

                    entries = [{'filename': str(i)} for i in range(20)]
                    with patch.object(creds, '_read_credential_files', AsyncMock(return_value=entries)), patch.object(
                            creds, '_process_credential_file', process):
                        response = await creds.upload_credentials_stream([], token='test')
                        try:
                            await asyncio.wait_for(response({'type': 'http', 'asgi': {
                                'spec_version': version}}, receive, send), 1)
                        except (ExceptionGroup, OSError) as exc:
                            # Starlette wraps send errors differently by ASGI version.
                            self.assertTrue(failed_send, repr(exc))
                        except Exception as exc:
                            from starlette.requests import ClientDisconnect
                            self.assertTrue(failed_send and isinstance(exc, ClientDisconnect), repr(exc))
                        self.assertEqual(active, 0)
                        self.assertEqual(len(started), creds.IMPORT_CONCURRENCY)
                        self.assertCountEqual(cancelled, started)
                        release.set()
                        await asyncio.sleep(0)
                        self.assertEqual(saved, [])
                        self.assertEqual([c async for c in response.body_iterator], [])

    async def test_completed_response_still_reports_all_committed_items(self):
        sent = []
        async def send(message):
            if message['type'] == 'http.response.body':
                sent.append(message.get('body', b''))
        async def receive():
            await asyncio.Event().wait()
        entries = [{'filename': str(i)} for i in range(4)]
        with patch.object(creds, '_read_credential_files', AsyncMock(return_value=entries)), patch.object(
                creds, '_process_credential_file', AsyncMock(return_value={'status': 'success'})) as process:
            response = await creds.upload_credentials_stream([], token='test')
            await asyncio.wait_for(response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, send), 1)
        final = json.loads(b''.join(sent).splitlines()[-1])
        self.assertEqual(final['type'], 'complete')
        self.assertEqual(final['uploaded_count'], 4)
        self.assertEqual(process.await_count, 4)
