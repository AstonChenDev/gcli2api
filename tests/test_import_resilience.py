"""A malformed item must not strand a streaming ZIP import forever."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
from src.panel import creds
import test_file_import
from test_file_import import archive, credential


class ImportResilienceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_file_import.FileImportTests.asyncSetUp
    asyncTearDown = test_file_import.FileImportTests.asyncTearDown

    async def test_deep_json_reports_failure_and_finishes_other_files(self):
        raw = archive([('deep.json', '[' * 10000 + '0' + ']' * 10000), ('good.json', credential(1)['content'])])
        with patch.object(creds, '_import_single_credential', AsyncMock(return_value={'status':'success'})) as importer:
            response = await asyncio.wait_for(self.client.post('/creds/upload-stream', files={'files':('test.zip',raw)}), 1)
        events = [json.loads(line) for line in response.text.splitlines()]
        self.assertEqual(events[-1]['type'], 'complete')
        self.assertEqual(events[-1]['total_count'], 2)
        self.assertEqual(events[-1]['uploaded_count'], 1)
        self.assertEqual([r['status'] for r in events[-1]['results']], ['error', 'success'])
        importer.assert_awaited_once()

    async def test_unexpected_item_failure_is_isolated_without_leaking_details(self):
        async def process(entry):
            if entry['filename'] == 'bad.json':
                raise RuntimeError('synthetic-sensitive-value')
            return {'status':'success'}
        async def consume():
            return [item async for item in creds._credential_import_events([
                {'filename':'bad.json'}, {'filename':'good.json'}, {'filename':'another.json'},
            ])]
        with patch.object(creds, '_process_credential_file', process):
            events = await asyncio.wait_for(consume(), 1)
        final = events[-1]
        self.assertEqual(final['type'], 'complete')
        self.assertEqual(final['uploaded_count'], 2)
        self.assertEqual([r['status'] for r in final['results']], ['error','success','success'])
        self.assertNotIn('synthetic-sensitive-value', json.dumps(events))
