"""Importing an exported note must not remove its whitespace or final newline."""
import json
import unittest
from unittest.mock import AsyncMock, patch

from src.panel import creds


class NoteImportFormattingTests(unittest.IsolatedAsyncioTestCase):
    async def test_import_preserves_backup_note_verbatim_and_legacy_import_omits_it(self):
        for note in ('  Work99  质保1个月\n  时间：2026.10.30\n', '', ' \t\n', None):
            with self.subTest(note=note):
                backup = {'refresh_token': 'synthetic', 'email': 'test@example.invalid',
                          'tier': 'ultra', 'project_id': 'test'}
                if note is not None:
                    backup['note'] = note
                async def refresh(instance):
                    instance.access_token = 'synthetic'
                with patch.object(creds.Credentials, 'refresh', refresh), patch.object(
                        creds, 'add_credential', AsyncMock()) as save:
                    result = await creds._process_credential_file({
                        'filename': 'backup.json', 'content': json.dumps(backup)})
                self.assertEqual(result['status'], 'success')
                if note is not None:
                    self.assertEqual(save.call_args.args[1]['note'], note)
                else:
                    self.assertNotIn('note', save.call_args.args[1])
