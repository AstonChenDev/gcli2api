"""The custom batch tester must not call an HTTP-200 error a success."""
import json
import unittest
from unittest.mock import AsyncMock, patch
import httpx
from src.panel import creds
from src.stream_events import PROMPT_REJECTION_NOTICE
import test_credential_tools


class CredentialProbeErrorTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_credential_tools.CredentialToolsTests.asyncSetUp
    asyncTearDown = test_credential_tools.CredentialToolsTests.asyncTearDown

    async def test_embedded_errors_empty_and_invalid_responses_do_not_clear_errors(self):
        cases = [
            ({'error': {'code':403, 'status':'PERMISSION_DENIED', 'message':'model forbidden'}}, 403),
            ({'candidates':[{'finishReason':'PROHIBITED_CONTENT'}]}, 400),
            ({'candidates':[{'content':{'parts':[{'text':PROMPT_REJECTION_NOTICE}]}}]}, 400),
            ({'candidates':[{'finishReason':'STOP', 'content':{'parts':[]}}]}, 502),
            ('not valid JSON', 500),
        ]
        for body, expected in cases:
            with self.subTest(expected=expected, body=body):
                self.db.update_credential_state.reset_mock()
                response_body = json.dumps({'response':body}) if isinstance(body,dict) else body
                with patch.object(creds, 'get_antigravity_api_url', AsyncMock(return_value='https://upstream.invalid')), patch(
                    'src.httpx_client.post_async', AsyncMock(return_value=httpx.Response(200,content=response_body)),
                ):
                    result = await self.client.post('/creds/test/a@example.invalid')
                self.assertEqual(result.status_code, expected)
                self.assertFalse(result.json()['success'])
                self.assertEqual(result.json()['status_code'], expected)
                updates = [call.args[1] for call in self.db.update_credential_state.call_args_list]
                self.assertTrue(updates)
                self.assertTrue(all(u.get('error_codes') == [expected] for u in updates))
                self.assertTrue(all('disabled' not in u for u in updates))
