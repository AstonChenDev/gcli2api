import json
import unittest
from unittest.mock import patch,AsyncMock
import httpx
import test_custom_audit
from src.api import antigravity as api
from src.stream_events import PROMPT_REJECTION_NOTICE

class NonStreamErrors(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def run_response(self, content, status=200):
        self.saved.reset_mock()
        response=httpx.Response(status,content=content)
        with patch.object(api,'post_async',AsyncMock(return_value=response)),patch.object(api,'record_api_call_error',AsyncMock()):
            result=await api.non_stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
        self.assertEqual(self.saved.await_count,1)
        row=self.saved.call_args.args[0]
        self.assertEqual(row['status_code'],result.status_code)
        self.assertEqual(row['success'],result.status_code==200)
        return result

    async def test_empty_body_and_metadata_only_are_502_not_false_200(self):
        for body in (b'',b'{}',b'{"usageMetadata":{"totalTokenCount":12}}'):
            response=await self.run_response(body)
            self.assertEqual(response.status_code,502)
            self.assertEqual(json.loads(response.body)['error']['status'],'UPSTREAM_EMPTY_RESPONSE')

    async def test_known_prompt_rejection_is_400(self):
        body={'response':{'candidates':[{'content':{'parts':[{'text':PROMPT_REJECTION_NOTICE}]}}]}}
        response=await self.run_response(json.dumps(body).encode())
        self.assertEqual(response.status_code,400)
        self.assertEqual(json.loads(response.body)['error']['message'],PROMPT_REJECTION_NOTICE)

    async def test_blocked_content_keeps_partial_output_and_reason(self):
        body={'response':{'candidates':[{'content':{'parts':[{'text':'partial answer'}]},'finishReason':'PROHIBITED_CONTENT'}]}}
        response=await self.run_response(json.dumps(body).encode())
        self.assertEqual(response.status_code,400)
        result=json.loads(response.body)
        self.assertEqual(result['error']['blockReason'],'PROHIBITED_CONTENT')
        self.assertEqual(result['partialResponse']['candidates'][0]['content']['parts'][0]['text'],'partial answer')

    async def test_explicit_upstream_error_codes_are_preserved(self):
        for status in (400,401,403,404,429,500,502,503):
            body=json.dumps({'error':{'code':status,'message':'synthetic upstream error'}}).encode()
            for http_status in (200,status):
                response=await self.run_response(body,http_status)
                self.assertEqual(response.status_code,status)
                self.assertEqual(json.loads(response.body)['error']['code'],status)

    async def test_invalid_json_is_not_success(self):
        response=await self.run_response(b'<html>invalid upstream</html>')
        self.assertEqual(response.status_code,500)
        self.assertEqual(json.loads(response.body)['error']['status'],'UPSTREAM_INVALID_EVENT')

    async def test_valid_answer_and_tool_response_are_not_errors(self):
        for part in ({'text':'answer'},{'functionCall':{'name':'tool','args':{}}}):
            response=await self.run_response(json.dumps({'candidates':[{'content':{'parts':[part]},'finishReason':'STOP'}]}).encode())
            self.assertEqual(response.status_code,200)
