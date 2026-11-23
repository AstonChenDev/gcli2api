import json
import unittest
from unittest.mock import patch,AsyncMock
import httpx
import test_custom_audit
from src.api import antigravity as api
from src.stream_events import PROMPT_REJECTION_NOTICE

class NonStreamErrors(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def run_response(self, content, status=200, *, candidate_count=1):
        self.saved.reset_mock()
        response=httpx.Response(status,content=content)
        with patch.object(api,'post_async',AsyncMock(return_value=response)),patch.object(api,'record_api_call_error',AsyncMock()):
            result=await api.non_stream_request({'model':'gemini-4-argon-medium','request':{
                'contents':[], 'generationConfig':{'candidateCount':candidate_count}}})
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

    async def test_partial_refusal_is_scoped_to_its_candidate(self):
        refused = {'index': 0, 'content': {'parts': [{'text': 'partial'}]}, 'finishReason': 'RECITATION'}
        answer = {'index': 1, 'content': {'parts': [{'text': 'answer'}]}, 'finishReason': 'STOP'}
        for candidates in ([refused, answer], [answer, refused], [refused]):
            body = {'response': {'candidates': candidates}}
            result = await self.run_response(json.dumps(body).encode(), candidate_count=2)
            if len(candidates) == 2:
                self.assertEqual(result.status_code, 200)
                self.assertEqual(json.loads(result.body), body)
            else:
                self.assertEqual(result.status_code, 400)
                self.assertEqual(json.loads(result.body)['error']['blockReason'], 'RECITATION')

    async def test_explicit_upstream_error_codes_are_preserved(self):
        for status in (400,401,403,404,429,500,502,503):
            body=json.dumps({'error':{'code':status,'message':'synthetic upstream error'}}).encode()
            for http_status in (200,status):
                response=await self.run_response(body,http_status)
                self.assertEqual(response.status_code,status)
                self.assertEqual(json.loads(response.body)['error']['code'],status)

    async def test_model_404_switches_projects_without_reusing_the_failed_credential(self):
        model = 'gemini-4-argon-medium'

        async def select(**kwargs):
            excluded = kwargs.get('exclude_emails')
            if not excluded:
                return 'first@example.invalid', {'access_token': 'first', 'project_id': 'project-a'}
            self.assertEqual(excluded, {'first@example.invalid'})
            return 'second@example.invalid', {'access_token': 'second', 'project_id': 'project-b'}

        async def upstream(**kwargs):
            if kwargs['credential_email'] == 'first@example.invalid':
                return httpx.Response(404, json={'error': {'message': 'model unavailable'}})
            self.assertEqual(kwargs['json']['project'], 'project-b')
            return httpx.Response(200, json={'candidates': [
                {'content': {'parts': [{'text': 'recovered'}]}, 'finishReason': 'STOP'}]})

        with patch.object(api, 'get_valid_credential', AsyncMock(side_effect=select)), \
             patch.object(api, 'post_async', AsyncMock(side_effect=upstream)) as post, \
             patch.object(api, 'get_retry_config', AsyncMock(return_value={
                 'retry_enabled': True, 'max_retries': 3, 'retry_interval': 0})), \
             patch.object(api, '_record_model_not_found', AsyncMock()) as marked:
            result = await api.non_stream_request({'model': model, 'request': {'contents': []}})

        self.assertEqual(result.status_code, 200)
        self.assertEqual(post.await_count, 2)
        marked.assert_awaited_once()
        self.assertEqual([call.args[0]['status_code'] for call in self.saved.call_args_list], [404, 200])

    async def test_invalid_json_is_not_success(self):
        response=await self.run_response(b'<html>invalid upstream</html>')
        self.assertEqual(response.status_code,500)
        self.assertEqual(json.loads(response.body)['error']['status'],'UPSTREAM_INVALID_EVENT')

    async def test_valid_answer_and_tool_response_are_not_errors(self):
        for part in ({'text':'answer'},{'functionCall':{'name':'tool','args':{}}}):
            response=await self.run_response(json.dumps({'candidates':[{'content':{'parts':[part]},'finishReason':'STOP'}]}).encode())
            self.assertEqual(response.status_code,200)
