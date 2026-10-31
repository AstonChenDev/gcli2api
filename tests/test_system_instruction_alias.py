"""Regression: custom prompt injection must never emit two oneof aliases."""
import copy
import json
import os
import re
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, HTTPException
from src.api import antigravity as api
from src.converter.request_fields import normalize_system_instruction
from src.converter.antigravity_fix import normalize_antigravity_request
from src.converter.anti_truncation import apply_anti_truncation
from src.router.antigravity import gemini
from src.thought_boundary import prepare_thought_boundary

SYSTEM = {'role': 'system', 'parts': [{'text': '保留原提示词。'}, {'text': 'Keep the second rule.'}]}
CONTENTS = [{'role': 'user', 'parts': [{'text': 'Please reply OK.'}]}]


class SystemAliasTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {'ARGON_THOUGHT_BOUNDARY_ENABLED': 'true'}))
        for name, value in {'get_valid_credential': ('test@example.invalid', {'access_token': 'synthetic', 'project_id': 'test'}),
            'get_antigravity_api_url': 'https://example.invalid', 'get_auto_ban_error_codes': [],
            'get_retry_config': {'max_retries': 0, 'retry_interval': 0, 'retry_enabled': False},
            'record_api_call_success': None, 'record_api_call_error': None, '_record_request_body_detail': ''}.items():
            self.stack.enter_context(patch.object(api, name, AsyncMock(return_value=value)))
        self.stack.enter_context(patch('src.usage_log.record_usage_log', AsyncMock()))
        for name, value in {'get_return_thoughts_to_frontend': True, 'get_model_rewrite_filter': (False, []), 'get_sensitive_word_filter': (False, [])}.items():
            self.stack.enter_context(patch('config.' + name, AsyncMock(return_value=value)))

    def assert_system(self, request):
        self.assertNotIn('system_instruction', request)
        self.assertEqual(request['systemInstruction']['parts'][:2], SYSTEM['parts'])

    def test_snake_camel_and_identical_aliases_preserve_original_and_stable_marker(self):
        markers = []
        for keys in [('system_instruction',), ('systemInstruction',), ('system_instruction', 'systemInstruction')]:
            request = {'contents': copy.deepcopy(CONTENTS), **{key: copy.deepcopy(SYSTEM) for key in keys}}
            original = copy.deepcopy(request)
            result, marker = prepare_thought_boundary('gemini-4-argon-medium', request)
            self.assert_system(result)
            self.assertEqual(result['systemInstruction']['role'], 'system')
            self.assertEqual(len(result['systemInstruction']['parts']), 3)
            self.assertIn(marker, result['systemInstruction']['parts'][-1]['text'])
            self.assertEqual(original, request)
            markers.append(marker)
        self.assertEqual(len(set(markers)), 1)

    def test_disabled_guard_and_structured_tools_still_have_one_system_field(self):
        request = {'contents': CONTENTS, 'system_instruction': SYSTEM}
        for extra in [{'tools': [{'functionDeclarations': []}]}, {'generationConfig': {'responseMimeType': 'application/json'}}]:
            result, marker = prepare_thought_boundary('gemini-4-argon-medium', {**request, **extra})
            self.assertIsNone(marker); self.assert_system(result)
            self.assertEqual(len(result['systemInstruction']['parts']), 2)
        with patch.dict(os.environ, {'ARGON_THOUGHT_BOUNDARY_ENABLED': 'false'}):
            result, marker = prepare_thought_boundary('gemini-4-argon-medium', request)
            self.assertIsNone(marker); self.assert_system(result)

    def test_null_missing_aliases_and_private_copy(self):
        for request in [{}, {'systemInstruction': None}, {'system_instruction': None, 'systemInstruction': None}]:
            self.assertEqual(normalize_system_instruction(request), {})
        result = normalize_system_instruction({'systemInstruction': None, 'system_instruction': SYSTEM})
        self.assert_system(result)
        result['systemInstruction']['parts'][0]['text'] = 'changed'
        self.assertEqual(SYSTEM['parts'][0]['text'], '保留原提示词。')

    def test_conflicting_aliases_rejected_without_silent_prompt_loss(self):
        request = {'systemInstruction': SYSTEM, 'system_instruction': {'parts': [{'text': 'different'}]}}
        original = copy.deepcopy(request)
        with self.assertRaises(HTTPException) as context:
            prepare_thought_boundary('gemini-4-argon-medium', request)
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(request, original)

    async def test_normalizer_antitruncation_and_final_wrapper_preserve_one_alias(self):
        original = {'model': 'gemini-4-argon-medium', 'contents': CONTENTS, 'system_instruction': SYSTEM}
        before = copy.deepcopy(original)
        normalized = await normalize_antigravity_request(original)
        self.assert_system(normalized)
        anti = apply_anti_truncation({'model': original['model'], 'request': original})
        self.assert_system(anti['request'])
        self.assertGreater(len(anti['request']['systemInstruction']['parts']), 2)
        payload, _ = await api.wrap_cli_request(original, original['model'], 'test')
        self.assert_system(payload['request'])
        self.assertEqual(original, before)

    async def test_real_route_pipeline_stream_and_nonstream_with_strict_upstream(self):
        captured = []
        async def streaming(**kwargs):
            inner = kwargs['body']['request']; self.assert_system(inner)
            captured.append(copy.deepcopy(inner))
            contract = inner['systemInstruction']['parts'][-1]['text']
            marker = re.search(r'<<<AGY_FINAL_[0-9a-f]+>>>', contract)[0]
            # Verify the existing thought/body separation still works end to end.
            for part in [{'text': 'native thought', 'thought': True}, {'text': 'unmarked thought' + marker + 'OK'}]:
                yield 'data: ' + json.dumps({'response': {'responseId': 'test-alias', 'candidates': [{'finishReason': 'STOP' if part['text'].endswith('OK') else 'FINISH_REASON_UNSPECIFIED', 'content': {'role': 'model', 'parts': [part]}}]}}) + '\n\n'
        async def nonstream(**kwargs):
            inner = kwargs['json']['request']; self.assert_system(inner)
            captured.append(copy.deepcopy(inner))
            return httpx.Response(200, json={'response': {'candidates': [{'content': {'parts': [{'text': 'OK'}]}}]}})
        app = FastAPI(); app.include_router(gemini.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        with patch.object(api, 'stream_post_async', streaming), patch.object(api, 'post_async', nonstream):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                for field in ('system_instruction', 'systemInstruction'):
                    for method in ('streamGenerateContent', 'generateContent'):
                        response = await client.post('/v1beta/models/gemini-4-argon-medium:' + method, json={'contents': CONTENTS, field: SYSTEM})
                        self.assertEqual(response.status_code, 200)
                        if method.startswith('stream'):
                            parts = [p for line in response.text.splitlines() if line.startswith('data: ') for c in json.loads(line[6:]).get('candidates', []) for p in c.get('content', {}).get('parts', [])]
                            self.assertEqual(''.join(p.get('text','') for p in parts if not p.get('thought')), 'OK')
                            self.assertEqual(''.join(p.get('text','') for p in parts if p.get('thought')), 'native thoughtunmarked thought')
        self.assertEqual(len(captured), 4)

    async def test_conflict_returns_http_400_before_stream_headers_or_upstream(self):
        app = FastAPI(); app.include_router(gemini.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        with patch.object(api, 'get_valid_credential', AsyncMock()) as credential:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                for method in ('streamGenerateContent', 'generateContent'):
                    response = await client.post('/v1beta/models/gemini-4-argon-medium:' + method, json={'contents':CONTENTS, 'systemInstruction':SYSTEM,'system_instruction':{'parts':[{'text':'conflict'}]}})
                    self.assertEqual(response.status_code, 400)
                    self.assertIn('内容冲突', response.json()['detail'])
        credential.assert_not_awaited()
