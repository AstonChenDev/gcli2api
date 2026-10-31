"""Cross-protocol invariants under different SSE packetization and endings."""
import json
import random
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
import httpx
from fastapi import FastAPI
from src.api import antigravity as api
from src.router.antigravity import gemini, openai, anthropic
import test_custom_audit
from test_thought_boundary import event
from test_boundary_part_metadata import MARKER


class ProtocolMatrixAuditTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_custom_audit.CustomAuditTests.asyncSetUp

    async def test_72_packetization_thought_continuation_and_error_combinations(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: 'test'
        app.dependency_overrides[openai.authenticate_bearer] = lambda: 'test'
        cases = [
            ('gemini', '/v1beta/models/gemini-4-argon-medium:streamGenerateContent', {'contents':[{'role':'user','parts':[{'text':'test'}]}]}),
            ('openai', '/v1/chat/completions', {'model':'gemini-4-argon-medium','stream':True,'messages':[{'role':'user','content':'test'}]}),
            ('anthropic', '/v1/messages', {'model':'gemini-4-argon-medium','stream':True,'max_tokens':4096,'messages':[{'role':'user','content':'test'}]}),
        ]
        planning = 'Thinking Process:\n1. **Check:** request.\n2. **Write:** answer.\n'
        answer = '正文第一行。\n第二行包含空格 和 emoji 🌙。'
        whole = planning + MARKER + answer
        completed = 0
        for protocol, url, body in cases:
            for include in (True, False):
                for anti in (True, False):
                    for finish in ('STOP', 'PROHIBITED_CONTENT'):
                        for split in ('whole', 'character', 'random'):
                            with self.subTest(protocol=protocol, include=include, anti=anti, finish=finish, split=split):
                                rng = random.Random(713)
                                pieces = [whole] if split == 'whole' else list(whole) if split == 'character' else []
                                if split == 'random':
                                    offset = 0
                                    while offset < len(whole):
                                        size = rng.randint(1, 17)
                                        pieces.append(whole[offset:offset+size]); offset += size
                                async def upstream(**kwargs):
                                    yield event([{'text':'native reasoning', 'thought':True, 'thoughtSignature':'native-test-signature'}])
                                    for index, piece in enumerate(pieces):
                                        yield event([{'text':piece, 'thoughtSignature':f'fragment-{index}'}])
                                    yield event([{'text':'', 'thoughtSignature':'final-metadata'}], finish)
                                self.saved.reset_mock()
                                with ExitStack() as stack:
                                    stack.enter_context(patch.object(api, 'stream_post_async', upstream))
                                    stack.enter_context(patch.object(api, 'prepare_thought_boundary', lambda m,r:(r,MARKER)))
                                    stack.enter_context(patch.object(api, 'record_api_call_error', AsyncMock()))
                                    stack.enter_context(patch('config.get_return_thoughts_to_frontend', AsyncMock(return_value=include)))
                                    for module in (gemini, openai, anthropic):
                                        stack.enter_context(patch.object(module, 'is_anti_truncation_model', return_value=anti))
                                        stack.enter_context(patch.object(module, 'get_anti_truncation_max_attempts', AsyncMock(return_value=1)))
                                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                                        response = await client.post(url, json=body)
                                self.assertEqual(response.status_code, 200)
                                reasoning, content, errors = '', '', []
                                for line in response.text.splitlines():
                                    if not line.startswith('data:') or line[5:].strip() == '[DONE]':
                                        continue
                                    item = json.loads(line[5:]); item = item.get('response', item)
                                    if item.get('error'):
                                        errors.append(item['error'])
                                    if protocol == 'gemini':
                                        for candidate in item.get('candidates', []):
                                            for part in candidate.get('content', {}).get('parts', []):
                                                if part.get('thought'):
                                                    reasoning += part.get('text', '')
                                                else:
                                                    content += part.get('text', '')
                                    elif protocol == 'openai':
                                        for choice in item.get('choices', []):
                                            delta = choice.get('delta', {})
                                            reasoning += delta.get('reasoning_content') or ''
                                            content += delta.get('content') or ''
                                    elif item.get('type') == 'content_block_delta':
                                        delta = item['delta']
                                        reasoning += delta.get('thinking', '')
                                        content += delta.get('text', '')
                                self.assertEqual(content, answer)
                                self.assertEqual(reasoning, 'native reasoning' + planning if include else '')
                                self.assertEqual(bool(errors), finish != 'STOP')
                                if errors:
                                    self.assertEqual(errors[-1]['blockReason'], 'PROHIBITED_CONTENT')
                                self.assertEqual(self.saved.await_count, 1)
                                row = self.saved.call_args.args[0]
                                self.assertEqual(row['status_code'], 200 if finish == 'STOP' else 400)
                                self.assertEqual(row['request_id'], response.headers['x-request-id'])
                                completed += 1
        self.assertEqual(completed, 72)
