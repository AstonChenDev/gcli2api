import asyncio
import copy
import io
import json
import unittest
import zipfile
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.api import antigravity as api
from src.converter.antigravity_fix import normalize_antigravity_request
from src.panel import creds
from src.storage.database import Database


class CustomAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        for name, value in {'get_return_thoughts_to_frontend':True,'get_model_rewrite_filter':(False,[]),'get_sensitive_word_filter':(False,[])}.items():
            self.stack.enter_context(patch('config.'+name,AsyncMock(return_value=value)))
        for name,value in {'get_valid_credential':('test@example.invalid',{'access_token':'synthetic','project_id':'test'}),'get_antigravity_api_url':'https://example.invalid','get_auto_ban_error_codes':[], 'get_retry_config':{'max_retries':0,'retry_interval':0,'retry_enabled':False},'record_api_call_success':None,'_record_request_body_detail':''}.items():
            self.stack.enter_context(patch.object(api,name,AsyncMock(return_value=value)))
        self.saved=self.stack.enter_context(patch('src.usage_log.record_usage_log',AsyncMock()))

    async def test_other_snake_aliases_do_not_duplicate_or_ignore_thought_optout(self):
        request={'model':'gemini-4-argon-medium','contents':[{'role':'user','parts':[{'text':'test'}]}],
            'generation_config':{'temperature':.3,'thinking_config':{'include_thoughts':False},'response_json_schema':{'type':'object','properties':{'system_instruction':{'type':'string'}}}},
            'tool_config':{'function_calling_config':{'mode':'AUTO'}},'safety_settings':[]}
        before=copy.deepcopy(request)
        normalized=await normalize_antigravity_request(request)
        payload,_=await api.wrap_cli_request(normalized,request['model'],'test')
        inner=payload['request']
        for alias in ('generation_config','tool_config','safety_settings'):
            self.assertNotIn(alias,inner)
        self.assertEqual(inner['generationConfig']['temperature'],.3)
        self.assertFalse(inner['generationConfig']['thinkingConfig']['includeThoughts'])
        self.assertIn('system_instruction',inner['generationConfig']['responseJsonSchema']['properties'])
        self.assertEqual(request,before)

    async def test_error_then_generator_close_does_not_add_fake_success(self):
        async def upstream(**kwargs):
            yield Response(status_code=502, content=json.dumps({'error': {'code': 502, 'status': 'UPSTREAM_EMPTY_RESPONSE'}}))
        with patch.object(api,'stream_post_async',upstream):
            stream=api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
            try:
                async for chunk in stream:
                    if isinstance(chunk,Response):
                        self.assertEqual(chunk.status_code,502)
                        break
            finally:
                await stream.aclose()
        self.assertEqual(self.saved.await_count,1)
        self.assertFalse(self.saved.call_args.args[0]['success'])

    async def test_cancelled_stream_is_recorded_once_as_interrupted(self):
        blocked=asyncio.Event()
        async def upstream(**kwargs):
            yield 'data: '+json.dumps({'response':{'responseId':'cancel-audit','candidates':[{'content':{'parts':[{'text':'reasoning','thought':True}]}}]}})+'\n\n'
            blocked.set()
            await asyncio.Event().wait()
        with patch.object(api,'stream_post_async',upstream):
            stream=api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}})
            await anext(stream)
            task=asyncio.create_task(anext(stream))
            await blocked.wait();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            await stream.aclose()
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],500)
        self.assertIn('STREAM_INTERRUPTED_UNKNOWN', self.saved.call_args.args[0]['error_message'])
        self.assertFalse(self.saved.call_args.args[0]['success'])

    async def test_anyio_disconnect_scope_allows_final_accounting(self):
        import anyio
        blocked = asyncio.Event()
        async def upstream(**kwargs):
            yield 'data: {"response":{"responseId":"anyio-cancel","candidates":[{"content":{"parts":[{"text":"thinking","thought":true}]}}]}}\n\n'
            blocked.set()
            await asyncio.Event().wait()
        async def delayed_detail(*args, **kwargs):
            await asyncio.sleep(.01)
            return ""
        async def consume():
            async for _ in api.stream_request({'model':'gemini-4-argon-medium','request':{'contents':[]}}):
                pass
        with patch.object(api,'stream_post_async',upstream), patch.object(api,'_record_request_body_detail',delayed_detail):
            async with anyio.create_task_group() as group:
                group.start_soon(consume)
                await blocked.wait()
                group.cancel_scope.cancel()
        self.assertEqual(self.saved.await_count,1)
        self.assertEqual(self.saved.call_args.args[0]['status_code'],500)
        self.assertIn('STREAM_INTERRUPTED_UNKNOWN', self.saved.call_args.args[0]['error_message'])

    async def test_schema_snake_case_bypasses_text_boundary_and_conflicts_reject(self):
        from src.thought_boundary import prepare_thought_boundary
        from src.converter.request_fields import normalize_gemini_request_fields
        from fastapi import HTTPException
        request = {'generation_config':{'response_mime_type':'application/json','response_json_schema':{'type':'object','properties':{'top_k':{'type':'number'}}}}}
        prepared, marker = prepare_thought_boundary('gemini-4-argon-medium', request)
        self.assertIsNone(marker)
        self.assertNotIn('systemInstruction',prepared)
        self.assertIn('top_k',prepared['generationConfig']['responseJsonSchema']['properties'])
        with self.assertRaises(HTTPException):
            normalize_gemini_request_fields({'generationConfig':{'topP':.5,'top_p':.9}})

    async def test_selected_export_and_reimport_preserve_real_tier_and_note(self):
        db=Database();db._engine=create_async_engine('sqlite+aiosqlite:///:memory:')
        db._session_factory=async_sessionmaker(db._engine,expire_on_commit=False);db._initialized=True
        try:
            async with db._engine.begin() as conn:await conn.run_sync(db._create_tables)
            await db.store_credential('audit@example.invalid',{'refresh_token':'synthetic','project_id':'test','tier':'ultra_lite'})
            await db.update_credential_state('audit@example.invalid',{'note':'备份备注'})
            app=FastAPI();app.include_router(creds.router);app.dependency_overrides[creds.verify_panel_token]=lambda:'test'
            async def refresh(instance):instance.access_token='synthetic-access'
            with patch.object(creds,'get_database',AsyncMock(return_value=db)), patch.object(creds.Credentials,'refresh',refresh),patch('src.credential_manager.get_database',AsyncMock(return_value=db)):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
                    response=await client.post('/creds/download-selected',json={'emails':['audit@example.invalid']})
                    self.assertEqual(response.status_code,200)
                    with zipfile.ZipFile(io.BytesIO(response.content)) as z:record=json.loads(z.read(z.namelist()[0]))
                    self.assertEqual(record['tier'],'ultra_lite')
                    self.assertEqual(record['note'],'备份备注')
                    for path in ('/creds/export/audit@example.invalid','/creds/download-all'):
                        exported=await client.get(path)
                        self.assertEqual(exported.status_code,200)
                        if path.endswith('download-all'):
                            with zipfile.ZipFile(io.BytesIO(exported.content)) as z: data=json.loads(z.read(z.namelist()[0]))
                        else: data=exported.json()
                        self.assertEqual((data['tier'],data['note']),('ultra_lite','备份备注'))
                    record['email']='restored@example.invalid'
                    response=await client.post('/creds/upload',files={'files':('restore.json',json.dumps(record))})
                    self.assertEqual(response.json()['uploaded_count'],1)
                    legacy=dict(record);legacy.pop('note')
                    response=await client.post('/creds/upload',files={'files':('legacy.json',json.dumps(legacy))})
                    self.assertEqual(response.json()['uploaded_count'],1)
            restored=await db.get_credential_state('restored@example.invalid')
            self.assertEqual((restored['tier'],restored['note']),('ultra_lite','备份备注'))
        finally:await db.close()
