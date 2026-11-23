"""Proxy CRUD and real loopback transports; no provider calls or credentials."""
import asyncio
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from anyio import CancelScope
from fastapi import FastAPI, HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src import credential_proxy, httpx_client
from src.google_oauth_api import Credentials
from src.panel import creds as panel_creds, proxies
from src.storage.database import AntigravityCredential, Base, Database
from src.utils import verify_panel_token


class CredentialProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = Database()
        self.db._engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        self.db._session_factory = async_sessionmaker(self.db._engine, expire_on_commit=False)
        self.db._initialized = True
        async with self.db._engine.begin() as c:
            await c.run_sync(Base.metadata.create_all)
        await self.db.store_credential('a@example.invalid', {'access_token': 'test', 'note': 'A'})
        await self.db.store_credential('b@example.invalid', {'access_token': 'test', 'note': 'B'})
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for module in (credential_proxy, proxies):
            self.stack.enter_context(patch.object(module, 'get_database', AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(httpx_client, '_shared_clients', []))
        self.stack.enter_context(patch.object(httpx_client, '_client_users', {}))
        self.stack.enter_context(patch.object(httpx_client, '_shared_clients_lock', asyncio.Lock()))
        self.stack.enter_context(patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value=None)))
        self.app = FastAPI()
        self.app.include_router(proxies.router)
        self.app.dependency_overrides[verify_panel_token] = lambda: 'test-panel'
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url='http://test')
        self.addAsyncCleanup(self.db.close)
        self.addAsyncCleanup(self.client.aclose)
        self.addAsyncCleanup(httpx_client.close_shared_clients)

    async def test_crud_binding_secret_redaction_and_refresh_preservation(self):
        created = await self.client.post('/proxies', json={'name': 'HK', 'url': 'socks5h://user:secret@localhost:1080'})
        self.assertEqual(created.status_code, 200)
        item = created.json()
        self.assertNotIn('secret', created.text)
        pid = item['id']
        self.assertEqual((await self.client.patch('/creds/proxy/a@example.invalid', json={'proxy_id': pid})).status_code, 200)
        await self.db.store_credential('a@example.invalid', {'access_token': 'renewed'})
        await self.db.update_access_token('a@example.invalid', '', 'newer', '')
        summary = await self.db.get_credentials_summary()
        row = next(r for r in summary['items'] if r['email'] == 'a@example.invalid')
        self.assertEqual(row['proxy_id'], pid)
        self.assertEqual(row['note'], 'A')
        listed = await self.client.get('/proxies')
        self.assertEqual(listed.json()['items'][0]['bound_count'], 1)
        self.assertNotIn('secret', listed.text)
        self.assertEqual((await self.client.delete('/proxies/' + pid)).status_code, 400)
        self.assertEqual((await self.client.patch('/proxies/' + pid, json={'name': 'renamed'})).status_code, 200)
        self.assertEqual(await credential_proxy.resolve_proxy('a@example.invalid'), 'socks5h://user:secret@localhost:1080')
        await self.client.patch('/creds/proxy/a@example.invalid', json={'proxy_id': None})
        self.assertIsNone(await credential_proxy.resolve_proxy('a@example.invalid'))
        self.assertEqual((await self.client.delete('/proxies/' + pid)).status_code, 200)

    async def test_validation_auth_and_dangling_binding_fail_closed(self):
        for url in ['file:///tmp/a', 'http://host', 'http://host:70000', 'socks4://host:1080', 'http://host:80/path', 'http://host:0', 'http://host:80/#x', 'http://host:80\nX']:
            result = await self.client.post('/proxies', json={'name': 'bad', 'url': url})
            self.assertEqual(result.status_code, 400, url)
        self.assertEqual((await self.client.patch('/creds/proxy/a@example.invalid', json={'proxy_id': 'missing'})).status_code, 404)
        self.assertEqual((await self.client.patch('/creds/proxy/missing@example.invalid', json={'proxy_id': None})).status_code, 404)
        async with self.db.session() as s:
            row = await s.scalar(select(AntigravityCredential).where(AntigravityCredential.email == 'a@example.invalid'))
            row.proxy_id = 'missing'
        with self.assertRaises(ValueError):
            await credential_proxy.resolve_proxy('a@example.invalid')
        self.app.dependency_overrides.clear()
        for method, path, body in [('GET', '/proxies', None), ('POST', '/proxies', {'name': 'x'}), ('PATCH', '/proxies/x', {'name': 'x'}), ('DELETE', '/proxies/x', None), ('PATCH', '/creds/proxy/a@example.invalid', {'proxy_id': None})]:
            self.assertIn((await self.client.request(method, path, json=body)).status_code, (401, 403))

    async def test_binding_patch_requires_an_explicit_choice(self):
        url = 'http://localhost:3000'
        item = await credential_proxy.save_proxy(self.db, 'bound', url)
        endpoint = '/creds/proxy/a@example.invalid'
        await self.client.patch(endpoint, json={'proxy_id': item['id']})
        for payload in ({}, {'proxyId': None}):
            result = await self.client.patch(endpoint, json=payload)
            self.assertEqual(result.status_code, 422)
            self.assertEqual(await credential_proxy.resolve_proxy('a@example.invalid'), url)
        self.assertEqual((await self.client.patch(endpoint, json={'proxy_id': None})).status_code, 200)
        self.assertIsNone(await credential_proxy.resolve_proxy('a@example.invalid'))

    async def test_proxy_update_reports_existing_bindings(self):
        item = await credential_proxy.save_proxy(self.db, 'initial', 'http://localhost:3000')
        for email in ('a@example.invalid', 'b@example.invalid'):
            await credential_proxy.bind_proxy(self.db, email, item['id'])
        updated = await self.client.patch('/proxies/' + item['id'], json={'name': 'renamed'})
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()['bound_count'], 2)
        listed = (await self.client.get('/proxies')).json()['items']
        self.assertEqual(updated.json(), listed[0])

    async def proxy_server(self, label, hold=None, status=b'200 OK'):
        seen = asyncio.Queue()
        tasks = set()
        async def serve(reader, writer):
            tasks.add(asyncio.current_task())
            try:
                headers = await reader.readuntil(b'\r\n\r\n')
                for line in headers.split(b'\r\n'):
                    if line.lower().startswith(b'content-length:'):
                        await reader.readexactly(int(line.split(b':', 1)[1]))
                seen.put_nowait(headers)
                if hold:
                    await hold.wait()
                body = label.encode()
                writer.write(b'HTTP/1.1 ' + status + b'\r\nConnection: close\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        async def cleanup():
            server.close()
            await server.wait_closed()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.addAsyncCleanup(cleanup)
        return f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}', seen

    async def test_panel_probe_uses_saved_proxy_and_checks_expected_status(self):
        for status, ok in [(b'204 No Content', True), (b'200 OK', False)]:
            url, seen = await self.proxy_server('', status=status)
            item = await credential_proxy.save_proxy(self.db, 'test', url)
            with patch.object(credential_proxy, '_PROBE_URL', 'http://probe.invalid/generate_204'):
                result = await self.client.post(f"/proxies/{item['id']}/test")
            self.assertEqual(result.status_code, 200)
            self.assertIs(result.json()['ok'], ok)
            self.assertGreaterEqual(result.json()['elapsed_ms'], 0)
            self.assertIn(b'GET http://probe.invalid/generate_204', seen.get_nowait())
        self.assertEqual((await self.client.post('/proxies/unknown/test')).status_code, 404)
        self.app.dependency_overrides.clear()
        self.assertIn((await self.client.post('/proxies/unknown/test')).status_code, (401, 403))

    async def test_probe_https_tunnel_auth_failure_and_timeout(self):
        url, seen = await self.proxy_server('', status=b'407 Proxy Authentication Required')
        url = url.replace('://', '://user:private-password@')
        item = await credential_proxy.save_proxy(self.db, 'test', url)
        route = f"/proxies/{item['id']}/test"
        result = await self.client.post(route)
        self.assertFalse(result.json()['ok'])
        self.assertIn('认证失败', result.json()['message'])
        self.assertNotIn('private-password', result.text)
        self.assertIn(b'CONNECT connectivitycheck.gstatic.com:443', seen.get_nowait())
        release = asyncio.Event()
        url, _ = await self.proxy_server('', hold=release)
        await credential_proxy.save_proxy(self.db, 'test', url, item['id'])
        try:
            with patch.object(credential_proxy, '_PROBE_TIMEOUT', 0.2):
                result = await self.client.post(route)
            self.assertFalse(result.json()['ok'])
            self.assertIn('超时', result.json()['message'])
        finally:
            release.set()

    async def test_real_proxy_isolation_rebinding_and_active_stream_survives(self):
        release = asyncio.Event()
        url_a, seen_a = await self.proxy_server('A', release)
        url_b, seen_b = await self.proxy_server('B')
        a = await credential_proxy.save_proxy(self.db, 'A', url_a)
        b = await credential_proxy.save_proxy(self.db, 'B', url_b)
        await credential_proxy.bind_proxy(self.db, 'a@example.invalid', a['id'])
        await credential_proxy.bind_proxy(self.db, 'b@example.invalid', b['id'])
        async def stream():
            return [c async for c in httpx_client.stream_post_async('http://upstream.invalid/generate', {}, native=True, credential_email='a@example.invalid')]
        task = asyncio.create_task(stream())
        try:
            self.assertIn(b'http://upstream.invalid/generate', await asyncio.wait_for(seen_a.get(), 3))
            response = await httpx_client.post_async('http://upstream.invalid/generate', json={}, credential_email='b@example.invalid', timeout=3)
            self.assertEqual(response.text, 'B')
            await credential_proxy.bind_proxy(self.db, 'a@example.invalid', b['id'])
            response = await httpx_client.get_async('http://upstream.invalid/models', credential_email='a@example.invalid', timeout=3)
            self.assertEqual(response.text, 'B')
            self.assertFalse(task.done())
            release.set()
            self.assertEqual(await asyncio.wait_for(task, 3), [b'A'])
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_replacement_between_panel_admission_and_send_cannot_receive_old_bearer(self):
        email = 'a@example.invalid'
        data = {'access_token': 'old', 'refresh_token': 'old-rt', 'expiry': '2099-01-01T00:00:00Z', 'project_id': 'p'}
        await self.db.store_credential(email, data)
        url, seen = await self.proxy_server('{"candidates":[{"content":{"parts":[{"text":"OK"}]}}]}')
        proxy = await credential_proxy.save_proxy(self.db, 'new route', url)
        async def replace(*args):
            await self.db.store_credential(email, dict(data, access_token='new', refresh_token='new-rt'))
            await credential_proxy.bind_proxy(self.db, email, proxy['id'])
            return AsyncMock()
        with patch.object(panel_creds, 'get_database', AsyncMock(return_value=self.db)), patch.object(
            panel_creds, 'get_antigravity_api_url', AsyncMock(return_value='http://upstream.invalid')
        ), patch.object(panel_creds, 'acquire_test_dispatch', replace):
            with self.assertRaises(HTTPException) as error:
                await panel_creds.test_credential(email, 'gemini-flash', 'test')
        self.assertEqual(error.exception.status_code, 409)
        self.assertTrue(seen.empty())

    async def test_removed_credential_cannot_dispatch_over_global_proxy(self):
        url, seen = await self.proxy_server('global')
        await self.db.delete_credential('a@example.invalid')
        with patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value=url)):
            with self.assertRaises(httpx.TransportError):
                await httpx_client.post_async('http://upstream.invalid/generate', credential_email='a@example.invalid')
        self.assertTrue(seen.empty())

    async def test_json_reimport_refresh_uses_bound_proxy_and_new_import_uses_global(self):
        bound, bound_seen = await self.proxy_server('{"access_token":"bound","refresh_token":"rotated","expires_in":3600}')
        default, default_seen = await self.proxy_server('{"access_token":"default","expires_in":3600}')
        proxy = await credential_proxy.save_proxy(self.db, 'bound', bound)
        await credential_proxy.bind_proxy(self.db, 'a@example.invalid', proxy['id'])
        with patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value=default)), patch(
            'src.google_oauth_api.get_oauth_proxy_url', AsyncMock(return_value='http://oauth.invalid')
        ), patch.object(panel_creds, 'add_credential', AsyncMock()) as save:
            for email, expected in [('a@example.invalid', 'bound'), ('new@example.invalid', 'default')]:
                result = await panel_creds._import_single_credential(dict(
                    email=email, refresh_token='synthetic', tier='ultra', project_id='p'))
                self.assertEqual(result['status'], 'success')
                self.assertEqual(save.await_args.args[1]['access_token'], expected)
                self.assertEqual(save.await_args.args[1]['refresh_token'], 'rotated' if expected == 'bound' else 'synthetic')
        self.assertEqual((bound_seen.qsize(), default_seen.qsize()), (1, 1))

    async def test_global_inheritance_oauth_refresh_and_no_direct_fallback(self):
        url, seen = await self.proxy_server('{"access_token":"renewed","expires_in":3600}')
        proxy = await credential_proxy.save_proxy(self.db, 'Auth', url)
        await credential_proxy.bind_proxy(self.db, 'a@example.invalid', proxy['id'])
        creds = Credentials.from_dict(await self.db.get_credential('a@example.invalid'))
        creds.refresh_token = 'dummy'
        with patch('src.google_oauth_api.get_oauth_proxy_url', AsyncMock(return_value='http://oauth.invalid')):
            await creds.refresh()
        self.assertEqual(creds.access_token, 'renewed')
        self.assertIn(b'http://oauth.invalid/token', await seen.get())
        with patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value=url)):
            response = await httpx_client.get_async('http://global.invalid/', credential_email='b@example.invalid', timeout=3)
            self.assertEqual(response.status_code, 200)
        # Reserve then close a local port to obtain a definite broken proxy.
        server = await asyncio.start_server(lambda r, w: None, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        server.close(); await server.wait_closed()
        await credential_proxy.save_proxy(self.db, 'Auth', f'http://user:secret@127.0.0.1:{port}', proxy['id'])
        with self.assertRaises(httpx.ConnectError) as caught:
            await httpx_client.get_async(url, credential_email='a@example.invalid', timeout=1)
        self.assertNotIn('secret', str(caught.exception))

    async def test_bound_proxy_overrides_project_until_unbound(self):
        project_url, _ = await self.proxy_server('project')
        bound_url, _ = await self.proxy_server('credential')
        email = 'a@example.invalid'
        item = await credential_proxy.save_proxy(self.db, 'Dedicated proxy', bound_url)
        await credential_proxy.bind_proxy(self.db, email, item['id'])
        with patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value=project_url)):
            async def request():
                return await httpx_client.get_async('http://upstream.invalid/', credential_email=email, timeout=3)

            self.assertEqual((await request()).text, 'credential')
            await credential_proxy.bind_proxy(self.db, email, None)
            self.assertEqual((await request()).text, 'project')

    async def test_revision_migration_preserves_rows_and_fences_later_replacements(self):
        email = 'a@example.invalid'
        proxy = await credential_proxy.save_proxy(self.db, 'existing', 'http://localhost:1234')
        await credential_proxy.bind_proxy(self.db, email, proxy['id'])
        await self.db.update_credential_state(email, {'rpm_limit': 2, 'disabled': True})
        async with self.db._engine.begin() as c:
            await c.execute(text('ALTER TABLE antigravity_credentials DROP COLUMN credential_revision'))
            await c.run_sync(self.db._create_tables)
            await c.run_sync(self.db._create_tables)
        before = await self.db.get_credential(email)
        self.assertEqual(before['credential_revision'], '')
        self.assertTrue(await self.db.update_credential_state(email, {'error_codes': [502]}, expected_revision=''))
        await self.db.store_credential(email, dict(before, refresh_token='replacement'))
        self.assertFalse(await self.db.update_credential_state(email, {'error_codes': [403]}, expected_revision=''))
        state = (await self.db.get_credentials_summary())['items']
        account = next(r for r in state if r['email'] == email)
        self.assertEqual((account['note'], account['rpm_limit'], account['proxy_id'], account['disabled']),
                         ('A', 2, proxy['id'], True))

    async def test_schema_upgrade_preserves_existing_credential(self):
        async with self.db._engine.begin() as c:
            await c.execute(text('ALTER TABLE antigravity_credentials DROP COLUMN proxy_id'))
            await c.run_sync(self.db._create_tables)
        data = await self.db.get_credentials_summary()
        self.assertEqual(len(data['items']), 2)
        self.assertTrue(all(r['proxy_id'] is None for r in data['items']))
        self.assertEqual((await self.db.get_credential('a@example.invalid'))['note'], 'A')

    async def test_cancelled_request_releases_busy_pool_reference(self):
        async def cancelled_request():
            with CancelScope() as scope:
                async with httpx_client.get_client() as client:
                    lock = httpx_client._shared_clients_lock
                    await lock.acquire()
                    asyncio.get_running_loop().call_later(.05, lock.release)
                    scope.cancel()
                    await asyncio.sleep(0)
            return client
        client = await asyncio.wait_for(cancelled_request(), 2)
        self.assertEqual(httpx_client._client_users.get(client, 0), 0)

    async def test_idle_pool_eviction_never_closes_active_client(self):
        urls = [f'http://127.0.0.1:{30000 + i}' for i in range(3)]
        with patch.object(httpx_client, '_MAX_IDLE_POOLS', 1), patch.object(httpx_client, 'get_proxy_config', AsyncMock(side_effect=urls)):
            async with httpx_client.get_streaming_client() as active:
                async with httpx_client.get_client() as idle:
                    pass
                async with httpx_client.get_client() as newest:
                    pass
                self.assertTrue(idle.is_closed)
                self.assertFalse(active.is_closed)
                self.assertFalse(newest.is_closed)
