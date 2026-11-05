"""Panel persistence, validation, defaults and hot reload of handshake controls."""
import json
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
from fastapi import HTTPException

import config
from src import httpx_client
from src.models import ConfigSaveRequest
from src.panel import config_routes

ENABLED = 'stream_handshake_timeout_enabled'
SECONDS = 'stream_handshake_timeout_seconds'


class HandshakeSettingsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(os.environ, {'STREAM_HANDSHAKE_TIMEOUT_ENABLED':'', 'STREAM_HANDSHAKE_TIMEOUT_SECONDS':''}))
        self.stack.enter_context(patch.object(config, '_config_initialized', True))
        self.stack.enter_context(patch.object(config, '_config_cache', {}))
        self.stored = {}
        self.db = AsyncMock()
        async def read():
            return dict(self.stored)
        async def save(key, value):
            self.stored[key] = value
            return True
        self.db.get_all_config.side_effect = read
        self.db.set_config.side_effect = save
        self.stack.enter_context(patch.object(config_routes, 'get_database', AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch('src.storage.database.get_database', AsyncMock(return_value=self.db)))
        self.stack.enter_context(patch.object(httpx_client, 'get_proxy_config', AsyncMock(return_value='')))

    async def save(self, values):
        return await config_routes.save_config(ConfigSaveRequest(config=values), token='synthetic')

    async def test_defaults_are_enabled_40_seconds(self):
        self.assertTrue(await config.get_stream_handshake_timeout_enabled())
        self.assertEqual(await config.get_stream_handshake_timeout_seconds(), 40)
        data = json.loads((await config_routes.get_config(token='synthetic')).body)['config']
        self.assertIs(data[ENABLED], True)
        self.assertEqual(data[SECONDS], 40)

    async def test_save_read_reload_and_clients_use_new_settings_without_restart(self):
        await self.save({ENABLED:True, SECONDS:17})
        async with httpx_client.get_streaming_client(trust_env=False) as old:
            self.assertEqual(old.handshake_timeout, 17)
            await self.save({ENABLED:False})
            async with httpx_client.get_streaming_client(trust_env=False) as disabled:
                self.assertIsNone(disabled.handshake_timeout)
                self.assertIsNone(disabled.timeout.connect)
            self.assertEqual(old.handshake_timeout, 17)
            self.assertEqual(self.stored[SECONDS], 17)
            await self.save({ENABLED:True, SECONDS:25})
            async with httpx_client.get_streaming_client(trust_env=False) as new:
                self.assertEqual(new.handshake_timeout, 25)
                self.assertEqual(new.timeout.connect, 25)
                self.assertIsNone(new.timeout.read)
        data = json.loads((await config_routes.get_config(token='synthetic')).body)['config']
        self.assertEqual((data[ENABLED],data[SECONDS]), (True,25))
        config._config_cache.clear()
        await config.reload_config()
        self.assertEqual(await config.get_stream_handshake_timeout_seconds(), 25)

    async def test_invalid_seconds_rejected_before_any_write(self):
        for value in (0, -1, 3601, True, 2.5, '40', None):
            with self.subTest(value=value), self.assertRaises(HTTPException) as caught:
                await self.save({ENABLED:False, SECONDS:value})
            self.assertEqual(caught.exception.status_code, 400)
        self.db.set_config.assert_not_awaited()

    async def test_invalid_switch_rejected(self):
        for value in ('false', 0, None):
            with self.subTest(value=value), self.assertRaises(HTTPException):
                await self.save({ENABLED:value})
        self.db.set_config.assert_not_awaited()

    async def test_environment_override_and_lock(self):
        with patch.dict(os.environ, {'STREAM_HANDSHAKE_TIMEOUT_ENABLED':'false', 'STREAM_HANDSHAKE_TIMEOUT_SECONDS':'80'}):
            self.assertFalse(await config.get_stream_handshake_timeout_enabled())
            self.assertEqual(await config.get_stream_handshake_timeout_seconds(), 80)
            await self.save({ENABLED:True, SECONDS:20})
            self.db.set_config.assert_not_awaited()
            data = json.loads((await config_routes.get_config(token='synthetic')).body)
            self.assertIn(ENABLED, data['env_locked'])
            self.assertIn(SECONDS, data['env_locked'])
            self.assertEqual(data['config'][SECONDS], 80)

    async def test_invalid_environment_seconds_fall_back_to_40(self):
        for value in ('0', '-1', 'nan', 'inf', 'abc', '3601'):
            with patch.dict(os.environ, {'STREAM_HANDSHAKE_TIMEOUT_SECONDS':value}):
                self.assertEqual(await config.get_stream_handshake_timeout_seconds(), 40)
