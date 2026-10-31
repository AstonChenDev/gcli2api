import unittest
from sqlalchemy import delete
from src.storage.database import UsageLog
from src.panel import usage_logs
import test_credential_usage


class OverallUsageTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = test_credential_usage.CredentialUsageTests.asyncTearDown

    async def asyncSetUp(self):
        await test_credential_usage.CredentialUsageTests.asyncSetUp(self)
        async with self.db.session() as s:
            await s.execute(delete(UsageLog))
            for id_, rid, model, ok, code, msg, ts in [
                (1, 'r1', 'medium', False, 429, 'QUOTA_EXHAUSTED', 100),
                (2, 'r1', 'high', True, 200, '', 101),
                (3, 'r2', 'medium', False, 400, '{"error":{"blockReason":"PROHIBITED_CONTENT"}}', 102),
                (4, 'r3', 'medium', False, 499, 'CLIENT_DISCONNECTED', 103),
                (5, 'r4', 'medium', True, 200, '', 104),
                (6, 'r4', 'medium', False, 500, 'UPSTREAM_INCOMPLETE_STREAM', 105),
                (700, '', 'medium', True, 200, '', 106),
                (701, '700', 'medium', False, 200, 'Legacy stream failure', 107),
                (702, '', 'medium', False, 0, '', 108),
            ]:
                s.add(UsageLog(id=id_, request_id=rid, model_name='alias', upstream_model_name=model,
                    credential_email='deleted@example.invalid' if id_ % 2 else '', success=ok,
                    status_code=code, error_message=msg, created_at=ts))

    async def test_retry_dedup_latest_result_and_legacy_ids(self):
        response = await self.client.get('/usage-logs/overall-stats')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        result = response.json()
        self.assertEqual(result['requests']['summary'], dict(total_requests=7, success_count=2, failure_count=5, success_rate=28.57))
        self.assertEqual(result['attempts']['summary'], dict(total_requests=9, success_count=3, failure_count=6, success_rate=33.33))
        self.assertNotIn(429, [r['status_code'] for r in result['requests']['error_codes']])
        self.assertIn(429, [r['status_code'] for r in result['attempts']['error_codes']])
        for scope in ('requests', 'attempts'):
            data = result[scope]
            self.assertEqual(sum(r['count'] for r in data['error_codes']), data['summary']['failure_count'])
            self.assertEqual(sum(r['total_requests'] for r in data['by_model']), data['summary']['total_requests'])

    async def test_filters_do_not_resurrect_old_retry_failures(self):
        result = await self.db.overall_usage_stats(end_timestamp=100)
        self.assertEqual(result['requests']['summary']['total_requests'], 0)
        self.assertEqual(result['attempts']['summary']['failure_count'], 1)
        result = await self.db.overall_usage_stats(model_name='medium')
        self.assertEqual(result['requests']['summary']['total_requests'], 6)
        self.assertEqual(result['requests']['summary']['success_count'], 1)
        self.assertEqual(result['attempts']['summary']['total_requests'], 8)
        self.assertNotIn(429, [r['status_code'] for r in result['requests']['error_codes']])

    async def test_all_error_messages_code_filter_and_pagination(self):
        result = (await self.client.get('/usage-logs/overall-errors')).json()
        self.assertEqual(result['total'], 5)
        self.assertEqual(sum(r['count'] for r in result['items']), 5)
        self.assertNotIn('QUOTA_EXHAUSTED', str(result))
        attempts = (await self.client.get('/usage-logs/overall-errors', params={'scope':'attempts', 'status_code':429})).json()
        self.assertEqual(attempts['items'][0]['error_message'], 'QUOTA_EXHAUSTED')
        first = await self.db.overall_usage_errors(limit=1)
        second = await self.db.overall_usage_errors(limit=1, offset=1)
        self.assertEqual(first['total'], 5)
        self.assertNotEqual(first['items'], second['items'])
        unknown = await self.db.overall_usage_errors(status_code=0)
        self.assertEqual(unknown['items'][0]['count'], 1)
        self.assertEqual(unknown['items'][0]['error_message'], '')

    async def test_empty_scope_has_no_fake_success_rate(self):
        result = await self.db.overall_usage_stats(start_timestamp=200)
        for scope in ('requests','attempts'):
            self.assertIsNone(result[scope]['summary']['success_rate'])
            self.assertEqual(result[scope]['error_codes'], [])
            self.assertEqual(result[scope]['by_model'], [])

    async def test_validation_and_auth(self):
        for path, params in [('/usage-logs/overall-stats', {'start_timestamp':200, 'end_timestamp':100}),
            ('/usage-logs/overall-errors', {'scope':'invalid'}), ('/usage-logs/overall-errors', {'limit':101}),
            ('/usage-logs/overall-errors', {'offset':-1})]:
            response = await self.client.get(path, params=params)
            self.assertIn(response.status_code, (400,422))
        self.app.dependency_overrides.clear()
        for path in ('/usage-logs/overall-stats','/usage-logs/overall-errors'):
            response = await self.client.get(path)
            self.assertIn(response.status_code, (401,403))
