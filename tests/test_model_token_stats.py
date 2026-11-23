import unittest
from sqlalchemy import delete
from src.storage.database import UsageLog
import test_credential_usage


class ModelTokenStatsTests(unittest.IsolatedAsyncioTestCase):
    asyncTearDown = test_credential_usage.CredentialUsageTests.asyncTearDown

    async def asyncSetUp(self):
        await test_credential_usage.CredentialUsageTests.asyncSetUp(self)
        async with self.db.session() as s:
            await s.execute(delete(UsageLog))
            for model, upstream, rid, email, ok, inp, out, cache, ts in [
                ('alias', 'gemini-4-carbon-high', 'retry', 'deleted@example.invalid', False, 80, 20, 40, 100),
                ('alias', 'gemini-4-hydrogen-high', 'retry', 'a@example.invalid', True, 200, 100, 50, 101),
                ('gemini-4-carbon-high', '', 'other', '', True, 50, 50, 0, 102),
                ('', '', '', '', False, 0, 0, 0, 103),
            ]:
                s.add(UsageLog(model_name=model, upstream_model_name=upstream, request_id=rid,
                    credential_email=email, success=ok, prompt_tokens=inp, completion_tokens=out,
                    cached_tokens=cache, total_tokens=inp+out, created_at=ts))

    async def test_consumption_keeps_failed_retry_and_deleted_accounts(self):
        response = await self.client.get('/usage-logs/model-tokens')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        data = response.json()
        self.assertEqual(data['summary']['total_tokens'], 500)
        self.assertEqual(data['summary']['cached_tokens'], 90)
        self.assertEqual(data['summary']['failed_tokens'], 100)
        self.assertEqual(data['summary']['positive_usage_attempts'], 3)
        self.assertEqual(data['summary']['attempts'], 4)
        self.assertEqual(data['summary']['first_request'], 100)
        self.assertEqual(data['summary']['last_request'], 103)
        carbon = next(r for r in data['items'] if r['model_name'] == 'gemini-4-carbon-high')
        self.assertEqual(carbon['total_tokens_percentage'], 40)
        self.assertAlmostEqual(sum(r['prompt_tokens_percentage'] for r in data['items']), 100, places=3)
        self.assertEqual(sum(r['total_tokens_percentage'] for r in data['items']), 100)
        self.assertIsNone(carbon['quota_percentage'])
        self.assertFalse(data['quota']['available'])

    async def test_filters_and_zero_denominators(self):
        data = await self.db.model_token_stats(start_timestamp=101, end_timestamp=102)
        self.assertEqual(data['summary']['total_tokens'], 400)
        only = await self.db.model_token_stats(credential_email='deleted@example.invalid')
        self.assertEqual(only['items'][0]['total_tokens_percentage'], 100)
        model = await self.db.model_token_stats(model_name='gemini-4-carbon-high')
        self.assertEqual(model['summary']['attempts'], 2)
        zero = await self.db.model_token_stats(start_timestamp=103)
        self.assertIsNone(zero['items'][0]['total_tokens_percentage'])
        empty = await self.db.model_token_stats(start_timestamp=104)
        self.assertEqual(empty['items'], [])
        self.assertIsNone(empty['summary']['first_request'])
        self.assertEqual(empty['summary']['total_tokens'], 0)

    async def test_not_capped_at_fifty_models(self):
        async with self.db.session() as s:
            for n in range(55):
                s.add(UsageLog(model_name=f'model-{n}', prompt_tokens=1, completion_tokens=1, created_at=200))
        data = await self.db.model_token_stats(start_timestamp=200)
        self.assertEqual(len(data['items']), 55)
        self.assertEqual(data['summary']['total_tokens'], 110)

    async def test_reported_total_and_fallback_preserve_upstream_differences(self):
        async with self.db.session() as s:
            s.add(UsageLog(model_name='reported', prompt_tokens=10, completion_tokens=20,
                           total_tokens=35, created_at=300))
            s.add(UsageLog(model_name='fallback', prompt_tokens=10, completion_tokens=20,
                           total_tokens=0, created_at=300))
        data = await self.db.model_token_stats(start_timestamp=300)
        self.assertEqual(data['summary']['total_tokens'], 65)
        self.assertEqual(data['summary']['total_difference_tokens'], 5)

    async def test_auth_and_invalid_ranges(self):
        for params in ({'start_timestamp': 200, 'end_timestamp': 100}, {'start_timestamp': 'nan'}, {'end_timestamp': 'inf'}):
            response = await self.client.get('/usage-logs/model-tokens', params=params)
            self.assertIn(response.status_code, (400, 422))
        self.app.dependency_overrides.clear()
        response = await self.client.get('/usage-logs/model-tokens')
        self.assertIn(response.status_code, (401, 403))
