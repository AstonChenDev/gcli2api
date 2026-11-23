import unittest
from unittest.mock import AsyncMock, patch

from src import google_oauth_api as oauth


class CredentialTierTests(unittest.IsolatedAsyncioTestCase):
    async def test_project_lookup_preserves_upstream_lite_distinction(self):
        for raw, expected in [("g1-ultra-lite-tier", "ultra_lite"),
                              ("g1-ultra-tier", "ultra"),
                              ("g1-pro-tier", "pro"), (None, None)]:
            with self.subTest(raw=raw), patch.object(
                oauth, "_try_load_code_assist", AsyncMock(return_value=("project", raw))
            ):
                self.assertEqual(await oauth.fetch_project_id_and_tier(
                    "synthetic", "test-agent", "https://upstream.invalid"
                ), ("project", expected))
