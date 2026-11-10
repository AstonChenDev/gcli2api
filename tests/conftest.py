"""Legacy transport tests use synthetic credentials, never the operator's DB.

The dedicated cooldown suite exercises the real admission path and SQL store.
Other suites continue to isolate transport/conversion behavior from persistence.
"""
from unittest.mock import patch

import pytest


class TransportOnlyLease:
    async def result(self, **kwargs):
        pass

    async def close(self):
        pass

    def iterate(self, stream):
        return stream


@pytest.fixture(autouse=True)
def isolate_legacy_transport_tests(request):
    if request.node.path.name.startswith("test_cooldown_499"):
        yield
        return

    async def admit(candidate, *args):
        return candidate, TransportOnlyLease()

    async def admit_test(*args):
        return TransportOnlyLease()

    with patch("src.api.antigravity.acquire_dispatch", admit), patch("src.panel.creds.acquire_test_dispatch", admit_test):
        yield
