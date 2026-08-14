import pytest

from src.request_context import ClientIPContextMiddleware, get_client_ip, resolve_client_ip
from src.stats_collector import StatsCollector
from src.storage.psql_manager import PSQLManager


def _scope(peer_ip: str, headers=None):
    return {
        "type": "http",
        "client": (peer_ip, 12345),
        "headers": headers or [],
    }


def test_resolve_client_ip_ignores_untrusted_forwarded_header(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    scope = _scope(
        "10.0.0.2",
        [(b"x-forwarded-for", b"198.51.100.8")],
    )

    assert resolve_client_ip(scope) == "10.0.0.2"


def test_resolve_client_ip_uses_header_from_trusted_proxy(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("TRUSTED_PROXY_IPS", "10.0.0.0/24")
    scope = _scope(
        "10.0.0.2",
        [(b"x-forwarded-for", b"198.51.100.8, 10.0.0.1")],
    )

    assert resolve_client_ip(scope) == "198.51.100.8"


def test_resolve_client_ip_rejects_spoofed_leftmost_forwarded_value(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("TRUSTED_PROXY_IPS", "10.0.0.0/24")
    scope = _scope(
        "10.0.0.2",
        [(b"x-forwarded-for", b"192.0.2.99, 198.51.100.8, 10.0.0.1")],
    )

    assert resolve_client_ip(scope) == "198.51.100.8"


@pytest.mark.asyncio
async def test_client_ip_context_covers_complete_http_request(monkeypatch):
    monkeypatch.delenv("TRUST_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    seen = []

    async def app(scope, receive, send):
        seen.append(get_client_ip())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        seen.append(get_client_ip())
        await send({"type": "http.response.body", "body": b""})

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        return None

    middleware = ClientIPContextMiddleware(app)
    await middleware(_scope("203.0.113.4"), receive, send)

    assert seen == ["203.0.113.4", "203.0.113.4"]
    assert get_client_ip() is None


def test_stats_collector_counts_ip_and_model():
    collector = StatsCollector()

    collector.record_request("gemini-2.5-flash", "geminicli", True, "203.0.113.4")
    collector.record_request("gemini-2.5-flash", "geminicli", False, "203.0.113.4")
    collector.record_request("gemini-2.5-pro", "geminicli", True, "203.0.113.4")

    assert collector._ip_request_counters == {
        ("203.0.113.4", "gemini-2.5-flash", "geminicli"): {
            "total": 2,
            "success": 1,
            "fail": 1,
        },
        ("203.0.113.4", "gemini-2.5-pro", "geminicli"): {
            "total": 1,
            "success": 1,
            "fail": 0,
        },
    }


@pytest.mark.asyncio
async def test_postgres_ip_summary_groups_models_under_each_ip():
    class FakeConnection:
        def __init__(self):
            self.calls = []

        async def fetch(self, query, *args):
            self.calls.append((query, args))
            if "COUNT(*) OVER()" in query:
                return [
                    {
                        "client_ip": "203.0.113.4",
                        "total": 3,
                        "success": 2,
                        "fail": 1,
                        "total_ips": 1,
                    }
                ]
            return [
                {
                    "client_ip": "203.0.113.4",
                    "model_name": "gemini-2.5-flash",
                    "total": 2,
                    "success": 1,
                    "fail": 1,
                },
                {
                    "client_ip": "203.0.113.4",
                    "model_name": "gemini-2.5-pro",
                    "total": 1,
                    "success": 1,
                    "fail": 0,
                },
            ]

    class AcquireContext:
        def __init__(self, connection):
            self.connection = connection

        async def __aenter__(self):
            return self.connection

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakePool:
        def __init__(self, connection):
            self.connection = connection

        def acquire(self):
            return AcquireContext(self.connection)

    connection = FakeConnection()
    manager = PSQLManager()
    manager._pool = FakePool(connection)
    manager._initialized = True

    result = await manager.get_ip_request_stats_summary(
        mode="geminicli",
        start_time=100,
        end_time=200,
        limit=10,
    )

    assert result == {
        "total_ips": 1,
        "limit": 10,
        "ips": [
            {
                "client_ip": "203.0.113.4",
                "total": 3,
                "success": 2,
                "fail": 1,
                "models": [
                    {
                        "model_name": "gemini-2.5-flash",
                        "total": 2,
                        "success": 1,
                        "fail": 1,
                    },
                    {
                        "model_name": "gemini-2.5-pro",
                        "total": 1,
                        "success": 1,
                        "fail": 0,
                    },
                ],
            }
        ],
    }
    assert connection.calls[0][1] == ("geminicli", 100, 200, 10)
    assert connection.calls[1][1] == ("geminicli", 100, 200, ["203.0.113.4"])
