from typing import AsyncIterator

from fastapi.testclient import TestClient

from app.brokers import get_broker
from app.brokers.base import CapacityError
from app.brokers.events import PipelineEvent
from app.config import settings
from app.main import app

client = TestClient(app)

_MINIMAL_PAYLOAD = {
    "statement_id": "s1",
    "account_holder": "A",
    "account_number": "1",
    "sort_code": "40-00-01",
    "transactions": [
        {
            "id": "t1",
            "date": "2026-08-01",
            "raw_description": "EMPLOYER SALARY BGC",
            "amount": "3000.00",
        }
    ],
}


class _AtCapacityBroker:
    async def submit(self, payload) -> str:
        raise CapacityError("assessment capacity reached")

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:  # pragma: no cover
        if False:
            yield None

    async def status(self, job_id):  # pragma: no cover
        raise NotImplementedError


def test_ingest_returns_429_with_retry_after_when_broker_at_capacity():
    app.dependency_overrides[get_broker] = lambda: _AtCapacityBroker()
    try:
        r = client.post("/api/v1/statements/ingest", json=_MINIMAL_PAYLOAD)
    finally:
        app.dependency_overrides.clear()
    assert r.status_code == 429
    assert "Retry-After" in r.headers
    assert r.headers["Retry-After"] == "10"


def test_health_reports_mode(monkeypatch):
    monkeypatch.setattr(settings, "JOB_BROKER", "inprocess")
    assert client.get("/health").json() == {"status": "healthy", "mode": "lite"}
    monkeypatch.setattr(settings, "JOB_BROKER", "celery")
    assert client.get("/health").json()["mode"] == "distributed"


def test_oversize_body_rejected(monkeypatch):
    monkeypatch.setattr(settings, "MAX_REQUEST_BYTES", 1000)
    big = {"blob": "x" * 5000}
    r = client.post("/api/v1/statements/ingest", json=big)
    assert r.status_code == 413
    assert r.json() == {"detail": "Request body too large."}


def test_cors_allows_configured_origin_not_wildcard():
    # App is imported with the default CORS_ALLOW_ORIGINS="http://localhost:5173".
    ok = client.get("/health", headers={"Origin": "http://localhost:5173"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:5173"
    other = client.get("/health", headers={"Origin": "http://evil.example"})
    assert other.headers.get("access-control-allow-origin") not in {"*", "http://evil.example"}


def test_ingest_route_carries_both_rate_limits():
    # NOTE: slowapi binds @limiter.limit(...) values at decoration time, so a runtime
    # monkeypatch of the limit string cannot change an already-decorated route, and the
    # app cannot be cheaply rebuilt in-process. Per plan RULING 3 this asserts the config
    # values are correct; full limiter enforcement is verified manually (task brief Step 5).
    assert settings.INGEST_PER_IP_LIMIT == "5/minute"
    assert settings.INGEST_GLOBAL_LIMIT == "60/minute"
