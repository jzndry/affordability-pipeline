from httpx import ASGITransport, AsyncClient

from app.brokers import get_broker
from app.main import app


class _StubBroker:
    async def submit(self, payload) -> str:
        return "stub-job-123"

    async def subscribe(self, job_id):  # pragma: no cover
        if False:
            yield None

    async def status(self, job_id):  # pragma: no cover
        raise NotImplementedError


async def test_non_blocking_ingestion_endpoint():
    """
    Verifies that statement ingestion returns HTTP 202 Accepted with a job_id
    without blocking the main event loop.
    """
    app.dependency_overrides[get_broker] = lambda: _StubBroker()
    payload = {
        "statement_id": "stmt_async_100",
        "account_holder": "Jane Doe",
        "account_number": "87654321",
        "sort_code": "40-00-01",
        "transactions": [
            {"id": "tx_101", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "2800.00"},
            {"id": "tx_102", "date": "2026-08-02", "raw_description": "POS 4829 BET365", "amount": "-50.00"},
        ],
    }
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/api/v1/statements/ingest", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    body = response.json()
    assert body["job_id"] == "stub-job-123"
    assert body["status"] == "PENDING"
    assert "enqueued" in body["message"]
