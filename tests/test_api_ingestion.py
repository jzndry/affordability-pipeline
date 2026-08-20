from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_non_blocking_ingestion_endpoint():
    """
    Verifies that statement ingestion returns HTTP 202 Accepted
    with a job_id without blocking the main event loop.
    """
    sample_payload = {
        "statement_id": "stmt_async_100",
        "account_holder": "Jane Doe",
        "account_number": "87654321",
        "sort_code": "40-00-01",
        "transactions": [
            {
                "id": "tx_101",
                "date": "2026-08-01",
                "raw_description": "EMPLOYER SALARY BGC",
                "amount": "2800.00",
                "category": "UNCATEGORISED",
            },
            {
                "id": "tx_102",
                "date": "2026-08-02",
                "raw_description": "POS 4829 BET365",
                "amount": "-50.00",
                "category": "UNCATEGORISED",
            },
        ],
    }

    # Mock Celery delay method so tests do not require a live Redis instance running
    with patch("app.main.process_affordability_assessment.delay") as mock_delay:
        mock_task = MagicMock()
        mock_task.id = "mock-job-id-12345"
        mock_delay.return_value = mock_task

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/api/v1/statements/ingest", json=sample_payload)

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "PENDING"
    assert data["job_id"] == "mock-job-id-12345"
    assert "enqueued" in data["message"]
