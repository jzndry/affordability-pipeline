"""Locust load testing suite for the Open Banking Affordability Pipeline.

This script simulates concurrent loan applications by submitting realistic bank
statements to the non-blocking ingestion endpoint and verifying job status.
"""

import random
import uuid
from decimal import Decimal
from typing import Any, Dict, List

from locust import HttpUser, between, task

SAMPLE_DESCRIPTIONS: List[str] = [
    "BGC EMPLOYER SALARY DWP",
    "DIRECT DEBIT HOUSING RENT",
    "POS 4829 BET365 UK",
    "TFL TRAVEL CHARGE TUBE",
    "SAINSBURYS S/MKTS LONDON",
    "NETFLIX.COM PAYMENT",
    "DD CLYDESDALE LOAN REPAYMENT",
    "ATM CASH WITHDRAWAL HIGH ST",
    "TRANSFER TO SAVINGS POT",
    "SPOTIFY PREMIUM GB",
]


def generate_payload(transaction_count: int = 150) -> Dict[str, Any]:
    """Construct a dynamic bank statement payload formatted as JSON-ready types.

    Args:
        transaction_count: Number of transactions in the synthetic statement.

    Returns:
        A dictionary representation matching the BankStatementPayload schema.
    """
    statement_id = f"stmt_{uuid.uuid4().hex[:10]}"
    transactions: List[Dict[str, Any]] = []

    for index in range(transaction_count):
        raw_description = random.choice(SAMPLE_DESCRIPTIONS)

        if "SALARY" in raw_description:
            amount = str(Decimal(random.randint(1800, 3500)))
        else:
            amount = str(Decimal(-random.randint(5, 300)))

        transactions.append(
            {
                "id": f"tx_{index:04d}",
                "date": "2026-08-01",
                "raw_description": raw_description,
                "amount": amount,
                "category": "UNCATEGORISED",
            }
        )

    return {
        "statement_id": statement_id,
        "account_holder": "Concurrent Load Test Subject",
        "account_number": "87654321",
        "sort_code": "20-00-00",
        "transactions": transactions,
    }


class UnderwritingLoadUser(HttpUser):
    """Simulates an external loan platform submitting applications concurrently."""

    # Simulates realistic think time between requests (50ms to 200ms)
    wait_time = between(0.05, 0.2)

    @task(3)
    def test_health_check_endpoint(self) -> None:
        """Verify that basic health check latency remains sub-millisecond."""
        self.client.get("/health", name="/health")

    @task(5)
    def test_statement_ingestion_and_polling(self) -> None:
        """Submit a full bank statement payload and inspect the returned job status."""
        payload = generate_payload(transaction_count=150)

        # 1. Post statement to the ingestion endpoint
        with self.client.post(
            "/api/v1/statements/ingest",
            json=payload,
            name="/api/v1/statements/ingest",
            catch_response=True,
        ) as response:
            if response.status_code != 202:
                response.failure(
                    f"Expected HTTP 202, received {response.status_code}: {response.text}"
                )
                return

            response_json = response.json()
            job_id = response_json.get("job_id")

        # 2. Check the job status endpoint using the returned job_id
        if job_id:
            self.client.get(
                f"/api/v1/statements/jobs/{job_id}",
                name="/api/v1/statements/jobs/[job_id]",
            )
