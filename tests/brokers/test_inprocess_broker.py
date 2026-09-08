import asyncio

import pytest

from app.brokers.base import CapacityError
from app.brokers.inprocess import InProcessBroker
from app.core.models import BankStatementPayload

APPROVE = {
    "statement_id": "s1", "account_holder": "A", "account_number": "1", "sort_code": "40-00-01",
    "transactions": [
        {"id": "t1", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "3000.00"},
        {"id": "t2", "date": "2026-08-02", "raw_description": "RENT LANDLORD", "amount": "-700.00"},
    ],
}


def _payload() -> BankStatementPayload:
    return BankStatementPayload.model_validate(APPROVE)


async def test_full_sequence_streamed_to_a_live_subscriber():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    events = [e.event async for e in broker.subscribe(job_id)]
    assert events == ["RECEIVED", "CATEGORISING", "CATEGORISING", "SCORING", "DECIDED"]


async def test_late_subscriber_still_gets_full_replay():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    await asyncio.sleep(0.05)  # let the job finish first
    events = [e.event async for e in broker.subscribe(job_id)]
    assert events[0] == "RECEIVED" and events[-1] == "DECIDED"


async def test_seq_numbers_are_monotonic():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    seqs = [e.seq async for e in broker.subscribe(job_id)]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


async def test_capacity_error_when_at_limit(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "MAX_CONCURRENT_ASSESSMENTS", 1)
    broker = InProcessBroker()
    broker._active = 1  # simulate one in flight
    with pytest.raises(CapacityError):
        await broker.submit(_payload())


async def test_concurrent_submits_are_capped_at_the_limit(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "MAX_CONCURRENT_ASSESSMENTS", 5)
    broker = InProcessBroker()

    results = await asyncio.gather(
        *(broker.submit(_payload()) for _ in range(8)),
        return_exceptions=True,
    )

    admitted = [r for r in results if isinstance(r, str)]
    rejected = [r for r in results if isinstance(r, CapacityError)]
    assert len(admitted) == 5
    assert len(rejected) == 3

    # let the in-flight jobs drain so _active returns to zero
    for job_id in admitted:
        async for _ in broker.subscribe(job_id):
            pass
    assert broker._active == 0


async def test_failed_event_emitted_on_pipeline_error(monkeypatch):
    broker = InProcessBroker()

    def boom(payload, emit):
        raise ValueError("categoriser exploded")

    monkeypatch.setattr("app.brokers.inprocess.run_underwriting_pipeline", boom)
    job_id = await broker.submit(_payload())
    events = [(e.event, e.error) async for e in broker.subscribe(job_id)]
    assert events[-1][0] == "FAILED"
    assert "exploded" in events[-1][1]


async def test_status_reports_success_after_completion():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    async for _ in broker.subscribe(job_id):
        pass
    status = await broker.status(job_id)
    assert status.status == "SUCCESS"
    assert status.result is not None and status.result["decision"] == "APPROVED"
