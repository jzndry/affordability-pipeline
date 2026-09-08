"""Shared JobBroker contract.

Both broker implementations (`InProcessBroker`, `CeleryRedisBroker`) must satisfy
the same four delivery properties. They are asserted once here via `_assert_*`
helpers and exercised against each broker through a thin, mode-specific harness.

  1. a live subscriber gets the full ordered, de-duplicated sequence ending in a
     terminal event;
  2. a subscriber connecting AFTER completion still replays the full sequence;
  3. no duplicate `seq` across the replay -> live boundary;
  4. `subscribe` terminates in bounded time for a job that never produces a
     terminal event (via a synthesized terminal).
"""

import asyncio
import json

import pytest
import redis.asyncio as aioredis

from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import PipelineEvent
from app.brokers.inprocess import InProcessBroker
from app.config import settings
from app.core.models import BankStatementPayload

_PAYLOAD = BankStatementPayload.model_validate(
    {
        "statement_id": "s1",
        "account_holder": "A",
        "account_number": "1",
        "sort_code": "40-00-01",
        "transactions": [
            {"id": "t1", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "3000.00"},
            {"id": "t2", "date": "2026-08-02", "raw_description": "RENT LANDLORD", "amount": "-700.00"},
        ],
    }
)

_STAGES = ["RECEIVED", "CATEGORISING", "SCORING", "DECIDED"]


def _assert_full_ordered_terminal(events: list[PipelineEvent]) -> None:
    assert [e.event for e in events] == _STAGES
    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)  # property 3: no duplicate seq
    assert events[-1].is_terminal


def _assert_bounded_synthetic_terminal(events: list[PipelineEvent]) -> None:
    assert events, "stream produced nothing"
    assert events[-1].is_terminal
    assert events[-1].event == "FAILED"
    assert len(set(e.seq for e in events)) == len(events)


# --------------------------------------------------------------------------- #
# InProcessBroker
# --------------------------------------------------------------------------- #
def _emit_all_stages(payload, emit):
    for name in _STAGES:
        emit(PipelineEvent(event=name))


def _emit_partial_then_die(payload, emit):
    emit(PipelineEvent(event="RECEIVED"))
    emit(PipelineEvent(event="CATEGORISING"))
    raise asyncio.CancelledError()


class TestInProcessBrokerContract:
    async def test_live_subscriber_gets_full_ordered_sequence(self, monkeypatch):
        monkeypatch.setattr("app.brokers.inprocess.run_underwriting_pipeline", _emit_all_stages)
        broker = InProcessBroker()
        job_id = await broker.submit(_PAYLOAD)
        events = [e async for e in broker.subscribe(job_id)]
        _assert_full_ordered_terminal(events)

    async def test_late_subscriber_still_replays_full_sequence(self, monkeypatch):
        monkeypatch.setattr("app.brokers.inprocess.run_underwriting_pipeline", _emit_all_stages)
        broker = InProcessBroker()
        job_id = await broker.submit(_PAYLOAD)
        await broker._channels[job_id].done.wait()
        events = [e async for e in broker.subscribe(job_id)]
        _assert_full_ordered_terminal(events)

    async def test_subscribe_terminates_without_a_terminal_event(self, monkeypatch):
        monkeypatch.setattr(
            "app.brokers.inprocess.run_underwriting_pipeline", _emit_partial_then_die
        )
        broker = InProcessBroker()
        job_id = await broker.submit(_PAYLOAD)
        events = await asyncio.wait_for(
            _collect(broker.subscribe(job_id)), timeout=2.0
        )
        assert [e.event for e in events] == ["RECEIVED", "CATEGORISING", "FAILED"]
        _assert_bounded_synthetic_terminal(events)


async def _collect(stream):
    return [e async for e in stream]


# --------------------------------------------------------------------------- #
# CeleryRedisBroker
# --------------------------------------------------------------------------- #
@pytest.fixture
async def redis_conn():
    conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    await conn.flushdb()
    yield conn
    await conn.flushdb()
    await conn.aclose()


async def _seed(conn, job_id, events):
    for e in events:
        await conn.rpush(
            f"underwriting_events:{job_id}", json.dumps(e.model_dump(mode="json"))
        )


async def _publish(conn, job_id, events):
    for e in events:
        await conn.publish(
            f"underwriting_jobs:{job_id}", json.dumps(e.model_dump(mode="json"))
        )


class TestCeleryRedisBrokerContract:
    async def test_live_subscriber_gets_full_ordered_sequence(self, redis_conn):
        job_id = "contract-live"
        await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
        broker = CeleryRedisBroker()

        async def publisher():
            await asyncio.sleep(0.1)
            await _publish(
                redis_conn,
                job_id,
                [
                    PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
                    PipelineEvent(event="SCORING", job_id=job_id, seq=2),
                    PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={}),
                ],
            )

        task = asyncio.create_task(publisher())
        try:
            events = [e async for e in broker.subscribe(job_id)]
        finally:
            await task
        _assert_full_ordered_terminal(events)

    async def test_late_subscriber_still_replays_full_sequence(self, redis_conn):
        job_id = "contract-late"
        await _seed(
            redis_conn,
            job_id,
            [PipelineEvent(event=name, job_id=job_id, seq=i) for i, name in enumerate(_STAGES)],
        )
        broker = CeleryRedisBroker()
        events = [e async for e in broker.subscribe(job_id)]
        _assert_full_ordered_terminal(events)

    async def test_no_duplicate_seq_across_replay_live_boundary(self, redis_conn):
        job_id = "contract-dedupe"
        await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
        broker = CeleryRedisBroker()

        async def publisher():
            await asyncio.sleep(0.05)
            await _publish(
                redis_conn,
                job_id,
                [
                    PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),  # duplicate
                    PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
                    PipelineEvent(event="SCORING", job_id=job_id, seq=2),
                    PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={}),
                ],
            )

        task = asyncio.create_task(publisher())
        try:
            events = [e async for e in broker.subscribe(job_id)]
        finally:
            await task
        _assert_full_ordered_terminal(events)

    async def test_subscribe_terminates_without_a_terminal_event(self, redis_conn, monkeypatch):
        monkeypatch.setattr("app.brokers.celery_redis.MAX_SUBSCRIBE_SECONDS", 0.2)
        monkeypatch.setattr("app.brokers.celery_redis.POLL_TIMEOUT_SECONDS", 0.05)
        job_id = "contract-never-terminal"
        await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
        broker = CeleryRedisBroker()

        events = await asyncio.wait_for(_collect(broker.subscribe(job_id)), timeout=5.0)

        assert [e.event for e in events] == ["RECEIVED", "FAILED"]
        assert events[-1].error == "subscription timed out"
        _assert_bounded_synthetic_terminal(events)
