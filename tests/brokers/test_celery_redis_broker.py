import pytest
import redis.asyncio as aioredis

from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import PipelineEvent
from app.config import settings
from app.worker import celery_app


class _FakeAsyncResult:
    def __init__(self, state: str, result: object = None, info: object = None) -> None:
        self.state = state
        self.result = result
        self.info = info


@pytest.fixture
async def redis_conn():
    conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    await conn.flushdb()
    yield conn
    await conn.flushdb()
    await conn.aclose()


async def _seed(conn, job_id: str, events: list[PipelineEvent]) -> None:
    import json

    for e in events:
        body = json.dumps(e.model_dump(mode="json"))
        await conn.rpush(f"underwriting_events:{job_id}", body)


async def test_subscribe_replays_completed_job(redis_conn):
    job_id = "job-replay-1"
    await _seed(
        redis_conn,
        job_id,
        [
            PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
            PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
            PipelineEvent(event="SCORING", job_id=job_id, seq=2),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={"decision": "APPROVED"}),
        ],
    )
    broker = CeleryRedisBroker()
    received = [e.event async for e in broker.subscribe(job_id)]
    assert received == ["RECEIVED", "CATEGORISING", "SCORING", "DECIDED"]


async def test_subscribe_tails_live_events_after_replay(redis_conn):
    job_id = "job-live-1"
    await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
    broker = CeleryRedisBroker()

    import asyncio
    import json

    async def publish_later():
        await asyncio.sleep(0.1)
        for e in [
            PipelineEvent(event="SCORING", job_id=job_id, seq=1),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=2, data={"decision": "REFERRED"}),
        ]:
            await redis_conn.publish(
                f"underwriting_jobs:{job_id}", json.dumps(e.model_dump(mode="json"))
            )

    task = asyncio.create_task(publish_later())
    received = [e.event async for e in broker.subscribe(job_id)]
    await task
    assert received == ["RECEIVED", "SCORING", "DECIDED"]


async def test_subscribe_dedupes_by_seq(redis_conn):
    job_id = "job-dupe-1"
    ev = [
        PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
        PipelineEvent(event="DECIDED", job_id=job_id, seq=1, data={}),
    ]
    await _seed(redis_conn, job_id, ev)
    broker = CeleryRedisBroker()

    import asyncio
    import json

    async def republish_seq1():
        await asyncio.sleep(0.05)
        await redis_conn.publish(
            f"underwriting_jobs:{job_id}", json.dumps(ev[1].model_dump(mode="json"))
        )

    asyncio.create_task(republish_seq1())
    received = [e.seq async for e in broker.subscribe(job_id)]
    assert received == [0, 1]


async def test_subscribe_reconciles_silent_worker_success(redis_conn, monkeypatch):
    monkeypatch.setattr("app.brokers.celery_redis.SILENCE_RECONCILE_SECONDS", 0.1)
    monkeypatch.setattr("app.brokers.celery_redis.POLL_TIMEOUT_SECONDS", 0.05)
    job_id = "job-silent-success"
    await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
    result_dict = {"decision": "APPROVED", "score": 0.91}
    monkeypatch.setattr(
        celery_app, "AsyncResult", lambda jid: _FakeAsyncResult("SUCCESS", result=result_dict)
    )
    broker = CeleryRedisBroker()

    events = [e async for e in broker.subscribe(job_id)]

    assert [e.event for e in events] == ["RECEIVED", "DECIDED"]
    assert events[-1].data == result_dict
    assert events[-1].job_id == job_id
    assert events[-1].is_terminal


async def test_subscribe_reconciles_silent_worker_failure(redis_conn, monkeypatch):
    monkeypatch.setattr("app.brokers.celery_redis.SILENCE_RECONCILE_SECONDS", 0.1)
    monkeypatch.setattr("app.brokers.celery_redis.POLL_TIMEOUT_SECONDS", 0.05)
    job_id = "job-silent-failure"
    await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
    monkeypatch.setattr(
        celery_app,
        "AsyncResult",
        lambda jid: _FakeAsyncResult("FAILURE", info=RuntimeError("worker died")),
    )
    broker = CeleryRedisBroker()

    events = [e async for e in broker.subscribe(job_id)]

    assert [e.event for e in events] == ["RECEIVED", "FAILED"]
    assert events[-1].error == "worker died"
    assert events[-1].job_id == job_id
    assert events[-1].is_terminal


async def test_subscribe_keeps_tailing_while_celery_still_running(redis_conn, monkeypatch):
    monkeypatch.setattr("app.brokers.celery_redis.SILENCE_RECONCILE_SECONDS", 0.1)
    monkeypatch.setattr("app.brokers.celery_redis.POLL_TIMEOUT_SECONDS", 0.05)
    job_id = "job-still-running"
    await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
    monkeypatch.setattr(celery_app, "AsyncResult", lambda jid: _FakeAsyncResult("STARTED"))
    broker = CeleryRedisBroker()

    import asyncio
    import json

    async def publish_later():
        await asyncio.sleep(0.4)  # several reconcile cycles pass first
        e = PipelineEvent(event="DECIDED", job_id=job_id, seq=1, data={"decision": "REFERRED"})
        await redis_conn.publish(
            f"underwriting_jobs:{job_id}", json.dumps(e.model_dump(mode="json"))
        )

    task = asyncio.create_task(publish_later())
    received = [e.event async for e in broker.subscribe(job_id)]
    await task

    assert received == ["RECEIVED", "DECIDED"]
