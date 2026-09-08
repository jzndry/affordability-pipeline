import pytest
import redis.asyncio as aioredis

from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import PipelineEvent
from app.config import settings


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
