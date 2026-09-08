import time
from typing import Any, AsyncIterator

import redis.asyncio as aioredis

from app.brokers.base import JobStatusResponse
from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.worker import celery_app, process_affordability_assessment

# Seconds to wait on each pub/sub poll before looping.
POLL_TIMEOUT_SECONDS = 0.5
# After this much pub/sub silence, reconcile the job against Celery so a
# hard-killed worker (SIGKILL / OOM / eviction) that never published FAILED
# cannot pin the generator forever.
SILENCE_RECONCILE_SECONDS = 60.0


def _terminal_event_from_status(
    job_id: str, status: JobStatusResponse, seq: int
) -> PipelineEvent:
    """Synthesize a terminal PipelineEvent from a terminal Celery status."""
    if status.status == "SUCCESS":
        return PipelineEvent(event="DECIDED", job_id=job_id, seq=seq, data=status.result)
    return PipelineEvent(event="FAILED", job_id=job_id, seq=seq, error=status.error)


class CeleryRedisBroker:
    """JobBroker backed by the Celery worker and Redis list + pub/sub."""

    async def submit(self, payload: BankStatementPayload) -> str:
        task = process_affordability_assessment.delay(payload.model_dump(mode="json"))
        return str(task.id)

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        pubsub = conn.pubsub()
        channel = f"underwriting_jobs:{job_id}"
        seen: set[int] = set()
        max_seq = -1
        try:
            await pubsub.subscribe(channel)  # subscribe before replay to avoid a gap

            for raw in await conn.lrange(f"underwriting_events:{job_id}", 0, -1):
                event = PipelineEvent.model_validate_json(raw)
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                max_seq = max(max_seq, event.seq)
                yield event
                if event.is_terminal:
                    return

            last_activity = time.monotonic()
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=POLL_TIMEOUT_SECONDS
                )
                if message is None or message.get("type") != "message":
                    if time.monotonic() - last_activity >= SILENCE_RECONCILE_SECONDS:
                        last_activity = time.monotonic()
                        reconciled = await self.status(job_id)
                        if reconciled.status in ("SUCCESS", "FAILURE"):
                            synthetic = _terminal_event_from_status(
                                job_id, reconciled, max_seq + 1
                            )
                            if synthetic.seq not in seen:
                                seen.add(synthetic.seq)
                                yield synthetic
                            return
                    continue
                event = PipelineEvent.model_validate_json(message["data"])
                last_activity = time.monotonic()
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                max_seq = max(max_seq, event.seq)
                yield event
                if event.is_terminal:
                    return
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()  # type: ignore[no-untyped-call]
            await conn.aclose()

    async def status(self, job_id: str) -> JobStatusResponse:
        result = celery_app.AsyncResult(job_id)
        state = result.state
        if state == "SUCCESS":
            data: dict[str, Any] = result.result
            return JobStatusResponse(job_id=job_id, status="SUCCESS", result=data)
        if state == "FAILURE":
            return JobStatusResponse(job_id=job_id, status="FAILURE", error=str(result.info))
        if state == "PENDING":
            return JobStatusResponse(job_id=job_id, status="PENDING")
        return JobStatusResponse(job_id=job_id, status="STARTED")
