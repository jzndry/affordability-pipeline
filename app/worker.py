import itertools
import json
from typing import Any, Dict

import redis
from celery import Celery

from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline

celery_app = Celery(
    "underwriting_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

redis_client = redis.Redis.from_url(settings.REDIS_URL)


def _publish(job_id: str, seq: "itertools.count[int]", event: PipelineEvent) -> None:
    event.job_id = job_id
    event.seq = next(seq)
    body = json.dumps(event.model_dump(mode="json"))
    try:
        pipe = redis_client.pipeline()
        pipe.rpush(f"underwriting_events:{job_id}", body)
        pipe.expire(f"underwriting_events:{job_id}", settings.EVENT_LOG_TTL_SECONDS)
        pipe.publish(f"underwriting_jobs:{job_id}", body)
        pipe.execute()
    except Exception:  # noqa: BLE001 - Redis must never crash the worker task
        pass


@celery_app.task(name="tasks.process_affordability_assessment", bind=True)  # type: ignore[untyped-decorator]
def process_affordability_assessment(self, payload_dict: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
    job_id: str = self.request.id
    seq: "itertools.count[int]" = itertools.count()

    def emit(event: PipelineEvent) -> None:
        _publish(job_id, seq, event)

    statement = BankStatementPayload.model_validate(payload_dict)
    try:
        assessment = run_underwriting_pipeline(statement, emit)
    except Exception as exc:  # noqa: BLE001 - surface failure as a terminal event
        emit(PipelineEvent(event="FAILED", error=str(exc)))
        raise
    return assessment.model_dump(mode="json")
