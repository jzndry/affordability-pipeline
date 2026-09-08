from functools import lru_cache

from app.brokers.base import CapacityError, JobBroker, JobStatusResponse
from app.brokers.events import TERMINAL_EVENTS, PipelineEvent, PipelineEventName
from app.config import settings

__all__ = [
    "CapacityError",
    "JobBroker",
    "JobStatusResponse",
    "PipelineEvent",
    "PipelineEventName",
    "TERMINAL_EVENTS",
    "get_broker",
]


@lru_cache(maxsize=1)
def get_broker() -> JobBroker:
    if settings.JOB_BROKER == "inprocess":
        from app.brokers.inprocess import InProcessBroker

        return InProcessBroker()
    from app.brokers.celery_redis import CeleryRedisBroker

    return CeleryRedisBroker()
