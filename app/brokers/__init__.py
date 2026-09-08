from functools import lru_cache

from app.brokers.base import CapacityError, JobBroker, JobStatusResponse
from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import TERMINAL_EVENTS, PipelineEvent, PipelineEventName
from app.brokers.inprocess import InProcessBroker
from app.config import settings

__all__ = [
    "CapacityError",
    "CeleryRedisBroker",
    "InProcessBroker",
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
        return InProcessBroker()
    return CeleryRedisBroker()
