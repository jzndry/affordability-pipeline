from app.brokers.base import CapacityError, JobBroker, JobStatusResponse
from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import TERMINAL_EVENTS, PipelineEvent, PipelineEventName

__all__ = [
    "CapacityError",
    "JobBroker",
    "JobStatusResponse",
    "PipelineEvent",
    "PipelineEventName",
    "TERMINAL_EVENTS",
    "get_broker",
]


def get_broker() -> "JobBroker":
    return CeleryRedisBroker()
