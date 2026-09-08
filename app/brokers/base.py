from typing import Any, AsyncIterator, Literal, Protocol

from pydantic import BaseModel

from app.brokers.events import PipelineEvent
from app.core.models import BankStatementPayload


class CapacityError(RuntimeError):
    """Raised by a broker when it cannot accept another assessment right now."""


class JobStatusResponse(BaseModel):
    job_id: str
    status: Literal["PENDING", "STARTED", "SUCCESS", "FAILURE"]
    result: dict[str, Any] | None = None
    error: str | None = None


class JobBroker(Protocol):
    async def submit(self, payload: BankStatementPayload) -> str: ...

    def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]: ...

    async def status(self, job_id: str) -> JobStatusResponse: ...
