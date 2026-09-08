import asyncio
import itertools
from dataclasses import dataclass, field
from typing import AsyncIterator
from uuid import uuid4

from app.brokers.base import CapacityError, JobStatusResponse
from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline


@dataclass
class JobChannel:
    events: list[PipelineEvent] = field(default_factory=list)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    subscribers: "set[asyncio.Queue[PipelineEvent]]" = field(default_factory=set)
    task: "asyncio.Task[None] | None" = None


class InProcessBroker:
    """JobBroker that runs the pipeline on the event loop with in-memory fan-out."""

    def __init__(self) -> None:
        self._channels: dict[str, JobChannel] = {}
        self._active = 0

    async def submit(self, payload: BankStatementPayload) -> str:
        if self._active >= settings.MAX_CONCURRENT_ASSESSMENTS:
            raise CapacityError("assessment capacity reached")
        job_id = uuid4().hex
        channel = JobChannel()
        self._channels[job_id] = channel
        channel.task = asyncio.create_task(self._run(job_id, payload))
        return job_id

    async def _run(self, job_id: str, payload: BankStatementPayload) -> None:
        channel = self._channels[job_id]
        seq: "itertools.count[int]" = itertools.count()
        self._active += 1

        def emit(event: PipelineEvent) -> None:
            event.job_id = job_id
            event.seq = next(seq)
            channel.events.append(event)
            for queue in list(channel.subscribers):
                queue.put_nowait(event)

        try:
            run_underwriting_pipeline(payload, emit)
        except Exception as exc:  # noqa: BLE001 - surface as terminal event
            emit(PipelineEvent(event="FAILED", error=str(exc)))
        finally:
            self._active -= 1
            channel.done.set()
            asyncio.get_running_loop().call_later(
                settings.EVENT_LOG_TTL_SECONDS, self._channels.pop, job_id, None
            )

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        channel = self._channels.get(job_id)
        if channel is None:
            return
        queue: "asyncio.Queue[PipelineEvent]" = asyncio.Queue()
        channel.subscribers.add(queue)
        seen: set[int] = set()
        try:
            for event in list(channel.events):
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return
            while True:
                event = await queue.get()
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return
        finally:
            channel.subscribers.discard(queue)

    async def status(self, job_id: str) -> JobStatusResponse:
        channel = self._channels.get(job_id)
        if channel is None:
            return JobStatusResponse(job_id=job_id, status="PENDING")
        if not channel.done.is_set():
            return JobStatusResponse(job_id=job_id, status="STARTED")
        terminal = channel.events[-1] if channel.events else None
        if terminal is not None and terminal.event == "DECIDED":
            return JobStatusResponse(job_id=job_id, status="SUCCESS", result=terminal.data)
        error = terminal.error if terminal is not None else "unknown failure"
        return JobStatusResponse(job_id=job_id, status="FAILURE", error=error)
