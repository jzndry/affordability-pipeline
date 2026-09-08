import asyncio
import itertools
from dataclasses import dataclass, field
from typing import AsyncGenerator
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
        # No await between the guard and this increment, so check-and-admit is
        # atomic under the single-threaded event loop and the cap actually holds.
        self._active += 1
        job_id = uuid4().hex
        channel = JobChannel()
        self._channels[job_id] = channel
        channel.task = asyncio.create_task(self._run(job_id, payload))
        return job_id

    async def _run(self, job_id: str, payload: BankStatementPayload) -> None:
        channel = self._channels[job_id]
        seq: "itertools.count[int]" = itertools.count()

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

    async def subscribe(self, job_id: str) -> AsyncGenerator[PipelineEvent, None]:
        channel = self._channels.get(job_id)
        if channel is None:
            return
        queue: "asyncio.Queue[PipelineEvent]" = asyncio.Queue()
        channel.subscribers.add(queue)
        seen: set[int] = set()
        max_seq = -1
        try:
            for event in list(channel.events):
                seen.add(event.seq)
                max_seq = max(max_seq, event.seq)
                yield event
                if event.is_terminal:
                    return

            # Live tail. Race the queue against the channel's done flag: if _run
            # exits via a BaseException/CancelledError it sets `done` without ever
            # emitting a terminal event, so an unconditional `await queue.get()`
            # would block this subscriber forever and burn a connection slot.
            done_task = asyncio.create_task(channel.done.wait())
            try:
                while True:
                    get_task = asyncio.create_task(queue.get())
                    try:
                        await asyncio.wait(
                            {get_task, done_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    except BaseException:
                        get_task.cancel()
                        raise

                    # A real event from the queue always wins when both are ready.
                    if get_task.done() and not get_task.cancelled():
                        event = get_task.result()
                        if event.seq in seen:
                            continue
                        seen.add(event.seq)
                        max_seq = max(max_seq, event.seq)
                        yield event
                        if event.is_terminal:
                            return
                        continue

                    # The job ended and the queue has nothing more: synthesize a
                    # terminal event so the stream (and the WS slot) is released.
                    get_task.cancel()
                    try:
                        await get_task
                    except asyncio.CancelledError:
                        pass
                    yield PipelineEvent(
                        event="FAILED",
                        job_id=job_id,
                        seq=max_seq + 1,
                        error="job ended without a terminal event",
                    )
                    return
            finally:
                done_task.cancel()
                try:
                    await done_task
                except asyncio.CancelledError:
                    pass
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
