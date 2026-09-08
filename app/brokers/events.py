import time
from typing import Any, Literal

from pydantic import BaseModel, Field

PipelineEventName = Literal["RECEIVED", "CATEGORISING", "SCORING", "DECIDED", "FAILED"]

TERMINAL_EVENTS: frozenset[str] = frozenset({"DECIDED", "FAILED"})


class PipelineEvent(BaseModel):
    event: PipelineEventName
    job_id: str = ""
    seq: int = 0
    ts: float = Field(default_factory=time.time)
    detail: str | None = None
    data: dict[str, Any] | None = None
    error: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.event in TERMINAL_EVENTS
