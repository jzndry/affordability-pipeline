from typing import AsyncIterator

from fastapi.testclient import TestClient

from app.brokers import get_broker
from app.brokers.events import PipelineEvent
from app.main import app

WS_PATH = "/api/v1/ws/underwriting/{job_id}"


class FakeBroker:
    def __init__(self, events: list[PipelineEvent]) -> None:
        self._events = events

    async def submit(self, payload):  # pragma: no cover - unused here
        return "unused"

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        for e in self._events:
            yield e

    async def status(self, job_id: str):  # pragma: no cover - unused here
        raise NotImplementedError


def _use(events: list[PipelineEvent]) -> None:
    app.dependency_overrides[get_broker] = lambda: FakeBroker(events)


def teardown_function() -> None:
    app.dependency_overrides.clear()


def test_streams_subscribed_then_every_event_in_order():
    job_id = "job-1"
    _use(
        [
            PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
            PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
            PipelineEvent(event="SCORING", job_id=job_id, seq=2),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={"decision": "APPROVED"}),
        ]
    )
    with TestClient(app).websocket_connect(WS_PATH.format(job_id=job_id)) as ws:
        assert ws.receive_json()["event"] == "SUBSCRIBED"
        assert [ws.receive_json()["event"] for _ in range(4)] == [
            "RECEIVED",
            "CATEGORISING",
            "SCORING",
            "DECIDED",
        ]


def test_forwards_failed_event():
    _use([PipelineEvent(event="FAILED", job_id="j", seq=0, error="kaboom")])
    with TestClient(app).websocket_connect(WS_PATH.format(job_id="j")) as ws:
        assert ws.receive_json()["event"] == "SUBSCRIBED"
        frame = ws.receive_json()
        assert frame["event"] == "FAILED"
        assert frame["error"] == "kaboom"
