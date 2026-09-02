import json
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from app.main import app

WS_PATH = "/api/v1/ws/underwriting/{job_id}"


def test_websocket_returns_completed_result_when_job_already_finished():
    """
    Race-condition fast path: if the Celery task finished before the client
    connected, the endpoint should return the stored result immediately without
    touching Redis Pub/Sub.
    """
    job_id = "job-ws-complete-001"
    result_payload = {
        "statement_id": "stmt_001",
        "decision": "APPROVED",
        "net_disposable_income": "1850.00",
        "risk_flags": [],
    }

    fake_result = MagicMock()
    fake_result.ready.return_value = True
    fake_result.successful.return_value = True
    fake_result.result = result_payload

    with patch("app.api.v1.websockets.celery_app.AsyncResult", return_value=fake_result):
        client = TestClient(app)
        with client.websocket_connect(WS_PATH.format(job_id=job_id)) as websocket:
            subscribed = websocket.receive_json()
            assert subscribed["event"] == "SUBSCRIBED"
            assert subscribed["job_id"] == job_id

            completed = websocket.receive_json()
            assert completed["event"] == "ASSESSMENT_COMPLETED"
            assert completed["job_id"] == job_id
            assert completed["data"] == result_payload


def test_websocket_streams_pubsub_event_when_job_pending():
    """
    Streaming path: while the job is still running, the endpoint subscribes to
    the job's Redis Pub/Sub channel and forwards the completion event to the
    client as soon as the worker publishes it.
    """
    job_id = "job-ws-pending-002"
    published_event = {
        "event": "ASSESSMENT_COMPLETED",
        "job_id": job_id,
        "data": {"decision": "DECLINED", "risk_flags": ["NO_VERIFIABLE_INCOME"]},
    }

    fake_result = MagicMock()
    fake_result.ready.return_value = False

    fake_pubsub = MagicMock()
    fake_pubsub.subscribe = AsyncMock()
    fake_pubsub.unsubscribe = AsyncMock()
    fake_pubsub.close = AsyncMock()
    fake_pubsub.get_message = AsyncMock(
        return_value={"type": "message", "data": json.dumps(published_event)}
    )

    fake_conn = MagicMock()
    fake_conn.pubsub.return_value = fake_pubsub
    fake_conn.aclose = AsyncMock()

    with (
        patch("app.api.v1.websockets.celery_app.AsyncResult", return_value=fake_result),
        patch("app.api.v1.websockets.aioredis.from_url", return_value=fake_conn),
    ):
        client = TestClient(app)
        with client.websocket_connect(WS_PATH.format(job_id=job_id)) as websocket:
            subscribed = websocket.receive_json()
            assert subscribed["event"] == "SUBSCRIBED"
            assert subscribed["job_id"] == job_id

            streamed = websocket.receive_json()
            assert streamed == published_event

    fake_pubsub.subscribe.assert_awaited_once_with(f"underwriting_jobs:{job_id}")
