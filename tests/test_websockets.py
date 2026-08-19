import pytest
from fastapi.testclient import TestClient
from app.api.v1.websockets import ws_manager
from app.main import app


def test_websocket_connection_and_subscription():
    """Verifies that a client can open a WebSocket and receive a subscription event."""
    client = TestClient(app)
    job_id = "job-test-ws-001"

    with client.websocket_connect(f"/api/v1/ws/underwriting/{job_id}") as websocket:
        # 1. Receive the initial subscription confirmation
        data = websocket.receive_json()
        assert data["event"] == "SUBSCRIBED"
        assert data["job_id"] == job_id
        assert job_id in ws_manager.active_subscriptions
        assert len(ws_manager.active_subscriptions[job_id]) == 1

    # 2. Verify socket is removed from active subscriptions upon disconnect
    assert job_id not in ws_manager.active_subscriptions


@pytest.mark.asyncio
async def test_connection_manager_broadcast():
    """Verifies that ConnectionManager correctly broadcasts messages to targeted sockets."""
    client = TestClient(app)
    job_id = "job-test-ws-002"

    with client.websocket_connect(f"/api/v1/ws/underwriting/{job_id}") as websocket:
        # Read initial subscription message
        _ = websocket.receive_json()

        # Simulate broadcasting a completed decision payload
        mock_event = {
            "event": "ASSESSMENT_COMPLETED",
            "job_id": job_id,
            "decision": "APPROVED",
            "net_disposable_income": "1850.00",
        }
        await ws_manager.broadcast_to_job(job_id, mock_event)

        # Receive and verify the broadcast payload
        received_event = websocket.receive_json()
        assert received_event["event"] == "ASSESSMENT_COMPLETED"
        assert received_event["decision"] == "APPROVED"
        assert received_event["net_disposable_income"] == "1850.00"