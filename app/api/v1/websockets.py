import json
from typing import Dict, Set
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/ws", tags=["WebSockets"])


class ConnectionManager:
    """Manages active WebSocket connections subscribed to underwriting job channels."""

    def __init__(self) -> None:
        # Maps job_id -> set of active WebSockets listening for that specific job
        self.active_subscriptions: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: str) -> None:
        """Accepts a socket connection and subscribes it to a job_id."""
        await websocket.accept()
        if job_id not in self.active_subscriptions:
            self.active_subscriptions[job_id] = set()
        self.active_subscriptions[job_id].add(websocket)

    def disconnect(self, websocket: WebSocket, job_id: str) -> None:
        """Removes a socket connection when a client disconnects."""
        if job_id in self.active_subscriptions:
            self.active_subscriptions[job_id].discard(websocket)
            if not self.active_subscriptions[job_id]:
                del self.active_subscriptions[job_id]

    async def broadcast_to_job(self, job_id: str, message: dict) -> None:
        """Sends a JSON message to all clients subscribed to a specific job_id."""
        if job_id in self.active_subscriptions:
            payload = json.dumps(message)
            dead_sockets = set()
            for connection in self.active_subscriptions[job_id]:
                try:
                    await connection.send_text(payload)
                except Exception:
                    dead_sockets.add(connection)

            # Clean up broken sockets
            for dead in dead_sockets:
                self.active_subscriptions[job_id].discard(dead)


# Global connection manager instance
ws_manager = ConnectionManager()


@router.websocket("/underwriting/{job_id}")
async def websocket_underwriting_endpoint(websocket: WebSocket, job_id: str) -> None:
    """
    WebSocket endpoint for real-time underwriting updates.
    Clients connect to /api/v1/ws/underwriting/{job_id} to receive the final decision.
    """
    await ws_manager.connect(websocket, job_id)
    try:
        # Send initial confirmation message
        await websocket.send_json({
            "event": "SUBSCRIBED",
            "job_id": job_id,
            "message": f"Successfully subscribed to real-time updates for job {job_id}.",
        })

        # Keep connection open waiting for disconnect or client heartbeats
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, job_id)