import contextlib
import logging

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from app.brokers import get_broker
from app.brokers.base import JobBroker
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["WebSockets"])

_ws_connections = 0


@router.websocket("/underwriting/{job_id}")
async def websocket_underwriting_endpoint(
    websocket: WebSocket,
    job_id: str,
    broker: JobBroker = Depends(get_broker),
) -> None:
    global _ws_connections
    if _ws_connections >= settings.MAX_WS_CONNECTIONS:
        await websocket.close(code=1013)  # 1013 = try again later
        return

    await websocket.accept()
    _ws_connections += 1
    try:
        await websocket.send_json({"event": "SUBSCRIBED", "job_id": job_id, "seq": -1})
        async with contextlib.aclosing(broker.subscribe(job_id)) as stream:
            async for event in stream:
                await websocket.send_json(event.model_dump(mode="json"))
    except WebSocketDisconnect:
        logger.info("client disconnected from job %s", job_id)
    except Exception:  # noqa: BLE001 - log and close, never propagate
        logger.exception("error streaming job %s", job_id)
    finally:
        _ws_connections -= 1
