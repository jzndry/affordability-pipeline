import asyncio
import json
import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.config import settings
from app.worker import celery_app

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["WebSockets"])


@router.websocket("/underwriting/{job_id}")
async def websocket_underwriting_endpoint(websocket: WebSocket, job_id: str) -> None:
    """
    WebSocket endpoint for real-time underwriting updates.
    Handles fast execution by checking Celery backend immediately,
    falling back to Redis Pub/Sub if still processing.
    """
    await websocket.accept()

    # 1. Send subscription confirmation
    await websocket.send_json(
        {
            "event": "SUBSCRIBED",
            "job_id": job_id,
            "message": f"Successfully subscribed to real-time updates for job {job_id}.",
        }
    )

    # 2. Race-condition check: did Celery already finish before WS connected?
    async_result = celery_app.AsyncResult(job_id)
    if async_result.ready():
        if async_result.successful():
            await websocket.send_json({
                "event": "ASSESSMENT_COMPLETED",
                "job_id": job_id,
                "data": async_result.result,
            })
        else:
            await websocket.send_json({
                "event": "ASSESSMENT_FAILED",
                "job_id": job_id,
                "error": str(async_result.info),
            })
        return

    # 3. If still pending, listen to Redis Pub/Sub
    redis_conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    pubsub = redis_conn.pubsub()
    channel_name = f"underwriting_jobs:{job_id}"
    await pubsub.subscribe(channel_name)

    try:
        while True:
            # Check for published messages with timeout
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)

            if message and message.get("type") == "message":
                raw_data = message.get("data")
                if isinstance(raw_data, str):
                    try:
                        await websocket.send_json(json.loads(raw_data))
                    except json.JSONDecodeError:
                        await websocket.send_text(raw_data)
                elif isinstance(raw_data, dict):
                    await websocket.send_json(raw_data)
                break

            # Fallback check on Celery task state
            if async_result.ready():
                if async_result.successful():
                    await websocket.send_json({
                        "event": "ASSESSMENT_COMPLETED",
                        "job_id": job_id,
                        "data": async_result.result,
                    })
                break

            await asyncio.sleep(0.05)

    except WebSocketDisconnect:
        logger.info("Client disconnected from WebSocket job %s", job_id)
    except Exception as exc:
        logger.error("Error streaming WebSocket updates for job %s: %s", job_id, str(exc))
    finally:
        await pubsub.unsubscribe(channel_name)
        await pubsub.close()
        await redis_conn.aclose()
