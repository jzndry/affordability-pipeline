from typing import Any, Dict

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from app.api.v1.websockets import router as websockets_router
from app.core.models import BankStatementPayload
from app.worker import celery_app, process_affordability_assessment

app = FastAPI(
    title="Open Banking Affordability Engine",
    version="1.0.0",
    description="Asynchronous credit underwriting and transaction categorisation API.",
)

# Mount the WebSocket router under /api/v1
app.include_router(websockets_router, prefix="/api/v1")


class IngestionResponse(BaseModel):
    message: str
    job_id: str
    status: str


@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy"}


@app.post(
    "/api/v1/statements/ingest",
    response_model=IngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Non-blocking Statement Ingestion",
    tags=["Statements"],
)
async def ingest_statement(payload: BankStatementPayload) -> IngestionResponse:
    """
    Accepts statement payloads and enqueues processing to Celery.
    Returns HTTP 202 Accepted with a job_id.
    """
    try:
        task = process_affordability_assessment.delay(payload.model_dump(mode="json"))
        return IngestionResponse(
            message="Statement received and enqueued for underwriting assessment.",
            job_id=task.id,
            status="PENDING",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enqueue statement task: {str(e)}",
        )


@app.get(
    "/api/v1/statements/jobs/{job_id}",
    summary="Check Assessment Job Status",
    tags=["Statements"],
)
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """Polls Celery/Redis for job execution status and results."""
    task_result = celery_app.AsyncResult(job_id)
    if task_result.state == "PENDING":
        return {"job_id": job_id, "status": "PENDING"}
    elif task_result.state == "SUCCESS":
        return {"job_id": job_id, "status": "SUCCESS", "result": task_result.result}
    elif task_result.state == "FAILURE":
        return {"job_id": job_id, "status": "FAILURE", "error": str(task_result.info)}
    return {"job_id": job_id, "status": task_result.state}
