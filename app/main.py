import os
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.api.v1.websockets import router as websockets_router
from app.core.models import BankStatementPayload
from app.worker import celery_app, process_affordability_assessment

# Initialise rate limiter keyed by client IP address
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="Open Banking Affordability Engine",
    version="1.0.0",
    description="Asynchronous credit underwriting and transaction categorisation API.",
)

# Attach rate limiter state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# Allow CORS for cross-origin frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the WebSocket router under /api/v1
app.include_router(websockets_router, prefix="/api/v1")

# Mount Static Frontend Assets
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/frontend", StaticFiles(directory=frontend_dir), name="frontend")


class IngestionResponse(BaseModel):
    message: str
    job_id: str
    status: str


MAX_TRANSACTIONS_LIMIT = 500


@app.get("/", include_in_schema=False)
async def serve_dashboard() -> FileResponse:
    """Serves the real-time underwriter dashboard frontend."""
    index_file = os.path.join(frontend_dir, "index.html")
    return FileResponse(index_file)


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
@limiter.limit("10/minute")
async def ingest_statement(
    request: Request,
    payload: BankStatementPayload,
) -> IngestionResponse:
    """
    Accepts statement payloads, enforces rate/size guardrails, and enqueues to Celery.
    Returns HTTP 202 Accepted with a job_id.
    """
    if len(payload.transactions) > MAX_TRANSACTIONS_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Statement exceeds maximum limit of {MAX_TRANSACTIONS_LIMIT} transactions.",
        )

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