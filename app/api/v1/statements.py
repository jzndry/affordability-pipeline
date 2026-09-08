from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.brokers import CapacityError, get_broker
from app.brokers.base import JobBroker
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.samples import SampleSummary, get_persona, list_personas
from app.ratelimit import limiter

router = APIRouter(prefix="/statements", tags=["Statements"])

MAX_TRANSACTIONS_LIMIT = 500


class IngestionResponse(BaseModel):
    message: str
    job_id: str
    status: str


@router.get("/samples", response_model=list[SampleSummary])
async def get_samples() -> list[SampleSummary]:
    """List the canonical persona fixtures and their expected underwriting outcome."""
    return list_personas()


@router.get("/samples/{persona_id}", response_model=BankStatementPayload)
async def get_sample(persona_id: str) -> BankStatementPayload:
    """Return the full bank statement payload for a single persona fixture."""
    try:
        return get_persona(persona_id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail=f"Unknown persona '{persona_id}'."
        ) from None


@router.post(
    "/ingest",
    response_model=IngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Non-blocking Statement Ingestion",
)
@limiter.limit(settings.INGEST_PER_IP_LIMIT)
@limiter.limit(settings.INGEST_GLOBAL_LIMIT, key_func=lambda: "global-ingest")
async def ingest_statement(
    request: Request,
    payload: BankStatementPayload,
    broker: JobBroker = Depends(get_broker),
) -> IngestionResponse:
    """
    Accepts statement payloads, enforces rate/size guardrails, and submits to the broker.
    Returns HTTP 202 Accepted with a job_id.
    """
    if len(payload.transactions) > MAX_TRANSACTIONS_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Statement exceeds maximum limit of {MAX_TRANSACTIONS_LIMIT} transactions.",
        )

    try:
        job_id = await broker.submit(payload)
    except CapacityError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="The demo is busy right now. Try again in a moment.",
            headers={"Retry-After": "10"},
        ) from None

    return IngestionResponse(
        message="Statement received and enqueued for underwriting assessment.",
        job_id=job_id,
        status="PENDING",
    )


@router.get("/jobs/{job_id}", summary="Check Assessment Job Status")
async def get_job_status(job_id: str, broker: JobBroker = Depends(get_broker)) -> dict[str, Any]:
    """Reports job execution status and results from the configured broker."""
    return (await broker.status(job_id)).model_dump(mode="json")
