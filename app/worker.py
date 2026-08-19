import json
from typing import Any, Dict
from celery import Celery
import redis
from app.config import settings
from app.core.affordability import AffordabilityEngine
from app.core.categoriser import TransactionCategoriser
from app.core.models import BankStatementPayload

# Initialise Celery application
celery_app = Celery(
    "underwriting_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,  # Using Redis as both broker and backend for Celery tasks
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

# Synchronous Redis client for publishing event notifications
redis_client = redis.Redis.from_url(settings.REDIS_URL)


@celery_app.task(name="tasks.process_affordability_assessment", bind=True)
def process_affordability_assessment(self, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Background Task:
    1. Deserialises raw dictionary into a domain model.
    2. Categorises all raw transaction strings.
    3. Evaluates credit affordability and risk rules.
    4. Publishes completion event to Redis Pub/Sub for WebSockets.
    5. Returns assessment dictionary.
    """
    # 1. Parse statement
    statement = BankStatementPayload.model_validate(payload_dict)

    # 2. Categorise transactions
    statement.transactions = TransactionCategoriser.process_statement(statement.transactions)

    # 3. Evaluate Affordability
    assessment = AffordabilityEngine.evaluate(statement)
    result_dict = assessment.model_dump(mode="json")

    # 4. Broadcast event via Redis Pub/Sub for WebSockets
    try:
        event_payload = {
            "event": "ASSESSMENT_COMPLETED",
            "job_id": self.request.id,
            "data": result_dict,
        }
        redis_client.publish(f"underwriting_jobs:{self.request.id}", json.dumps(event_payload))
    except Exception:
        # Non-blocking safeguard if Redis Pub/Sub is unreachable in standalone unit tests
        pass

    return result_dict