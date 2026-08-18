from typing import Any, Dict
from celery import Celery
from app.config import settings
from app.core.affordability import AffordabilityEngine
from app.core.categoriser import TransactionCategoriser
from app.core.models import BankStatementPayload

# Initialise Celery application
celery_app = Celery(
    "underwriting_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)


@celery_app.task(name="tasks.process_affordability_assessment")
def process_affordability_assessment(payload_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Background Task:
    1. Deserialises raw dictionary into a domain model.
    2. Categorises all raw transaction strings.
    3. Evaluates credit affordability and risk rules.
    4. Returns assessment dictionary.
    """
    # 1. Parse statement
    statement = BankStatementPayload.model_validate(payload_dict)

    # 2. Categorise transactions
    statement.transactions = TransactionCategoriser.process_statement(statement.transactions)

    # 3. Evaluate Affordability
    assessment = AffordabilityEngine.evaluate(statement)

    # 4. Return as JSON-serialisable dictionary
    return assessment.model_dump(mode="json")