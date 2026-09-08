from typing import Callable

from app.brokers.events import PipelineEvent
from app.core.affordability import AffordabilityEngine
from app.core.categoriser import TransactionCategoriser
from app.core.models import AffordabilityAssessment, BankStatementPayload

EmitFn = Callable[[PipelineEvent], None]


def run_underwriting_pipeline(
    payload: BankStatementPayload, emit: EmitFn
) -> AffordabilityAssessment:
    """Run parse -> categorise -> score -> decide, announcing each stage via ``emit``.

    ``emit`` is transport-agnostic: it may publish to Redis, push to an in-memory
    queue, or collect into a list. This function never raises ``PipelineEvent``
    FAILED itself; callers are responsible for translating exceptions.
    """
    emit(PipelineEvent(event="RECEIVED"))

    emit(PipelineEvent(event="CATEGORISING"))
    payload.transactions = TransactionCategoriser.process_statement(payload.transactions)
    category_count = len({t.category for t in payload.transactions})
    emit(
        PipelineEvent(
            event="CATEGORISING",
            detail=(
                f"Sorted {len(payload.transactions)} transactions "
                f"into {category_count} categories"
            ),
        )
    )

    emit(PipelineEvent(event="SCORING"))
    assessment = AffordabilityEngine.evaluate(payload)

    emit(PipelineEvent(event="DECIDED", data=assessment.model_dump(mode="json")))
    return assessment
