from decimal import Decimal

from app.brokers.events import PipelineEvent
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline

PAYLOAD = {
    "statement_id": "stmt_pipe_1",
    "account_holder": "Test Person",
    "account_number": "12345678",
    "sort_code": "40-00-01",
    "transactions": [
        {"id": "t1", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "2000.00"},
        {"id": "t2", "date": "2026-08-03", "raw_description": "RENT TO LANDLORD", "amount": "-800.00"},
        {"id": "t3", "date": "2026-08-05", "raw_description": "BET365 UK", "amount": "-500.00"},
    ],
}


def _collect() -> tuple[list[PipelineEvent], object]:
    events: list[PipelineEvent] = []
    assessment = run_underwriting_pipeline(
        BankStatementPayload.model_validate(PAYLOAD), events.append
    )
    return events, assessment


def test_emits_stages_in_order():
    events, _ = _collect()
    assert [e.event for e in events] == [
        "RECEIVED",
        "CATEGORISING",
        "CATEGORISING",
        "SCORING",
        "DECIDED",
    ]


def test_categorising_detail_reports_counts():
    events, _ = _collect()
    detail = events[2].detail
    assert detail is not None and "3 transactions" in detail


def test_decided_event_carries_assessment_dict():
    events, assessment = _collect()
    decided = events[-1]
    assert decided.data == assessment.model_dump(mode="json")
    assert decided.data["decision"] == "DECLINED"  # gambling 25% of income


def test_return_value_matches_direct_engine_call():
    from app.core.affordability import AffordabilityEngine
    from app.core.categoriser import TransactionCategoriser

    stmt = BankStatementPayload.model_validate(PAYLOAD)
    stmt.transactions = TransactionCategoriser.process_statement(stmt.transactions)
    expected = AffordabilityEngine.evaluate(stmt)

    _, assessment = _collect()
    assert assessment == expected
    assert assessment.gambling_income_ratio == Decimal("0.25")
