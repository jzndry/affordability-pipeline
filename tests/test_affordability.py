from datetime import date
from decimal import Decimal
from app.core.affordability import AffordabilityEngine
from app.core.models import BankStatementPayload, Transaction, TransactionCategory, UnderwritingDecision


def test_prime_borrower_approved():
    statement = BankStatementPayload(
        statement_id="stmt_001",
        account_holder="Alice Prime",
        account_number="12345678",
        sort_code="20-00-00",
        transactions=[
            Transaction(
                id="tx_1",
                date=date(2026, 8, 1),
                raw_description="MONTHLY SALARY",
                amount=Decimal("3500.00"),
                category=TransactionCategory.INCOME,
            ),
            Transaction(
                id="tx_2",
                date=date(2026, 8, 2),
                raw_description="RENT PAYMENT",
                amount=Decimal("-1000.00"),
                category=TransactionCategory.HOUSING_AND_RENT,
            ),
            Transaction(
                id="tx_3",
                date=date(2026, 8, 3),
                raw_description="TESCO GROCERIES",
                amount=Decimal("-300.00"),
                category=TransactionCategory.GROCERIES,
            ),
        ],
    )
    assessment = AffordabilityEngine.evaluate(statement)
    assert assessment.decision == UnderwritingDecision.APPROVED
    assert assessment.total_income == Decimal("3500.00")
    assert assessment.net_disposable_income == Decimal("2200.00")
    assert len(assessment.risk_flags) == 0


def test_gambling_risk_auto_declined():
    statement = BankStatementPayload(
        statement_id="stmt_002",
        account_holder="Bob Gambler",
        account_number="12345678",
        sort_code="20-00-00",
        transactions=[
            Transaction(
                id="tx_1",
                date=date(2026, 8, 1),
                raw_description="SALARY",
                amount=Decimal("2000.00"),
                category=TransactionCategory.INCOME,
            ),
            Transaction(
                id="tx_2",
                date=date(2026, 8, 2),
                raw_description="BET365 CASINO",
                amount=Decimal("-300.00"),
                category=TransactionCategory.GAMBLING,
            ),
        ],
    )
    assessment = AffordabilityEngine.evaluate(statement)
    assert assessment.decision == UnderwritingDecision.DECLINED
    assert any("EXCESSIVE_GAMBLING_RISK" in flag for flag in assessment.risk_flags)


def test_high_debt_to_income_declined():
    statement = BankStatementPayload(
        statement_id="stmt_003",
        account_holder="Charlie Debt",
        account_number="12345678",
        sort_code="20-00-00",
        transactions=[
            Transaction(
                id="tx_1",
                date=date(2026, 8, 1),
                raw_description="SALARY",
                amount=Decimal("2000.00"),
                category=TransactionCategory.INCOME,
            ),
            Transaction(
                id="tx_2",
                date=date(2026, 8, 5),
                raw_description="KLARNA LOAN",
                amount=Decimal("-900.00"),
                category=TransactionCategory.LOAN_REPAYMENT,
            ),
        ],
    )
    assessment = AffordabilityEngine.evaluate(statement)
    assert assessment.decision == UnderwritingDecision.DECLINED
    assert any("HIGH_DEBT_TO_INCOME_RATIO" in flag for flag in assessment.risk_flags)


def test_zero_income_edge_case():
    statement = BankStatementPayload(
        statement_id="stmt_004",
        account_holder="Dana Unemployed",
        account_number="12345678",
        sort_code="20-00-00",
        transactions=[
            Transaction(
                id="tx_1",
                date=date(2026, 8, 1),
                raw_description="COFFEE SHOP",
                amount=Decimal("-4.50"),
                category=TransactionCategory.ENTERTAINMENT,
            )
        ],
    )
    assessment = AffordabilityEngine.evaluate(statement)
    assert assessment.decision == UnderwritingDecision.DECLINED
    assert "NO_VERIFIABLE_INCOME" in assessment.risk_flags