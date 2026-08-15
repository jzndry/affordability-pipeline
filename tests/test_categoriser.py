from datetime import date
from decimal import Decimal
from app.core.categoriser import TransactionCategoriser
from app.core.models import Transaction, TransactionCategory


def test_clean_noisy_description():
    raw = "POS 4829 14OCT26 BET365 UK LONDON"
    cleaned = TransactionCategoriser.clean_description(raw)
    assert "POS" not in cleaned
    assert "4829" not in cleaned
    assert "14OCT26" not in cleaned
    assert "BET365" in cleaned


def test_categorise_gambling_transaction():
    tx = Transaction(
        id="tx_01",
        date=date(2026, 8, 15),
        raw_description="CARD PURCHASE 10AUG26 SKYBET UK",
        amount=Decimal("-25.00"),
    )
    result = TransactionCategoriser.categorise_transaction(tx)
    assert result.category == TransactionCategory.GAMBLING


def test_categorise_income_transaction():
    tx = Transaction(
        id="tx_02",
        date=date(2026, 8, 1),
        raw_description="BGC EMPLOYER SALARY AUG 2026",
        amount=Decimal("3200.00"),
    )
    result = TransactionCategoriser.categorise_transaction(tx)
    assert result.category == TransactionCategory.INCOME


def test_categorise_loan_repayment():
    tx = Transaction(
        id="tx_03",
        date=date(2026, 8, 5),
        raw_description="DIRECT DEBIT KLARNA PAY IN 3",
        amount=Decimal("-45.00"),
    )
    result = TransactionCategoriser.categorise_transaction(tx)
    assert result.category == TransactionCategory.LOAN_REPAYMENT