from decimal import ROUND_HALF_UP, Decimal
from typing import List

from app.core.models import (
    AffordabilityAssessment,
    BankStatementPayload,
    TransactionCategory,
    UnderwritingDecision,
)

MAX_GAMBLING_RATIO = Decimal("0.10")  # >= 10% -> Auto Decline
REFERRAL_GAMBLING_RATIO = Decimal("0.05")  # >= 5% -> Manual Referral
MAX_DEBT_TO_INCOME_RATIO = Decimal("0.40")  # >= 40% -> Auto Decline
MIN_DISPOSABLE_INCOME = Decimal("150.00")  # Minimum £150 buffer required


class AffordabilityEngine:
    """Computes cash-flow metrics and credit risk indicators with exact Decimal precision."""

    @staticmethod
    def _round_currency(amount: Decimal) -> Decimal:
        return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @classmethod
    def evaluate(cls, statement: BankStatementPayload) -> AffordabilityAssessment:
        total_income = Decimal("0.00")
        essential_expenditure = Decimal("0.00")
        discretionary_expenditure = Decimal("0.00")
        gambling_expenditure = Decimal("0.00")
        debt_repayments = Decimal("0.00")

        for t in statement.transactions:
            if t.amount > 0:
                total_income += t.amount
            else:
                abs_amount = abs(t.amount)
                if t.category in (
                    TransactionCategory.HOUSING_AND_RENT,
                    TransactionCategory.UTILITIES_AND_BILLS,
                    TransactionCategory.GROCERIES,
                ):
                    essential_expenditure += abs_amount
                elif t.category == TransactionCategory.LOAN_REPAYMENT:
                    debt_repayments += abs_amount
                    essential_expenditure += abs_amount
                elif t.category == TransactionCategory.GAMBLING:
                    gambling_expenditure += abs_amount
                    discretionary_expenditure += abs_amount
                else:
                    discretionary_expenditure += abs_amount

        total_outgoings = essential_expenditure + discretionary_expenditure
        net_disposable_income = total_income - total_outgoings

        safe_income = total_income if total_income > 0 else Decimal("1.00")
        gambling_ratio = gambling_expenditure / safe_income
        debt_ratio = debt_repayments / safe_income

        risk_flags: List[str] = []
        decision = UnderwritingDecision.APPROVED

        if total_income == Decimal("0.00"):
            decision = UnderwritingDecision.DECLINED
            risk_flags.append("NO_VERIFIABLE_INCOME")

        if gambling_ratio >= MAX_GAMBLING_RATIO:
            decision = UnderwritingDecision.DECLINED
            risk_flags.append(f"EXCESSIVE_GAMBLING_RISK ({(gambling_ratio * 100):.1f}% of income)")
        elif gambling_ratio >= REFERRAL_GAMBLING_RATIO:
            if decision != UnderwritingDecision.DECLINED:
                decision = UnderwritingDecision.REFERRED
            risk_flags.append(
                f"ELEVATED_GAMBLING_ACTIVITY ({(gambling_ratio * 100):.1f}% of income)"
            )

        if debt_ratio >= MAX_DEBT_TO_INCOME_RATIO:
            decision = UnderwritingDecision.DECLINED
            risk_flags.append(f"HIGH_DEBT_TO_INCOME_RATIO ({(debt_ratio * 100):.1f}%)")

        if net_disposable_income < MIN_DISPOSABLE_INCOME:
            if decision != UnderwritingDecision.DECLINED:
                decision = UnderwritingDecision.REFERRED
            risk_flags.append(f"LOW_NET_DISPOSABLE_INCOME (£{net_disposable_income:.2f})")

        return AffordabilityAssessment(
            statement_id=statement.statement_id,
            account_holder=statement.account_holder,
            total_income=cls._round_currency(total_income),
            essential_expenditure=cls._round_currency(essential_expenditure),
            discretionary_expenditure=cls._round_currency(discretionary_expenditure),
            gambling_expenditure=cls._round_currency(gambling_expenditure),
            net_disposable_income=cls._round_currency(net_disposable_income),
            gambling_income_ratio=cls._round_currency(gambling_ratio),
            debt_to_income_ratio=cls._round_currency(debt_ratio),
            decision=decision,
            risk_flags=risk_flags,
        )
