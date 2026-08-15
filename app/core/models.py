from datetime import date
from decimal import Decimal
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


class TransactionCategory(str, Enum):
    INCOME = "INCOME"
    HOUSING_AND_RENT = "HOUSING_AND_RENT"
    UTILITIES_AND_BILLS = "UTILITIES_AND_BILLS"
    GROCERIES = "GROCERIES"
    LOAN_REPAYMENT = "LOAN_REPAYMENT"
    GAMBLING = "GAMBLING"
    ENTERTAINMENT = "ENTERTAINMENT"
    UNCATEGORISED = "UNCATEGORISED"


class UnderwritingDecision(str, Enum):
    APPROVED = "APPROVED"
    REFERRED = "REFERRED"
    DECLINED = "DECLINED"


class Transaction(BaseModel):
    id: str = Field(..., description="Unique transaction ID")
    date: date
    raw_description: str
    cleaned_description: Optional[str] = None
    amount: Decimal = Field(..., description="Positive for income, negative for expenses")
    category: TransactionCategory = TransactionCategory.UNCATEGORISED

    @field_validator("amount", mode="before")
    @classmethod
    def parse_exact_decimal(cls, value: object) -> Decimal:
        """Enforces exact Decimal conversion without float intermediate loss."""
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))


class BankStatementPayload(BaseModel):
    statement_id: str
    account_holder: str
    account_number: str
    sort_code: str
    transactions: List[Transaction]


class AffordabilityAssessment(BaseModel):
    statement_id: str
    account_holder: str
    total_income: Decimal
    essential_expenditure: Decimal
    discretionary_expenditure: Decimal
    gambling_expenditure: Decimal
    net_disposable_income: Decimal
    gambling_income_ratio: Decimal
    debt_to_income_ratio: Decimal
    decision: UnderwritingDecision
    risk_flags: List[str]