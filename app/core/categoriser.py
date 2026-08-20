import re
from typing import Dict, List, Pattern

from app.core.models import Transaction, TransactionCategory

NOISE_PATTERNS: List[Pattern[str]] = [
    re.compile(r"\b(POS|DEB|CARD|PURCHASE|DIRECT DEBIT|SO|BGC|FASTER PAYMENTS)\b", re.IGNORECASE),
    re.compile(r"\b\d{2}[A-Z]{3}\d{2}\b", re.IGNORECASE),
    re.compile(r"\b\d{4,16}\b"),
    re.compile(r"[*#_/-]+"),
    re.compile(r"\s+"),
]

CATEGORY_RULES: Dict[TransactionCategory, Pattern[str]] = {
    TransactionCategory.INCOME: re.compile(
        r"\b(SALARY|PAYROLL|WAGES|EMPLOYER|HMRC|PENSION|BENEFITS|DIVIDEND)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.HOUSING_AND_RENT: re.compile(
        r"\b(RENT|MORTGAGE|HOUSING|ESTATE AGENT|LETTINGS|LANDLORD)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.UTILITIES_AND_BILLS: re.compile(
        r"\b(BRITISH GAS|OCTOPUS ENERGY|THAMES WATER|EDF|E\.ON|COUNCIL TAX|EE|VODAFONE|O2|BT GROUP)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.GROCERIES: re.compile(
        r"\b(TESCO|SAINSBURY|ASDA|WAITROSE|ALDI|LIDL|MORRISONS|MARKS & SPENCER|M&S)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.LOAN_REPAYMENT: re.compile(
        r"\b(KLARNA|CLEARPAY|AMEX|BARCLAYCARD|ZOPA|LOAN|CREDIT CARD|CAPITAL ONE|FINANCE)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.GAMBLING: re.compile(
        r"\b(BET365|SKYBET|SKY BET|PADDY POWER|LADBROKES|WILLIAM HILL|BETFAIR|POKERSTARS|CASINO|LOTTERY)\b",
        re.IGNORECASE,
    ),
    TransactionCategory.ENTERTAINMENT: re.compile(
        r"\b(NETFLIX|SPOTIFY|PRIME VIDEO|DISNEY\+|CINEMA|STEAM|PLAYSTATION|PUB|BAR|RESTAURANT)\b",
        re.IGNORECASE,
    ),
}


class TransactionCategoriser:
    """Cleans noisy bank descriptions and categorises transactions."""

    @staticmethod
    def clean_description(raw_text: str) -> str:
        cleaned = raw_text
        for pattern in NOISE_PATTERNS:
            cleaned = pattern.sub(" ", cleaned)
        return cleaned.strip().upper()

    @classmethod
    def categorise_transaction(cls, transaction: Transaction) -> Transaction:
        cleaned = cls.clean_description(transaction.raw_description)
        transaction.cleaned_description = cleaned

        if transaction.amount > 0:
            if CATEGORY_RULES[TransactionCategory.INCOME].search(cleaned):
                transaction.category = TransactionCategory.INCOME
                return transaction

        for category, pattern in CATEGORY_RULES.items():
            if category == TransactionCategory.INCOME:
                continue
            if pattern.search(cleaned):
                transaction.category = category
                return transaction

        transaction.category = TransactionCategory.UNCATEGORISED
        return transaction

    @classmethod
    def process_statement(cls, transactions: List[Transaction]) -> List[Transaction]:
        return [cls.categorise_transaction(t) for t in transactions]
