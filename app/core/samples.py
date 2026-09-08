from datetime import date
from typing import Literal

from pydantic import BaseModel

from app.core.models import BankStatementPayload, Transaction


class SampleSummary(BaseModel):
    id: str
    label: str
    blurb: str
    expected_decision: Literal["APPROVED", "REFERRED", "DECLINED"]


def _tx(idx: int, day: int, desc: str, amount: str) -> Transaction:
    return Transaction(
        id=f"t{idx}",
        date=date(2026, 9, day),
        raw_description=desc,
        amount=amount,  # type: ignore[arg-type]  # validator coerces str -> Decimal
    )


_STABLE = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "3200.00"),
    _tx(2, 2, "RENT PAYMENT TO LANDLORD", "-1150.00"),
    _tx(3, 3, "COUNCIL TAX BOROUGH", "-145.00"),
    _tx(4, 3, "OCTOPUS ENERGY LTD", "-95.00"),
    _tx(5, 4, "VODAFONE LTD", "-32.00"),
    _tx(6, 5, "TESCO STORES 3345", "-78.00"),
    _tx(7, 9, "SAINSBURYS SMKT", "-64.00"),
    _tx(8, 14, "TESCO STORES", "-52.00"),
    _tx(9, 18, "ALDI STORES", "-41.00"),
    _tx(10, 24, "SAINSBURYS SMKT", "-70.00"),
    _tx(11, 6, "ZOPA LOAN REPAYMENT", "-220.00"),
    _tx(12, 7, "NETFLIX.COM", "-12.99"),
    _tx(13, 8, "SPOTIFY P0741", "-11.99"),
    _tx(14, 10, "PRIME VIDEO", "-5.99"),
    _tx(15, 15, "THE PUB COMPANY", "-48.00"),
    _tx(16, 21, "RESTAURANT LE JARDIN", "-55.00"),
    _tx(17, 12, "BET365 UK", "-25.00"),
    _tx(18, 19, "AMAZON MARKETPLACE", "-40.00"),
    _tx(19, 26, "ARGOS RETAIL", "-60.00"),
]

_GAMBLING = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "2600.00"),
    _tx(2, 2, "RENT TO LETTINGS AGENT", "-820.00"),
    _tx(3, 3, "COUNCIL TAX", "-120.00"),
    _tx(4, 4, "EE LIMITED", "-40.00"),
    _tx(5, 5, "TESCO STORES", "-85.00"),
    _tx(6, 11, "SAINSBURYS SMKT", "-72.00"),
    _tx(7, 17, "LIDL GB", "-38.00"),
    _tx(8, 23, "ASDA SUPERSTORE", "-55.00"),
    _tx(9, 7, "NETFLIX.COM", "-12.99"),
    _tx(10, 8, "SPOTIFY", "-11.99"),
    _tx(11, 13, "THE RED LION PUB", "-35.00"),
    _tx(12, 6, "BET365", "-60.00"),
    _tx(13, 9, "SKYBET", "-45.00"),
    _tx(14, 12, "PADDY POWER", "-50.00"),
    _tx(15, 15, "WILLIAM HILL", "-40.00"),
    _tx(16, 18, "BET365", "-35.00"),
    _tx(17, 22, "LADBROKES", "-55.00"),
    _tx(18, 27, "BETFAIR", "-45.00"),
    _tx(19, 20, "AMAZON", "-30.00"),
]

_INDEBTED = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "2900.00"),
    _tx(2, 2, "RENT PAYMENT LANDLORD", "-780.00"),
    _tx(3, 3, "COUNCIL TAX", "-135.00"),
    _tx(4, 4, "BRITISH GAS", "-85.00"),
    _tx(5, 5, "O2 UK", "-30.00"),
    _tx(6, 6, "TESCO", "-80.00"),
    _tx(7, 12, "MORRISONS", "-70.00"),
    _tx(8, 18, "SAINSBURYS", "-75.00"),
    _tx(9, 24, "ALDI", "-45.00"),
    _tx(10, 7, "KLARNA", "-180.00"),
    _tx(11, 9, "CLEARPAY", "-120.00"),
    _tx(12, 11, "ZOPA LOAN", "-260.00"),
    _tx(13, 14, "BARCLAYCARD", "-150.00"),
    _tx(14, 16, "CAPITAL ONE", "-110.00"),
    _tx(15, 19, "CAR FINANCE PLC", "-290.00"),
    _tx(16, 22, "AMEX", "-160.00"),
    _tx(17, 8, "NETFLIX.COM", "-12.99"),
    _tx(18, 20, "THE PUB", "-25.00"),
]

_THIN = [
    _tx(1, 3, "FASTPAY WAGES J DOE", "780.00"),
    _tx(2, 18, "FASTPAY WAGES J DOE", "670.00"),
    _tx(3, 4, "RENT TO LANDLORD", "-820.00"),
    _tx(4, 5, "COUNCIL TAX", "-140.00"),
    _tx(5, 6, "THAMES WATER", "-35.00"),
    _tx(6, 8, "TESCO", "-95.00"),
    _tx(7, 15, "ALDI", "-70.00"),
    _tx(8, 22, "SAINSBURYS", "-105.00"),
    _tx(9, 9, "NETFLIX.COM", "-12.99"),
    _tx(10, 10, "SPOTIFY", "-11.99"),
    _tx(11, 20, "AMAZON", "-25.00"),
]


def _statement(persona_id: str, holder: str, txns: list[Transaction]) -> BankStatementPayload:
    return BankStatementPayload(
        statement_id=f"sample_{persona_id}",
        account_holder=holder,
        account_number="00000000",
        sort_code="40-00-01",
        transactions=txns,
    )


PERSONAS: dict[str, BankStatementPayload] = {
    "stable_earner": _statement("stable_earner", "Alex Stable", _STABLE),
    "gambling_risk": _statement("gambling_risk", "Sam Fielding", _GAMBLING),
    "over_indebted": _statement("over_indebted", "Jo Marsh", _INDEBTED),
    "thin_file": _statement("thin_file", "Riley Novak", _THIN),
}

PERSONA_META: dict[str, dict[str, str]] = {
    "stable_earner": {
        "label": "Stable earner",
        "blurb": "Regular salary, low commitments, a healthy monthly surplus.",
        "expected_decision": "APPROVED",
    },
    "gambling_risk": {
        "label": "Gambling risk",
        "blurb": "Steady income, but betting spend is well above the safe threshold.",
        "expected_decision": "DECLINED",
    },
    "over_indebted": {
        "label": "Over-indebted",
        "blurb": "Most of each pay cheque already goes to loan and card repayments.",
        "expected_decision": "DECLINED",
    },
    "thin_file": {
        "label": "Thin file",
        "blurb": "Irregular gig-work income leaves almost nothing spare each month.",
        "expected_decision": "REFERRED",
    },
}


def list_personas() -> list[SampleSummary]:
    return [
        SampleSummary(
            id=pid,
            label=PERSONA_META[pid]["label"],
            blurb=PERSONA_META[pid]["blurb"],
            expected_decision=PERSONA_META[pid]["expected_decision"],  # type: ignore[arg-type]
        )
        for pid in PERSONAS
    ]


def get_persona(persona_id: str) -> BankStatementPayload:
    return PERSONAS[persona_id].model_copy(deep=True)
