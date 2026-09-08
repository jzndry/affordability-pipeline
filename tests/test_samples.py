import pytest
from fastapi.testclient import TestClient

from app.core.pipeline import run_underwriting_pipeline
from app.core.samples import PERSONA_META, PERSONAS, get_persona, list_personas
from app.main import app

EXPECTED = {
    "stable_earner": ("APPROVED", None),
    "gambling_risk": ("DECLINED", "EXCESSIVE_GAMBLING_RISK"),
    "over_indebted": ("DECLINED", "HIGH_DEBT_TO_INCOME_RATIO"),
    "thin_file": ("REFERRED", "LOW_NET_DISPOSABLE_INCOME"),
}


@pytest.mark.parametrize("persona_id", list(EXPECTED))
def test_persona_produces_documented_decision(persona_id: str):
    decision, flag_prefix = EXPECTED[persona_id]
    payload = get_persona(persona_id)
    assessment = run_underwriting_pipeline(payload, lambda _e: None)
    assert assessment.decision.value == decision
    if flag_prefix is None:
        assert assessment.risk_flags == []
    else:
        assert any(f.startswith(flag_prefix) for f in assessment.risk_flags)
        assert len(assessment.risk_flags) == 1  # one headline cause per persona


def test_meta_matches_pipeline_outcome():
    for persona_id, meta in PERSONA_META.items():
        assert meta["expected_decision"] == EXPECTED[persona_id][0]


def test_list_endpoint_returns_four_summaries():
    body = TestClient(app).get("/api/v1/statements/samples").json()
    assert {s["id"] for s in body} == set(PERSONAS)
    assert all({"id", "label", "blurb", "expected_decision"} <= s.keys() for s in body)


def test_detail_endpoint_returns_valid_payload():
    body = TestClient(app).get("/api/v1/statements/samples/stable_earner").json()
    assert body["statement_id"]
    assert len(body["transactions"]) >= 10


def test_detail_endpoint_404_for_unknown_persona():
    assert TestClient(app).get("/api/v1/statements/samples/nope").status_code == 404


def test_list_personas_returns_summary_per_persona():
    summaries = list_personas()
    assert {s.id for s in summaries} == set(PERSONAS)
    assert all(s.expected_decision in {"APPROVED", "REFERRED", "DECLINED"} for s in summaries)
