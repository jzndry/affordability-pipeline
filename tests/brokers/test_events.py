from app.brokers.events import TERMINAL_EVENTS, PipelineEvent


def test_event_defaults_are_filled():
    e = PipelineEvent(event="CATEGORISING")
    assert e.seq == 0
    assert e.job_id == ""
    assert isinstance(e.ts, float) and e.ts > 0
    assert e.is_terminal is False


def test_decided_is_terminal_and_carries_data():
    e = PipelineEvent(event="DECIDED", job_id="j1", seq=4, data={"decision": "APPROVED"})
    assert e.is_terminal is True
    assert e.data == {"decision": "APPROVED"}


def test_failed_is_terminal():
    assert PipelineEvent(event="FAILED", error="boom").is_terminal is True


def test_terminal_set_contents():
    assert TERMINAL_EVENTS == frozenset({"DECIDED", "FAILED"})


def test_round_trips_through_json():
    e = PipelineEvent(event="SCORING", job_id="j1", seq=2)
    assert PipelineEvent.model_validate(e.model_dump(mode="json")) == e
