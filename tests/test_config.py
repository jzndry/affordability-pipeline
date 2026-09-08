# tests/test_config.py
from app.config import Settings


def test_defaults_match_spec():
    s = Settings()
    assert s.JOB_BROKER == "celery"
    assert s.MAX_CONCURRENT_ASSESSMENTS == 5
    assert s.MAX_WS_CONNECTIONS == 50
    assert s.MAX_REQUEST_BYTES == 262_144
    assert s.INGEST_PER_IP_LIMIT == "5/minute"
    assert s.INGEST_GLOBAL_LIMIT == "60/minute"
    assert s.EVENT_LOG_TTL_SECONDS == 900


def test_cors_origins_parsed_into_list():
    s = Settings(CORS_ALLOW_ORIGINS="http://localhost:5173, https://example.com")
    assert s.cors_allow_origins_list == ["http://localhost:5173", "https://example.com"]


def test_cors_origins_empty_is_empty_list():
    assert Settings(CORS_ALLOW_ORIGINS="").cors_allow_origins_list == []


def test_job_broker_rejects_unknown_value():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(JOB_BROKER="rabbitmq")
