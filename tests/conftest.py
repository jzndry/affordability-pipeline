import pytest

from app.adapters.plaid_adapter import PlaidAdapter
from app.config import settings
from app.ratelimit import limiter


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Clear slowapi's fixed-window storage between tests so the per-IP 5/minute
    ingest limit does not accumulate across the suite."""
    yield
    reset = getattr(limiter, "reset", None)
    if callable(reset):
        reset()
        return
    storage = getattr(limiter, "_storage", None)  # pragma: no cover - fallback
    inner = getattr(storage, "storage", None)
    if inner is not None:
        inner.clear()


@pytest.fixture
def plaid_adapter() -> PlaidAdapter:
    """Fixture to provide a configured PlaidAdapter instance."""
    return PlaidAdapter()


@pytest.fixture
def has_valid_plaid_credentials() -> bool:
    """Checks whether the user has set any plaid credentials in .env."""
    return bool(settings.PLAID_CLIENT_ID and settings.PLAID_SECRET)
