import pytest

from app.adapters.plaid_adapter import PlaidAdapter
from app.config import settings


@pytest.fixture
def plaid_adapter() -> PlaidAdapter:
    """Fixture to provide a configured PlaidAdapter instance."""
    return PlaidAdapter()


@pytest.fixture
def has_valid_plaid_credentials() -> bool:
    """Checks whether the user has set any plaid credentials in .env."""
    return bool(settings.PLAID_CLIENT_ID and settings.PLAID_SECRET)
