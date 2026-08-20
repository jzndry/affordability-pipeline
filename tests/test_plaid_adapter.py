from unittest.mock import MagicMock, patch

import pytest

from app.adapters.plaid_adapter import PlaidAdapter

# UNIT TEST (Fast, Mocked, Offline - Ideal for GitHub Actions CI)


def test_plaid_pagination_loop():
    """
    Unit Test: Verifies that fetch_raw_transactions continues fetching pages
    until 'has_more' is False.
    """
    with patch("app.adapters.plaid_adapter.plaid_api.PlaidApi") as mock_api_class:
        mock_client = MagicMock()
        mock_api_class.return_value = mock_client

        # Page 1: 1 item, has_more=True
        page_1 = MagicMock()
        item_1 = MagicMock()
        item_1.to_dict.return_value = {
            "transaction_id": "tx_01",
            "name": "Tesco Superstore",
            "amount": 42.50,
            "date": "2026-08-10",
            "iso_currency_code": "GBP",
        }
        page_1.added = [item_1]
        page_1.has_more = True
        page_1.next_cursor = "cursor_page_2"

        # Page 2: 1 item, has_more=False
        page_2 = MagicMock()
        item_2 = MagicMock()
        item_2.to_dict.return_value = {
            "transaction_id": "tx_02",
            "name": "Sky Bet",
            "amount": 20.00,
            "date": "2026-08-11",
            "iso_currency_code": "GBP",
        }
        page_2.added = [item_2]
        page_2.has_more = False
        page_2.next_cursor = "cursor_end"

        # Instruct mock client to return page 1, then page 2
        mock_client.transactions_sync.side_effect = [page_1, page_2]

        adapter = PlaidAdapter()
        results = adapter.fetch_raw_transactions(access_token="mock_access_token")

        # Assertions
        assert len(results) == 2, "Expected 2 transactions collected across both pages"
        assert results[0]["transaction_id"] == "tx_01"
        assert results[1]["transaction_id"] == "tx_02"
        assert mock_client.transactions_sync.call_count == 2


# INTEGRATION TEST (Live Plaid Sandbox API)


def test_live_plaid_sandbox_integration(
    plaid_adapter: PlaidAdapter, has_valid_plaid_credentials: bool
):
    """
    Integration Test:
    - Authenticates against Plaid Sandbox.
    - Generates a sandbox item access token.
    - Fetches live transactions via cursor sync.
    - Asserts that the returned data matches expected financial schema.
    """
    if not has_valid_plaid_credentials:
        pytest.skip("Plaid credentials missing or default in .env - skipping live test.")

    # 1. Generate sandbox access token
    access_token = plaid_adapter.generate_sandbox_access_token()
    assert access_token is not None
    assert access_token.startswith("access-sandbox-"), (
        "Token must be a valid Plaid sandbox access token"
    )

    # 2. Fetch transactions via cursor sync
    transactions = plaid_adapter.fetch_raw_transactions(access_token=access_token)

    # 3. Verify data integrity
    assert isinstance(transactions, list), "Expected transactions to be returned as a list"
    assert len(transactions) > 0, "Plaid Sandbox should return at least one transaction"

    first_tx = transactions[0]
    assert "transaction_id" in first_tx, "Transaction must contain a transaction_id"
    assert "amount" in first_tx, "Transaction must contain an amount field"
    assert "date" in first_tx, "Transaction must contain a date"
    assert first_tx["amount"] is not None
