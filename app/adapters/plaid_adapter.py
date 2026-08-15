import time
from typing import Any, Dict, List, Optional
import plaid
from plaid.api import plaid_api
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.products import Products
from plaid.model.sandbox_item_fire_webhook_request import SandboxItemFireWebhookRequest
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest

from app.config import settings


class PlaidAdapter:
    """Handles communication with the Plaid Sandbox API."""

    def __init__(self) -> None:
        configuration = plaid.Configuration(
            host=plaid.Environment.Sandbox,
            api_key={
                "clientId": settings.PLAID_CLIENT_ID,
                "secret": settings.PLAID_SECRET,
            },
        )
        api_client = plaid.ApiClient(configuration)
        self.client = plaid_api.PlaidApi(api_client)

    def generate_sandbox_access_token(self) -> str:
        """Creates a mock bank account item and returns an access token."""
        public_token_request = SandboxPublicTokenCreateRequest(
            institution_id="ins_109508",  # First Platypus Bank (the plaid sandbox default)
            initial_products=[Products("transactions")],
        )
        public_token_response = self.client.sandbox_public_token_create(public_token_request)
        public_token = public_token_response.public_token

        exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
        exchange_response = self.client.item_public_token_exchange(exchange_request)
        access_token = exchange_response.access_token

        # Instruct Plaid Sandbox to immediately build initial transaction history
        try:
            webhook_request = SandboxItemFireWebhookRequest(
                access_token=access_token,
                webhook_code="DEFAULT_UPDATE",
            )
            self.client.sandbox_item_fire_webhook(webhook_request)
        except Exception:
            pass

        return access_token

    def fetch_raw_transactions(
        self, access_token: str, max_retries: int = 5, retry_delay: float = 1.5
    ) -> List[Dict[str, Any]]:
        """
        Fetches all transactions using cursor sync.
        Retries briefly if Plaid's sandbox workers have not finished populating data.
        """
        for attempt in range(max_retries):
            cursor: Optional[str] = None
            has_more = True
            all_added: List[Dict[str, Any]] = []

            while has_more:
                request = (
                    TransactionsSyncRequest(access_token=access_token, cursor=cursor)
                    if cursor
                    else TransactionsSyncRequest(access_token=access_token)
                )
                response = self.client.transactions_sync(request)

                for tx in response.added:
                    all_added.append(tx.to_dict())

                has_more = response.has_more
                cursor = response.next_cursor

            if all_added:
                return all_added

            # If transactions are not ready yet, wait briefly before retrying
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

        return []