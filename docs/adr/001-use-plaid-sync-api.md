# ADR 001: Use Plaid `/transactions/sync` for Bank Data Ingestion

## Status
Accepted

## Context
We need to ingest bank transaction data from Open Banking accounts to perform credit affordability and risk assessments. We must choose how to retrieve this data from Plaid reliably without missing historical updates, modified transactions, or pending-to-settled status shifts.

## Decision
We will use Plaid's modern  `/transactions/sync` endpoint rather than the legacy `/transactions/get` endpoint or an immediate webhook-only architecture.

## Consequences

### Advantages 
* **Works like a digital bookmark:** The API uses a "cursor" (a bookmark token). Every time we sync, Plaid tells us exactly what was added, modified, or removed since our last bookmark.
* **No missing or duplicate data:** The older method asked for date ranges (e.g. "give me the last 30 days"). If a bank delayed or retroactively edited a transaction, date queries could easily miss it or duplicate it. The sync endpoint prevents this entirely.
* **Simpler automated testing:** We can trigger synchronisations directly from our backend code without needing external public webhook URLs.

### Disadvantages 
* **We must save the bookmark:** Our application has to store the latest cursor string so it knows where to pick up on the next sync call.
* **Slightly more code upfront:** We need logic to handle added, updated, and deleted transactions separately, rather than reading a single flat list.