# ADR 006: Real-Time Underwriting Decision Streaming via WebSockets and Redis Pub/Sub

## Status
Accepted

## Context
Underwriter dashboards and checkout applications need immediate notification when an asynchronous credit assessment finishes. HTTP polling wastes network bandwidth and introduces latency.

## Decision
We will use native WebSockets managed by FastAPI, backed by Redis Pub/Sub, to push completed assessment events directly to connected clients in real time.

## Consequences

### Advantages (Why this is better)
* **Instant Delivery:** Underwriters receive decisions the instant the background worker finishes (<5ms).
* **Zero Polling Overhead:** Eliminates repetitive HTTP requests and database/Redis polling lookups.
* **Horizontal Scalability:** Redis Pub/Sub allows multiple API gateway instances to broadcast events to clients across different servers.

### Disadvantages (The trade-offs)
* **Connection State Management:** The server must handle disconnects, reconnects, and stale socket cleanup.