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

## Update (2026-09)
Pub/Sub alone drops any event published before a subscriber attaches, so a WebSocket
client that connects mid-job missed everything up to that point. Delivery is now backed
by a per-job replay log: every pipeline event is appended to a Redis list
(`underwriting_events:{job_id}`, 900s TTL) as well as published. A subscriber first
replays that list (de-duplicated by sequence number) and then tails Pub/Sub, so a late
subscriber still receives the full ordered sequence from `RECEIVED` onward.

The same delivery contract is also available without Redis. An `InProcessBroker`
variant keeps the replay log and subscriber fan-out in memory and runs the pipeline on
the web server's event loop, so the free single-instance deploy gets the identical
event stream with no Celery or Redis dependency. Both brokers implement one `JobBroker`
interface and are selected by the `JOB_BROKER` environment variable.