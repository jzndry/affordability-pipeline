# ADR 005: Asynchronous Task Queuing via Celery and Redis

## Status
Under review

## Context
Running categorisation and risk evaluations synchronously on the API thread degrades throughput and increases request latency. We need an asynchronous queuing architecture to achieve sub-50ms acknowledgment times.

## Decision
We will use Celery with a Redis message broker and result backend to handle transaction ingestion and scoring tasks asynchronously.

## Consequences

### Advantages
* **Sub-50ms Latency:** The API immediately responds with HTTP 202 Accepted and a `job_id`.
* **Process Isolation:** Heavy CPU processing does not degrade the asynchronous FastAPI event loop.
* **Fault Tolerance:** Built-in task retries and state tracking in Redis.

### Disadvantages
* **Infrastructure Overhead:** Requires running a Redis broker and managing independent worker processes.