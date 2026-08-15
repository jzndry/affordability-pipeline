# Roadmap

# Objective (End goal) 

A high-throughput, non-blocking! financial service capable of:
- Ingesting bank statements with sub-50ms API response times.
- Processing complex categorisation and risk calculations off the main thread.
- Streaming live underwriting decisions (APPROVED, REFERRED, DECLINED) directly to client dashboards over WebSockets.

![Ss of project](project_overview.png)

# Implementation points of concern

Zero Floating-Point Drift : Add Decimal

Asynchronous Non-Blocking I/O: Decouple statement uploads from compute-heavy processing via Celery and Redis.

Deterministic & Auditable Scoring: Rule-based categorisation and risk matrix yielding explicit, human-readable risk flags for FCA compliance.

Real-Time Push Decisions: Bi-directional WebSockets instead of client polling.

Production CI/CD Quality: 90%+ test coverage, strict linting (ruff), static typing (mypy), Dockerised orchestration, and GitHub Actions automation.

# Phases of implementation 

## Stage 1: Ingestion & Open Banking Adapter [COMPLETED]

Objective: Establish a secure link to Plaid Sandbox and handle transaction ingestion.


## Stage 2: Core Domain Logic & Risk Engine 

Objective: Build deterministic transaction sanitisation, categorisation, and underwriting scoring models.

## Stage 3: Asynchronous Task Queue (FastAPI + Celery + Redis)

Objective: Offload heavy statement processing to background workers to guarantee sub-50ms API responses.

## Stage 4: Real-Time WebSockets & API Gateway

Objective: Stream completed underwriting decisions to connected underwriter dashboards.

## Stage 5: Benchmarking & Performance Profiling

Objective: Measure and validate performance metrics against project claims.

## Stage 6: Containerisation, CI/CD & Repository Polish

Objective: Ensure one-command reproducibility and automated build verification.