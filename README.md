# Open Banking Credit Affordability Engine

A real-time financial pipeline that takes in bank statements, classifies every transaction, computes debt-to-income and risk metrics, and streams an underwriting decision (**APPROVED / REFERRED / DECLINED**) back to the client over a live WebSocket connection.

---

## The reason behind the project

Lenders have to check whether a borrower can actually afford a loan. To do that properly you read their bank statements: how much comes in, how much goes out, and whether there are warning signs such as heavy gambling or too much existing debt. This project performs that check **automatically and in real time**.


Scoring a statement is real computational work. If the web server did that work while the user waited, the site would feel slow and would collapse under load. So the work is split in three:

1. The web server **accepts the statement and immediately returns a job ticket** which takes milliseconds and never slows down.
2. A separate **background worker** does the heavy scoring, off the main thread.
3. The moment the worker finishes, the decision is **pushed live to the client**. No polling, no page refresh.

The pattern of doing a fast, process in the background and then streaming the result is the core of the project. It is how production systems stay responsive under pressure.

---

## How a request flows through the system

```
  Client uploads a statement
        │
        ▼
  ┌───────────────┐   HTTP 202 Accepted + job_id        (responds in milliseconds)
  │  FastAPI      │ ─────────────────────────────────▶  Client
  │  web gateway  │
  └───────┬───────┘
          │  enqueue job
          ▼
  ┌───────────────┐   Redis acts as the job queue
  │     Redis     │◀────────────────────────────┐
  └───────┬───────┘                             │  publish: "job done"
          │  worker picks up the job            │
          ▼                                     │
  ┌───────────────┐                             │
  │ Celery worker │  clean → categorise → score → decide
  └───────────────┘                             │
          │ ────────────────────────────────────┘
          ▼
  ┌───────────────┐   WebSocket connection stays open
  │  WebSocket    │ ─────────────────────────────────▶  Decision appears
  │  gateway      │                                     on the client instantly
  └───────────────┘
```
---

## Each part of the codebase, in a more simplified manner

### 1. FastAPI; the front door (`app/main.py`)

The web server. It exposes a small set of endpoints:

| Endpoint | Purpose |
| --- | --- |
| `POST /api/v1/statements/ingest` | Accepts a statement, hands the work off, returns a `job_id`. Responds in milliseconds. |
| `GET /api/v1/statements/jobs/{job_id}` | Polling fallback; "is this job finished yet?" |
| `GET /api/v1/statements/samples` | Lists the built-in persona fixtures and their expected decision. |
| `GET /api/v1/statements/samples/{id}` | Returns the full bank statement payload for one persona. |
| `WS /api/v1/ws/underwriting/{job_id}` | Live results feed (see section 7). |
| `GET /health` | Used by hosting platforms to check if the server is up. |

FastAPI also generates its own interactive API documentation at `/docs` due to its in built SwaggerUI

### 2. Pydantic; the bouncer (`app/core/models.py`)

Before a statement is accepted, Pydantic validates its shape: an account holder, a sort code, and a list of transactions with dates and amounts. Malformed data is rejected immediately with a clear error and never reaches the scoring logic.

### 3. Exact decimal money handling

Computers are poor at decimals. `0.1 + 0.2` evaluates to `0.30000000000000004`. For money that is unacceptable. Every financial value in this project uses Python's `Decimal` type, so all totals are exact to the penny.Financial auditing rules expect this. 
*See [`docs/adr/002-decimal-financial-modelling.md`](docs/adr/002-decimal-financial-modelling.md)*

### 4. The categoriser; making sense of messy bank text (`app/core/categoriser.py`)

Real bank descriptions look like `POS 4829 14OCT26 BET365 UK`. This component:

- **Strips the noise** (card codes, reference numbers, dates) to leave `BET365`.
- **Matches it to a category** using a dictionary of patterns: `BET365` &rarr; Gambling, `TESCO` &rarr; Groceries, `KLARNA` &rarr; Loan repayment, and so on.

It is rule-based on purpose. No machine learning needed as it is set in stone. Every categorisation can
therefore be explained and audited, and it runs in well under a millisecond.
*See [`docs/adr/003-regex.md`](docs/adr/003-regex.md)*

### 5. The affordability engine; the credit decision maker (`app/core/affordability.py`)

Once transactions are categorised, they are summed into buckets (income, essential spending, discretionary spending, gambling, debt repayments) and a fixed set of rules is applied:

| Warning sign | Trigger | Result |
| --- | --- | --- |
| No verifiable income | income is £0 | **DECLINED** |
| Gambling as a share of income | ≥ 10% / ≥ 5% | **DECLINED** / **REFERRED** |
| Existing debt as a share of income | ≥ 40% | **DECLINED** |
| Money left over each month | < £150 | **REFERRED** |

Every non-approval carries a plain-English **risk flag** (e.g. `HIGH_DEBT_TO_INCOME_RATIO (45.0%)`). The same statement always produces the same decision so it is fully deterministic, not a black box.
*See [`docs/adr/004-engine-design.md`](docs/adr/004-engine-design.md)*

### 6. Celery + Redis; the background workforce (`app/worker.py`)

- **Redis** is an in-memory data store used here as a **job queue**. The web server drops jobs into it.
- **Celery** is the **worker** that pulls jobs off the queue and runs the scoring, entirely separately from the web server.

Because scoring happens here, the web server itself never slows down which means it keeps accepting uploads at full speed while statements are processed in the background. 
*See [`docs/adr/005-redis-celery.md`](docs/adr/005-redis-celery.md)*

### 7. WebSockets + Redis Pub/Sub; the live results feed (`app/api/v1/websockets.py`)

Normally a browser has to keep asking "finished yet?". A **WebSocket** is a connection that stays open, so the server can tell the browser the instant the result is ready.

**Redis Pub/Sub** is the internal announcement channel: as the worker moves through a job it publishes each stage, and the WebSocket listening for that `job_id` forwards them to the right client. Every stage is also written to a short-lived per-job replay log (`underwriting_events:{job_id}`, kept for 900 seconds), so a client that connects late still receives the full ordered sequence from the beginning. *See [`docs/adr/006-real-time-decision-streaming.md`](docs/adr/006-real-time-decision-streaming.md)*

The stream uses a fixed vocabulary of event names: `RECEIVED`, `CATEGORISING`, `SCORING`, `DECIDED`, `FAILED` (plus a one-off `SUBSCRIBED` frame when the socket opens). This replaces the earlier single `ASSESSMENT_COMPLETED` event.

### 7a. Two ways to run the pipeline

The web layer talks to a single `JobBroker` interface; which implementation it gets is chosen by the `JOB_BROKER` environment variable.

- **Distributed (`JOB_BROKER=celery`)** — the full stack. `docker compose up` starts the web gateway, a Celery worker, and Redis; jobs are queued through Redis and scored on the worker, with results streamed back over Redis Pub/Sub. This is the production-shaped setup.
- **In-process (`JOB_BROKER=inprocess`)** — no Celery, no Redis. The pipeline runs on the web server's event loop with in-memory fan-out to WebSocket subscribers. The deployed demo uses this so it can run on a single free instance.

Both modes expose exactly the same API, event stream, and behaviour.

### 7b. The deployed demo

The public demo runs the in-process mode on a single instance behind Cloudflare, which is the sole ingress and terminates TLS in front of the app.

### 8. Plaid adapter; connecting to (real) bank data (`app/adapters/plaid_adapter.py`)

**Plaid** is a service that lets applications securely pull a user's bank transactions which is the same "Open Banking" mechanism budgeting apps use. This project includes a working connector to Plaid's **sandbox** (a test bank with synthetic data). It is disabled in the public demo, for now until I can add it safely (protected against malicious use). 
*See [`docs/adr/001-use-plaid-sync-api.md`](docs/adr/001-use-plaid-sync-api.md)*

### 9. Rate limiting

A safety valve: each client IP may upload at most 10 statements per minute, which keeps the public demo from being overwhelmed or run up as a cost. This is what I will be adding to the real demo soon.

### 10. Automated quality checks

| Tool | What it does |
| --- | --- |
| **pytest** | Test suite proving the categoriser and decision engine give correct answers for known cases (a prime borrower is approved, a gambling-heavy statement is declined, etc.). |
| **mypy** | Static type checker, run in `strict` mode; catches type errors before the code runs. |
| **ruff** | Enforces a consistent code style automatically. |

### 11. Benchmarking , testing the processing speed (`benchmarks/`)

- **`benchmark_engine.py`** measures raw speed. milliseconds to score a 1,000-transaction statement and sustained transactions/second.
- **`locustfile.py`** simulates many concurrent uploads to validate the "fast under load" claim.

### 12. Docker + Docker Compose

**Docker** packages the app with everything it needs so it runs identically anywhere. **Docker Compose** starts every piece together web gateway, worker, and Redis with a single command.

### 13. GitHub Actions; the automated quality gate, makes sure the tests are run (`.github/workflows/ci.yml`)

On every push, GitHub automatically runs the linter, the type checker, and the full test suite. Failures are flagged before code is merged. I do need to add some full integration tests soon.

### 14. The frontend dashboard (`app/frontend/`)

The interactive dashboard (React + Vite) is under active development. It will let you pick a sample customer, watch the statement get processed live over the WebSocket, and see the decision with its risk flags, alongside a walkthrough of the architecture above.

---

## Quick start (Will be using Docker)

Under development, there will be a step by step instruction on how to run this project on your machine, but for the time being, the focus is on producing a live demo. 

## Project structure

```
app/
  main.py                  FastAPI gateway: routes, validation, rate limiting
  worker.py                Celery worker and the underwriting task
  config.py                Environment-driven settings
  core/
    models.py              Pydantic domain models (Decimal money types)
    categoriser.py         Rule-based transaction cleaning and categorisation
    affordability.py       Deterministic risk and affordability decision engine
  api/v1/
    websockets.py          Real-time decision streaming (WebSocket + Redis Pub/Sub)
  adapters/
    plaid_adapter.py       Open Banking data ingestion (Plaid sandbox)
  frontend/                React + Vite dashboard (in development)
tests/                     pytest suite
benchmarks/                CPU benchmark and Locust load test
docs/adr/                  Architecture Decision Records
```

---

## Design decisions

Each significant choice is recorded as an Architecture Decision Record in
[`docs/adr/`](docs/adr/):

| ADR | Decision |
| --- | --- |
| 001 | Use Plaid `/transactions/sync` for bank data ingestion |
| 002 | Exact `Decimal` representation for all financial calculations |
| 003 | Rule-based regular-expression categorisation (no ML) |
| 004 | Deterministic policy matrix for credit decisioning |
| 005 | Asynchronous task queuing via Celery and Redis |
| 006 | Real-time decision streaming via WebSockets and Redis Pub/Sub |
