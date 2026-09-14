# Backend

A plain English walkthrough of this branch right now. This is the system the frontend work is going to be built against. This is so I can understand how to implement my frontend but also improve my documentation ability.

---

## The shape of it, in few sentences

A bank statement comes in, a background worker scores it, and the result gets pushed onto the browser the moment it is ready. 

There are four main pieces. The web gateway at `app/main.py`, the domain logic files `app/core/`, the background worker `app/worker.py` and the live results feed `app/api/v1/websockets.py`. 

Redis sits underneath the last three, doing two unrelated jobs at once — it's both the task queue and the Celery pulls from, and the announcement channel the WebSocket listens on.

---

## The web gateway (`app/main.py`)

FastAPI exposes four endpoints:

- `POST /api/v1/statements/ingest` : This validates the statement coming in. It checks it is not absurdly large and hands it to Celery. It returns a `job_id` — typically in single digit milliseconds.
- `GET /api/v1/statements/jobs/{job_id}` : A polling fallback. It asks Celery/Redis directly whether it is done yet. Exists for anyone not using the WebSocket.
- `WS /api/v1/ws/underwriting/{job_id}` : The live feed (see below).
- `GET /health` : A standard liveness check for what will be hosting this.


FastAPI actually also builds `/docs`. This is an interactive page listing every endpoint, generated automatically from the same type hints and Pydantic models the code already uses, so the documentation can't drift out of sync with the actual API.


---

## The domain logic (`app/core/`) — this part should remain constant

This is the actual "brain" of the system, and it's deliberately boring in the best way: given the same statement, it always produces the same answer. So it is objective and deterministic

**`models.py`** is an initial verifier. Before anything is scored, Pydantic checks the shape of the incoming JSON — an account holder, a sort code, a list of transactions with real dates and amounts — and rejects anything malformed before it goes anywhere near the scoring logic. Every money value is a `Decimal`, never a `float`, because `0.1 + 0.2` famously does not equal `0.3` in floating point. In financial systems, this is an error which cannot occur.

**`categoriser.py`** turns messy real bank text, like `POS 4829 14OCT26 BET365 UK`, into something useful. It strips the noise (card reference numbers, dates, punctuation) down to `BET365`, then matches what's left against a dictionary of patterns: `BET365` → Gambling, `TESCO` → Groceries, `KLARNA` → Loan repayment, and so on. It's plain regular expressions on purpose — every categorisation can be explained line-by-line if a decision is ever challenged, and it runs in well under a millisecond per transaction.

**`affordability.py`** is the actual policy. Categorised transactions get summed into buckets — income, essential spending, discretionary spending, gambling, debt repayments. A fixed set of rules decides the outcome, the rules that I have set are described in the table below.

| Warning sign                       | Trigger      | Result                      |
| ---------------------------------- | ------------ | --------------------------- |
| No verifiable income               | income is £0 | **DECLINED**                |
| Gambling as a share of income      | ≥ 10% / ≥ 5% | **DECLINED** / **REFERRED** |
| Existing debt as a share of income | ≥ 40%        | **DECLINED**                |
| Money left over each month         | < £150       | **REFERRED**                |

Every non-approval comes with a plain-English risk flag, like
`HIGH_DEBT_TO_INCOME_RATIO (45.0%)` — nothing about the decision is a black
box.

---

## The background worker (`app/worker.py`)

Celery is the worker that actually runs the two functions above, off the main thread, so the web server itself never slows down while a statement is being scored. Right now this logic lives *inside* the Celery task itself — parse, categorise, score, and publish the result are all one function, written specifically for Celery. That's fine for a single, always-on distributed deployment; it's the reason a Redis-free "lite" deploy mode doesn't exist yet on this branch.

When a job finishes, the worker does one more thing: it publishes a single message —

```json
{"event": "ASSESSMENT_COMPLETED", "job_id": "...", "data": { ...the decision... }}
```

— to a Redis Pub/Sub channel named `underwriting_jobs:{job_id}`. That message is the *only* thing anyone downstream ever hears about this job; there's no "I've started categorising" or "I'm scoring now" announcement in between.

---

## The live results feed (`app/api/v1/websockets.py`)

A WebSocket is a connection that stays open, so instead of the browser repeatedly asking "finished yet?", the server can push the answer through the instant it's ready. The endpoint here has to handle one genuine race condition: what if the job finishes *before* the browser finishes connecting?

It solves that with a two-step check, in order:

1. **On connect**, ask Celery directly: has this job already finished? If yes, send the stored result immediately and close — no Redis involved. 
2. 2. **If not**, subscribe to that job's Redis Pub/Sub channel and wait, polling for a message every 50ms, until either the worker publishes the completion event or the Celery result flips to "done" on its own.

This works, but it's a hand-rolled race-condition check rather than a general mechanism — it only knows how to catch the *one* moment of overlap between "the job just finished" and "the socket just connected." It doesn't have a way to tell a browser "here's everything that already happened" if more than one thing happens before it connects, because right now only one thing (`ASSESSMENT_COMPLETED`) ever happens at all.

---

## What's deliberately switched off

**`app/adapters/plaid_adapter.py`** is a real, working connector to Plaid's sandbox (a test bank with fake data) — Plaid is the service that lets an app securely pull a user's actual bank transactions. It exists and is tested,but nothing in `main.py` calls it; it's not wired into any route, so it can't be reached by a visitor. It stays that way until there's a plan for using it safely in a public demo.

---

## Running it

`docker-compose.yml` starts three containers together: `redis`, the FastAPI `api` gateway, and the Celery `worker` — all talking to the same Redis instance over the compose network. `Procfile` describes the same two processes (`web` and `worker`) for a hosting platform like Heroku that reads Procfiles directly. Both assume Redis is always there; there's currently no environment switch to run this pipeline any other way.

---

## Tests

`pytest` covers the categoriser and the decision engine directly (a prime borrower gets approved, a gambling-heavy statement gets declined, and so on), plus the ingestion endpoint and the WebSocket's two connection scenarios (job already finished vs. job still running) using mocked Celery and Redis. `mypy --strict` and `ruff` run in CI on every push. There's no integration test that spins up a real Redis and Celery worker end-to-end yet — everything above the categoriser/engine layer is tested against
mocks.

---

## The frontend, as it stands here

`app/frontend/` is currently stock Vite+React boilerplate.
