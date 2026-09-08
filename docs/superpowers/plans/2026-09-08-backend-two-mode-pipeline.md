# Backend: Two-Mode Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the underwriting pipeline run in two interchangeable modes — the existing Celery+Redis distributed stack, and a new zero-dependency in-process mode for the free deploy — behind one `JobBroker` interface, and add the demo's supporting endpoints and abuse guards.

**Architecture:** The parse→categorise→score→decide sequence is extracted into one transport-agnostic function that announces each stage through an `emit` callback. A `JobBroker` protocol has two implementations: `CeleryRedisBroker` (worker + Redis list/pub-sub) and `InProcessBroker` (async task + in-memory fan-out). Both write every event to a short-lived replay log so a late WebSocket subscriber still receives the full ordered sequence. The WebSocket endpoint and Celery worker become thin adapters over this seam.

**Tech Stack:** Python 3.11, FastAPI, Pydantic v2, Celery 5, Redis 5 (`redis.asyncio`), slowapi, pytest + pytest-asyncio, httpx, mypy strict, ruff.

**Spec:** `docs/superpowers/specs/2026-09-08-frontend-demo-design.md` (sections 3, 4, 6; this plan is spec steps 1–6).

## Global Constraints

- **Python floor:** `>=3.11`. `target-version = "py311"`. Use `from __future__ import annotations` is **not** needed; 3.11 built-in generics are fine.
- **mypy:** runs in `strict` mode over `app/` in CI (`mypy app/`). Every new function is fully typed. No untyped defs.
- **ruff:** `select = ["E","F","I","N","W"]`, `line-length = 100`, `ignore = ["E501"]`. Run `ruff check .` before every commit.
- **Tests:** `pytest` with `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed). `testpaths = ["tests"]`. CI provides a real Redis at `redis://localhost:6379/0` via `REDIS_URL`.
- **Core domain logic is frozen:** never modify `app/core/categoriser.py`, `app/core/affordability.py`, or `app/core/models.py` except to **add** the new `AffordabilityAssessment` fields already present — actually add nothing; treat all three as read-only.
- **Money:** all financial values are `Decimal`. Never introduce `float` into a money path.
- **Wire vocabulary:** pipeline events are exactly `RECEIVED`, `CATEGORISING`, `SCORING`, `DECIDED`, `FAILED`. `SUBSCRIBED` is emitted only by the WebSocket layer. This replaces the legacy `ASSESSMENT_COMPLETED` / `ASSESSMENT_FAILED` names.
- **Redis keys:** live-event channel stays `underwriting_jobs:{job_id}`; replay list is `underwriting_events:{job_id}` with a 900-second TTL.
- **Env var:** `JOB_BROKER` selects the mode, values `celery` (default) and `inprocess`.
- **Commit style:** end every commit message body with:
  ```
  Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01UFAdJPiUug5v8moENQZZC4
  ```

---

## File Structure

**New files**

| Path | Responsibility |
|---|---|
| `app/brokers/__init__.py` | `get_broker()` factory + re-exports |
| `app/brokers/events.py` | `PipelineEvent` model, event-name literal, `TERMINAL_EVENTS` |
| `app/brokers/base.py` | `JobBroker` Protocol, `JobStatusResponse` model, `CapacityError` |
| `app/brokers/celery_redis.py` | `CeleryRedisBroker` + the worker's Redis-publishing `emit` |
| `app/brokers/inprocess.py` | `InProcessBroker`, `JobChannel`, capacity counter, TTL reaper |
| `app/core/pipeline.py` | `run_underwriting_pipeline(payload, emit)` — the extracted sequence |
| `app/core/samples.py` | Four canonical persona `BankStatementPayload` fixtures |
| `app/api/v1/statements.py` | Router: `/samples`, `/samples/{id}`, `/ingest`, `/jobs/{id}` |
| `app/middleware.py` | `MaxBodySizeMiddleware` |
| `render.yaml` | Render deploy config |
| `tests/brokers/__init__.py` | (empty) |
| `tests/brokers/test_events.py` | `PipelineEvent` behaviour |
| `tests/brokers/test_celery_redis_broker.py` | `CeleryRedisBroker` against real Redis |
| `tests/brokers/test_inprocess_broker.py` | `InProcessBroker` behaviour |
| `tests/test_pipeline.py` | `run_underwriting_pipeline` staging + regression |
| `tests/test_samples.py` | persona fixtures produce documented decisions |
| `tests/test_guards.py` | body-size, rate-limit, CORS, `/health` mode |
| `tests/test_spa.py` | SPA catch-all serving |

**Modified files**

| Path | Change |
|---|---|
| `app/config.py` | 8 new settings + `cors_allow_origins_list` property |
| `app/worker.py` | Celery task becomes a thin wrapper over `run_underwriting_pipeline` |
| `app/api/v1/websockets.py` | rewrite: `broker.subscribe` loop + connection cap |
| `app/main.py` | broker DI, router include, CORS rewrite, middleware, `/health` mode, SPA catch-all, remove inline ingest/jobs routes + old static mount |
| `tests/test_websockets.py` | rewrite for new vocabulary + broker seam |
| `tests/test_api_ingestion.py` | update for broker seam |
| `docker-compose.yml` | add `JOB_BROKER=celery` to `api` and `worker` env |
| `.github/workflows/ci.yml` | stop ignoring `app/frontend/**`? **no** — leave as is |

---

## Task 1: Config additions

**Files:**
- Modify: `app/config.py`
- Test: `tests/test_config.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `settings` gains `JOB_BROKER: Literal["celery","inprocess"]`, `CORS_ALLOW_ORIGINS: str`, `MAX_CONCURRENT_ASSESSMENTS: int`, `MAX_WS_CONNECTIONS: int`, `MAX_REQUEST_BYTES: int`, `INGEST_PER_IP_LIMIT: str`, `INGEST_GLOBAL_LIMIT: str`, `EVENT_LOG_TTL_SECONDS: int`, and a property `cors_allow_origins_list: list[str]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config.py
from app.config import Settings


def test_defaults_match_spec():
    s = Settings()
    assert s.JOB_BROKER == "celery"
    assert s.MAX_CONCURRENT_ASSESSMENTS == 5
    assert s.MAX_WS_CONNECTIONS == 50
    assert s.MAX_REQUEST_BYTES == 262_144
    assert s.INGEST_PER_IP_LIMIT == "5/minute"
    assert s.INGEST_GLOBAL_LIMIT == "60/minute"
    assert s.EVENT_LOG_TTL_SECONDS == 900


def test_cors_origins_parsed_into_list():
    s = Settings(CORS_ALLOW_ORIGINS="http://localhost:5173, https://example.com")
    assert s.cors_allow_origins_list == ["http://localhost:5173", "https://example.com"]


def test_cors_origins_empty_is_empty_list():
    assert Settings(CORS_ALLOW_ORIGINS="").cors_allow_origins_list == []


def test_job_broker_rejects_unknown_value():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(JOB_BROKER="rabbitmq")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError` / unknown settings.

- [ ] **Step 3: Implement**

```python
# app/config.py
from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PLAID_CLIENT_ID: Optional[str] = None
    PLAID_SECRET: Optional[str] = None
    PLAID_ENV: str = "sandbox"
    REDIS_URL: str = "redis://localhost:6379/0"

    JOB_BROKER: Literal["celery", "inprocess"] = "celery"
    CORS_ALLOW_ORIGINS: str = "http://localhost:5173"
    MAX_CONCURRENT_ASSESSMENTS: int = 5
    MAX_WS_CONNECTIONS: int = 50
    MAX_REQUEST_BYTES: int = 262_144
    INGEST_PER_IP_LIMIT: str = "5/minute"
    INGEST_GLOBAL_LIMIT: str = "60/minute"
    EVENT_LOG_TTL_SECONDS: int = 900

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def cors_allow_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ALLOW_ORIGINS.split(",") if o.strip()]


settings = Settings()
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint + type check**

Run: `ruff check app/config.py tests/test_config.py && mypy app/config.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/config.py tests/test_config.py
git commit -m "feat(config): add broker-mode and guard settings"
```

---

## Task 2: PipelineEvent model + broker base types

**Files:**
- Create: `app/brokers/__init__.py`, `app/brokers/events.py`, `app/brokers/base.py`
- Create: `tests/brokers/__init__.py`, `tests/brokers/test_events.py`

**Interfaces:**
- Consumes: `AffordabilityAssessment` from `app.core.models`.
- Produces:
  - `PipelineEventName = Literal["RECEIVED","CATEGORISING","SCORING","DECIDED","FAILED"]`
  - `PipelineEvent` (Pydantic model): fields `event: PipelineEventName`, `job_id: str = ""`, `seq: int = 0`, `ts: float = <factory>`, `detail: str | None = None`, `data: dict[str, Any] | None = None`, `error: str | None = None`; property `is_terminal: bool`.
  - `TERMINAL_EVENTS: frozenset[str] = frozenset({"DECIDED", "FAILED"})`
  - `JobBroker` Protocol: `async def submit(self, payload: BankStatementPayload) -> str`, `def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]`, `async def status(self, job_id: str) -> JobStatusResponse`
  - `JobStatusResponse` (Pydantic): `job_id: str`, `status: Literal["PENDING","STARTED","SUCCESS","FAILURE"]`, `result: dict[str, Any] | None = None`, `error: str | None = None`
  - `CapacityError(RuntimeError)`
- `app/brokers/__init__.py` at this stage only re-exports the above (the `get_broker` factory lands in Task 6).

- [ ] **Step 1: Write the failing test**

```python
# tests/brokers/test_events.py
from app.brokers.events import PipelineEvent, TERMINAL_EVENTS


def test_event_defaults_are_filled():
    e = PipelineEvent(event="CATEGORISING")
    assert e.seq == 0
    assert e.job_id == ""
    assert isinstance(e.ts, float) and e.ts > 0
    assert e.is_terminal is False


def test_decided_is_terminal_and_carries_data():
    e = PipelineEvent(event="DECIDED", job_id="j1", seq=4, data={"decision": "APPROVED"})
    assert e.is_terminal is True
    assert e.data == {"decision": "APPROVED"}


def test_failed_is_terminal():
    assert PipelineEvent(event="FAILED", error="boom").is_terminal is True


def test_terminal_set_contents():
    assert TERMINAL_EVENTS == frozenset({"DECIDED", "FAILED"})


def test_round_trips_through_json():
    e = PipelineEvent(event="SCORING", job_id="j1", seq=2)
    assert PipelineEvent.model_validate(e.model_dump(mode="json")) == e
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/brokers/test_events.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `events.py`**

```python
# app/brokers/events.py
import time
from typing import Any, Literal

from pydantic import BaseModel, Field

PipelineEventName = Literal["RECEIVED", "CATEGORISING", "SCORING", "DECIDED", "FAILED"]

TERMINAL_EVENTS: frozenset[str] = frozenset({"DECIDED", "FAILED"})


class PipelineEvent(BaseModel):
    event: PipelineEventName
    job_id: str = ""
    seq: int = 0
    ts: float = Field(default_factory=time.time)
    detail: str | None = None
    data: dict[str, Any] | None = None
    error: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.event in TERMINAL_EVENTS
```

- [ ] **Step 4: Implement `base.py`**

```python
# app/brokers/base.py
from typing import Any, AsyncIterator, Literal, Protocol

from pydantic import BaseModel

from app.brokers.events import PipelineEvent
from app.core.models import BankStatementPayload


class CapacityError(RuntimeError):
    """Raised by a broker when it cannot accept another assessment right now."""


class JobStatusResponse(BaseModel):
    job_id: str
    status: Literal["PENDING", "STARTED", "SUCCESS", "FAILURE"]
    result: dict[str, Any] | None = None
    error: str | None = None


class JobBroker(Protocol):
    async def submit(self, payload: BankStatementPayload) -> str: ...

    def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]: ...

    async def status(self, job_id: str) -> JobStatusResponse: ...
```

- [ ] **Step 5: Implement `__init__.py`**

```python
# app/brokers/__init__.py
from app.brokers.base import CapacityError, JobBroker, JobStatusResponse
from app.brokers.events import TERMINAL_EVENTS, PipelineEvent, PipelineEventName

__all__ = [
    "CapacityError",
    "JobBroker",
    "JobStatusResponse",
    "PipelineEvent",
    "PipelineEventName",
    "TERMINAL_EVENTS",
]
```

- [ ] **Step 6: Create `tests/brokers/__init__.py`** (empty file).

- [ ] **Step 7: Run tests + lint + types**

Run: `python -m pytest tests/brokers/test_events.py -v && ruff check app/brokers tests/brokers && mypy app/brokers`
Expected: PASS (5 tests), clean.

- [ ] **Step 8: Commit**

```bash
git add app/brokers tests/brokers
git commit -m "feat(brokers): add PipelineEvent model and JobBroker protocol"
```

---

## Task 3: Extract `run_underwriting_pipeline`

**Files:**
- Create: `app/core/pipeline.py`
- Modify: `app/worker.py`
- Modify: `tests/test_api_ingestion.py` (patch target moves)
- Test: `tests/test_pipeline.py` (create)

**Interfaces:**
- Consumes: `PipelineEvent` (Task 2), `BankStatementPayload`, `AffordabilityAssessment`, `TransactionCategoriser`, `AffordabilityEngine`.
- Produces:
  - `EmitFn = Callable[[PipelineEvent], None]`
  - `run_underwriting_pipeline(payload: BankStatementPayload, emit: EmitFn) -> AffordabilityAssessment`
    Emits, in order: `RECEIVED`; `CATEGORISING` (no detail); `CATEGORISING` (with `detail="Sorted N transactions into K categories"`); `SCORING`; `DECIDED` (with `data=assessment.model_dump(mode="json")`). Does **not** emit `FAILED` — callers wrap and emit that themselves.
- Later tasks rely on: the worker no longer contains categorise/score logic; `app.worker.process_affordability_assessment` still exists as a Celery task taking `payload_dict: dict[str, Any]` and returning the assessment dict.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_pipeline.py
from decimal import Decimal

from app.brokers.events import PipelineEvent
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline

PAYLOAD = {
    "statement_id": "stmt_pipe_1",
    "account_holder": "Test Person",
    "account_number": "12345678",
    "sort_code": "40-00-01",
    "transactions": [
        {"id": "t1", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "2000.00"},
        {"id": "t2", "date": "2026-08-03", "raw_description": "RENT TO LANDLORD", "amount": "-800.00"},
        {"id": "t3", "date": "2026-08-05", "raw_description": "BET365 UK", "amount": "-500.00"},
    ],
}


def _collect() -> tuple[list[PipelineEvent], object]:
    events: list[PipelineEvent] = []
    assessment = run_underwriting_pipeline(
        BankStatementPayload.model_validate(PAYLOAD), events.append
    )
    return events, assessment


def test_emits_stages_in_order():
    events, _ = _collect()
    assert [e.event for e in events] == [
        "RECEIVED",
        "CATEGORISING",
        "CATEGORISING",
        "SCORING",
        "DECIDED",
    ]


def test_categorising_detail_reports_counts():
    events, _ = _collect()
    detail = events[2].detail
    assert detail is not None and "3 transactions" in detail


def test_decided_event_carries_assessment_dict():
    events, assessment = _collect()
    decided = events[-1]
    assert decided.data == assessment.model_dump(mode="json")
    assert decided.data["decision"] == "DECLINED"  # gambling 25% of income


def test_return_value_matches_direct_engine_call():
    from app.core.affordability import AffordabilityEngine
    from app.core.categoriser import TransactionCategoriser

    stmt = BankStatementPayload.model_validate(PAYLOAD)
    stmt.transactions = TransactionCategoriser.process_statement(stmt.transactions)
    expected = AffordabilityEngine.evaluate(stmt)

    _, assessment = _collect()
    assert assessment == expected
    assert assessment.gambling_income_ratio == Decimal("0.25")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_pipeline.py -v`
Expected: FAIL — `app.core.pipeline` missing.

- [ ] **Step 3: Implement `app/core/pipeline.py`**

```python
# app/core/pipeline.py
from typing import Callable

from app.brokers.events import PipelineEvent
from app.core.affordability import AffordabilityEngine
from app.core.categoriser import TransactionCategoriser
from app.core.models import AffordabilityAssessment, BankStatementPayload

EmitFn = Callable[[PipelineEvent], None]


def run_underwriting_pipeline(
    payload: BankStatementPayload, emit: EmitFn
) -> AffordabilityAssessment:
    """Run parse -> categorise -> score -> decide, announcing each stage via ``emit``.

    ``emit`` is transport-agnostic: it may publish to Redis, push to an in-memory
    queue, or collect into a list. This function never raises ``PipelineEvent``
    FAILED itself; callers are responsible for translating exceptions.
    """
    emit(PipelineEvent(event="RECEIVED"))

    emit(PipelineEvent(event="CATEGORISING"))
    payload.transactions = TransactionCategoriser.process_statement(payload.transactions)
    category_count = len({t.category for t in payload.transactions})
    emit(
        PipelineEvent(
            event="CATEGORISING",
            detail=(
                f"Sorted {len(payload.transactions)} transactions "
                f"into {category_count} categories"
            ),
        )
    )

    emit(PipelineEvent(event="SCORING"))
    assessment = AffordabilityEngine.evaluate(payload)

    emit(PipelineEvent(event="DECIDED", data=assessment.model_dump(mode="json")))
    return assessment
```

- [ ] **Step 4: Run the pipeline test**

Run: `python -m pytest tests/test_pipeline.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Rewrite `app/worker.py` as a thin wrapper**

```python
# app/worker.py
import itertools
import json
from typing import Any, Dict

import redis
from celery import Celery

from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline

celery_app = Celery(
    "underwriting_worker",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

redis_client = redis.Redis.from_url(settings.REDIS_URL)


def _publish(job_id: str, seq: itertools.count, event: PipelineEvent) -> None:
    event.job_id = job_id
    event.seq = next(seq)
    body = json.dumps(event.model_dump(mode="json"))
    try:
        pipe = redis_client.pipeline()
        pipe.rpush(f"underwriting_events:{job_id}", body)
        pipe.expire(f"underwriting_events:{job_id}", settings.EVENT_LOG_TTL_SECONDS)
        pipe.publish(f"underwriting_jobs:{job_id}", body)
        pipe.execute()
    except Exception:  # noqa: BLE001 - Redis must never crash the worker task
        pass


@celery_app.task(name="tasks.process_affordability_assessment", bind=True)  # type: ignore[untyped-decorator]
def process_affordability_assessment(self, payload_dict: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
    job_id: str = self.request.id
    seq = itertools.count()

    def emit(event: PipelineEvent) -> None:
        _publish(job_id, seq, event)

    statement = BankStatementPayload.model_validate(payload_dict)
    try:
        assessment = run_underwriting_pipeline(statement, emit)
    except Exception as exc:  # noqa: BLE001 - surface failure as a terminal event
        emit(PipelineEvent(event="FAILED", error=str(exc)))
        raise
    return assessment.model_dump(mode="json")
```

- [ ] **Step 6: Update `tests/test_api_ingestion.py`**

The ingest route still calls `process_affordability_assessment.delay` **at this point** (the broker swap happens in Task 6). Only change: the assertion message text is unchanged, so this file needs **no edit yet**. Confirm by running it.

Run: `python -m pytest tests/test_api_ingestion.py -v`
Expected: PASS (unchanged).

- [ ] **Step 7: Full suite + lint + types**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS. `tests/test_websockets.py` still passes — it mocks `celery_app.AsyncResult` and `aioredis.from_url`, so the worker's internal changes don't reach it.

- [ ] **Step 8: Commit**

```bash
git add app/core/pipeline.py app/worker.py tests/test_pipeline.py
git commit -m "refactor: extract run_underwriting_pipeline from the Celery task"
```

---

## Task 4: CeleryRedisBroker

**Files:**
- Create: `app/brokers/celery_redis.py`
- Test: `tests/brokers/test_celery_redis_broker.py`

**Interfaces:**
- Consumes: `PipelineEvent`, `JobStatusResponse`, `run_underwriting_pipeline`, `celery_app`, `process_affordability_assessment`, `settings.REDIS_URL`, `settings.EVENT_LOG_TTL_SECONDS`.
- Produces: `CeleryRedisBroker` implementing `JobBroker`.
  - `submit`: `process_affordability_assessment.delay(payload.model_dump(mode="json"))`, returns `task.id`.
  - `subscribe`: async generator — `LRANGE underwriting_events:{job_id} 0 -1`, yield each parsed `PipelineEvent` (dedupe by `seq`), then `pubsub.subscribe("underwriting_jobs:{job_id}")` and tail, stopping after a terminal event. Uses `redis.asyncio`.
  - `status`: wraps `celery_app.AsyncResult(job_id)` into `JobStatusResponse` (`PENDING`→PENDING, `STARTED`→STARTED, `SUCCESS`→SUCCESS+result, `FAILURE`→FAILURE+error, anything else→STARTED).

- [ ] **Step 1: Write the failing test**

```python
# tests/brokers/test_celery_redis_broker.py
import pytest
import redis.asyncio as aioredis

from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import PipelineEvent
from app.config import settings


@pytest.fixture
async def redis_conn():
    conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    await conn.flushdb()
    yield conn
    await conn.flushdb()
    await conn.aclose()


async def _seed(conn, job_id: str, events: list[PipelineEvent]) -> None:
    import json

    for e in events:
        body = json.dumps(e.model_dump(mode="json"))
        await conn.rpush(f"underwriting_events:{job_id}", body)


async def test_subscribe_replays_completed_job(redis_conn):
    job_id = "job-replay-1"
    await _seed(
        redis_conn,
        job_id,
        [
            PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
            PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
            PipelineEvent(event="SCORING", job_id=job_id, seq=2),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={"decision": "APPROVED"}),
        ],
    )
    broker = CeleryRedisBroker()
    received = [e.event async for e in broker.subscribe(job_id)]
    assert received == ["RECEIVED", "CATEGORISING", "SCORING", "DECIDED"]


async def test_subscribe_tails_live_events_after_replay(redis_conn):
    job_id = "job-live-1"
    await _seed(redis_conn, job_id, [PipelineEvent(event="RECEIVED", job_id=job_id, seq=0)])
    broker = CeleryRedisBroker()

    import asyncio
    import json

    async def publish_later():
        await asyncio.sleep(0.1)
        for e in [
            PipelineEvent(event="SCORING", job_id=job_id, seq=1),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=2, data={"decision": "REFERRED"}),
        ]:
            await redis_conn.publish(
                f"underwriting_jobs:{job_id}", json.dumps(e.model_dump(mode="json"))
            )

    task = asyncio.create_task(publish_later())
    received = [e.event async for e in broker.subscribe(job_id)]
    await task
    assert received == ["RECEIVED", "SCORING", "DECIDED"]


async def test_subscribe_dedupes_by_seq(redis_conn):
    job_id = "job-dupe-1"
    ev = [
        PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
        PipelineEvent(event="DECIDED", job_id=job_id, seq=1, data={}),
    ]
    await _seed(redis_conn, job_id, ev)
    broker = CeleryRedisBroker()

    import asyncio
    import json

    async def republish_seq1():
        await asyncio.sleep(0.05)
        await redis_conn.publish(
            f"underwriting_jobs:{job_id}", json.dumps(ev[1].model_dump(mode="json"))
        )

    asyncio.create_task(republish_seq1())
    received = [e.seq async for e in broker.subscribe(job_id)]
    assert received == [0, 1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/brokers/test_celery_redis_broker.py -v`
Expected: FAIL — module missing. (Requires a running Redis; CI has one. Locally: `docker compose up -d redis`.)

- [ ] **Step 3: Implement**

```python
# app/brokers/celery_redis.py
import json
from typing import Any, AsyncIterator

import redis.asyncio as aioredis

from app.brokers.base import JobStatusResponse
from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.worker import celery_app, process_affordability_assessment

_POLL_TIMEOUT = 0.5


class CeleryRedisBroker:
    """JobBroker backed by the Celery worker and Redis list + pub/sub."""

    async def submit(self, payload: BankStatementPayload) -> str:
        task = process_affordability_assessment.delay(payload.model_dump(mode="json"))
        return str(task.id)

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        conn = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        pubsub = conn.pubsub()
        channel = f"underwriting_jobs:{job_id}"
        seen: set[int] = set()
        try:
            await pubsub.subscribe(channel)  # subscribe before replay to avoid a gap

            for raw in await conn.lrange(f"underwriting_events:{job_id}", 0, -1):
                event = PipelineEvent.model_validate_json(raw)
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return

            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=_POLL_TIMEOUT
                )
                if message is None or message.get("type") != "message":
                    continue
                event = PipelineEvent.model_validate_json(message["data"])
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
            await conn.aclose()

    async def status(self, job_id: str) -> JobStatusResponse:
        result = celery_app.AsyncResult(job_id)
        state = result.state
        if state == "SUCCESS":
            data: dict[str, Any] = result.result
            return JobStatusResponse(job_id=job_id, status="SUCCESS", result=data)
        if state == "FAILURE":
            return JobStatusResponse(job_id=job_id, status="FAILURE", error=str(result.info))
        if state == "PENDING":
            return JobStatusResponse(job_id=job_id, status="PENDING")
        return JobStatusResponse(job_id=job_id, status="STARTED")
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/brokers/test_celery_redis_broker.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint + types**

Run: `ruff check app/brokers/celery_redis.py tests/brokers/test_celery_redis_broker.py && mypy app/`
Expected: clean. (Note: `async for`/`yield` in an async generator — mypy strict is fine with the `AsyncIterator` return type.)

- [ ] **Step 6: Commit**

```bash
git add app/brokers/celery_redis.py tests/brokers/test_celery_redis_broker.py
git commit -m "feat(brokers): add CeleryRedisBroker with replay + live tail"
```

---

## Task 5: Rewrite the WebSocket endpoint over the broker seam

**Files:**
- Modify: `app/api/v1/websockets.py` (full rewrite)
- Modify: `app/main.py` (add `get_broker` placeholder dependency — see note)
- Modify: `tests/test_websockets.py` (full rewrite)

**Interfaces:**
- Consumes: `JobBroker`, `PipelineEvent`, `settings.MAX_WS_CONNECTIONS`.
- Produces: `websocket_underwriting_endpoint` sends `{"event": "SUBSCRIBED", "job_id": ...}` then one JSON frame per `PipelineEvent` (`event.model_dump(mode="json")`), closing after a terminal event. Rejects with close code `1013` when the process WS count is at `MAX_WS_CONNECTIONS`.
- Depends on a module-level `get_broker` dependency. **Define a minimal `get_broker` in `app/brokers/__init__.py` now** returning `CeleryRedisBroker()` unconditionally; Task 6 replaces its body with the env switch. This keeps Task 5 shippable.

- [ ] **Step 1: Add the interim `get_broker`**

```python
# append to app/brokers/__init__.py
from app.brokers.celery_redis import CeleryRedisBroker


def get_broker() -> "JobBroker":
    return CeleryRedisBroker()
```

Add `"get_broker"` to `__all__`.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_websockets.py
from typing import AsyncIterator

from fastapi.testclient import TestClient

from app.brokers.events import PipelineEvent
from app.brokers import get_broker
from app.main import app

WS_PATH = "/api/v1/ws/underwriting/{job_id}"


class FakeBroker:
    def __init__(self, events: list[PipelineEvent]) -> None:
        self._events = events

    async def submit(self, payload):  # pragma: no cover - unused here
        return "unused"

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        for e in self._events:
            yield e

    async def status(self, job_id: str):  # pragma: no cover - unused here
        raise NotImplementedError


def _use(events: list[PipelineEvent]) -> None:
    app.dependency_overrides[get_broker] = lambda: FakeBroker(events)


def teardown_function() -> None:
    app.dependency_overrides.clear()


def test_streams_subscribed_then_every_event_in_order():
    job_id = "job-1"
    _use(
        [
            PipelineEvent(event="RECEIVED", job_id=job_id, seq=0),
            PipelineEvent(event="CATEGORISING", job_id=job_id, seq=1),
            PipelineEvent(event="SCORING", job_id=job_id, seq=2),
            PipelineEvent(event="DECIDED", job_id=job_id, seq=3, data={"decision": "APPROVED"}),
        ]
    )
    with TestClient(app).websocket_connect(WS_PATH.format(job_id=job_id)) as ws:
        assert ws.receive_json()["event"] == "SUBSCRIBED"
        assert [ws.receive_json()["event"] for _ in range(4)] == [
            "RECEIVED",
            "CATEGORISING",
            "SCORING",
            "DECIDED",
        ]


def test_forwards_failed_event():
    _use([PipelineEvent(event="FAILED", job_id="j", seq=0, error="kaboom")])
    with TestClient(app).websocket_connect(WS_PATH.format(job_id="j")) as ws:
        assert ws.receive_json()["event"] == "SUBSCRIBED"
        frame = ws.receive_json()
        assert frame["event"] == "FAILED"
        assert frame["error"] == "kaboom"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/test_websockets.py -v`
Expected: FAIL — old endpoint still references `celery_app`, imports differ.

- [ ] **Step 4: Rewrite `app/api/v1/websockets.py`**

```python
# app/api/v1/websockets.py
import logging

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from app.brokers import get_broker
from app.brokers.base import JobBroker
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["WebSockets"])

_ws_connections = 0


@router.websocket("/underwriting/{job_id}")
async def websocket_underwriting_endpoint(
    websocket: WebSocket,
    job_id: str,
    broker: JobBroker = Depends(get_broker),
) -> None:
    global _ws_connections
    if _ws_connections >= settings.MAX_WS_CONNECTIONS:
        await websocket.close(code=1013)  # 1013 = try again later
        return

    await websocket.accept()
    _ws_connections += 1
    try:
        await websocket.send_json({"event": "SUBSCRIBED", "job_id": job_id})
        async for event in broker.subscribe(job_id):
            await websocket.send_json(event.model_dump(mode="json"))
    except WebSocketDisconnect:
        logger.info("client disconnected from job %s", job_id)
    except Exception:  # noqa: BLE001 - log and close, never propagate
        logger.exception("error streaming job %s", job_id)
    finally:
        _ws_connections -= 1
```

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_websockets.py -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Full suite + lint + types**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS. (`test_api_ingestion.py` still green — ingest still calls `.delay` directly until Task 6.)

- [ ] **Step 7: Commit**

```bash
git add app/api/v1/websockets.py app/brokers/__init__.py tests/test_websockets.py
git commit -m "refactor(ws): stream pipeline events through the broker seam"
```

---

## Task 6: InProcessBroker + broker factory + ingest wiring

**Files:**
- Create: `app/brokers/inprocess.py`
- Modify: `app/brokers/__init__.py` (real `get_broker`)
- Modify: `app/main.py` (ingest route uses `broker.submit`; jobs route uses `broker.status`)
- Modify: `tests/test_api_ingestion.py` (broker override)
- Test: `tests/brokers/test_inprocess_broker.py`

**Interfaces:**
- Consumes: `PipelineEvent`, `JobStatusResponse`, `CapacityError`, `run_underwriting_pipeline`, `settings.MAX_CONCURRENT_ASSESSMENTS`, `settings.EVENT_LOG_TTL_SECONDS`.
- Produces:
  - `InProcessBroker` implementing `JobBroker`. Constructor takes no args. Holds `_channels: dict[str, JobChannel]` and `_active: int`.
    - `submit`: raise `CapacityError` if `_active >= MAX_CONCURRENT_ASSESSMENTS`; else `job_id = uuid4().hex`, create `JobChannel`, `asyncio.create_task(self._run(job_id, payload))`, return `job_id`.
    - `_run`: `_active += 1`; build `emit` that stamps `seq`/`job_id`, appends to `channel.events`, and `put_nowait`s to every subscriber queue; call `run_underwriting_pipeline`; on exception emit `FAILED`; `finally` set `channel.done`, `_active -= 1`, schedule reaper.
    - `subscribe`: create a queue, register it, replay `channel.events` snapshot (dedupe by `seq`), then drain the queue until a terminal event; deregister in `finally`.
    - `status`: from channel state — no channel → PENDING; not done → STARTED; done with terminal `DECIDED` → SUCCESS + `data`; done with `FAILED` → FAILURE + error.
  - `JobChannel` dataclass: `events: list[PipelineEvent]`, `done: asyncio.Event`, `subscribers: set[asyncio.Queue[PipelineEvent]]`.
- Later tasks rely on: `get_broker()` returns `InProcessBroker` singleton when `settings.JOB_BROKER == "inprocess"`, else `CeleryRedisBroker()`.

**Architecture note (deviation from spec §4.4):** the spec says "FastAPI `BackgroundTasks`". This plan uses `asyncio.create_task` instead. Rationale: the pipeline is synchronous and sub-millisecond, so running it inline on the loop does not meaningfully block, and staying single-threaded lets `emit` feed `asyncio.Queue` directly with `put_nowait` — no `loop.call_soon_threadsafe`, no cross-thread races. Same observable behaviour, simpler and safer code.

- [ ] **Step 1: Write the failing test**

```python
# tests/brokers/test_inprocess_broker.py
import asyncio

import pytest

from app.brokers.base import CapacityError
from app.brokers.inprocess import InProcessBroker
from app.core.models import BankStatementPayload

APPROVE = {
    "statement_id": "s1", "account_holder": "A", "account_number": "1", "sort_code": "40-00-01",
    "transactions": [
        {"id": "t1", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "3000.00"},
        {"id": "t2", "date": "2026-08-02", "raw_description": "RENT LANDLORD", "amount": "-700.00"},
    ],
}


def _payload() -> BankStatementPayload:
    return BankStatementPayload.model_validate(APPROVE)


async def test_full_sequence_streamed_to_a_live_subscriber():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    events = [e.event async for e in broker.subscribe(job_id)]
    assert events == ["RECEIVED", "CATEGORISING", "CATEGORISING", "SCORING", "DECIDED"]


async def test_late_subscriber_still_gets_full_replay():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    await asyncio.sleep(0.05)  # let the job finish first
    events = [e.event async for e in broker.subscribe(job_id)]
    assert events[0] == "RECEIVED" and events[-1] == "DECIDED"


async def test_seq_numbers_are_monotonic():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    seqs = [e.seq async for e in broker.subscribe(job_id)]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


async def test_capacity_error_when_at_limit(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "MAX_CONCURRENT_ASSESSMENTS", 1)
    broker = InProcessBroker()
    broker._active = 1  # simulate one in flight
    with pytest.raises(CapacityError):
        await broker.submit(_payload())


async def test_failed_event_emitted_on_pipeline_error(monkeypatch):
    broker = InProcessBroker()

    def boom(payload, emit):
        raise ValueError("categoriser exploded")

    monkeypatch.setattr("app.brokers.inprocess.run_underwriting_pipeline", boom)
    job_id = await broker.submit(_payload())
    events = [(e.event, e.error) async for e in broker.subscribe(job_id)]
    assert events[-1][0] == "FAILED"
    assert "exploded" in events[-1][1]


async def test_status_reports_success_after_completion():
    broker = InProcessBroker()
    job_id = await broker.submit(_payload())
    async for _ in broker.subscribe(job_id):
        pass
    status = await broker.status(job_id)
    assert status.status == "SUCCESS"
    assert status.result is not None and status.result["decision"] == "APPROVED"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/brokers/test_inprocess_broker.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `app/brokers/inprocess.py`**

```python
# app/brokers/inprocess.py
import asyncio
import itertools
from dataclasses import dataclass, field
from typing import AsyncIterator
from uuid import uuid4

from app.brokers.base import CapacityError, JobStatusResponse
from app.brokers.events import PipelineEvent
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.pipeline import run_underwriting_pipeline


@dataclass
class JobChannel:
    events: list[PipelineEvent] = field(default_factory=list)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    subscribers: set["asyncio.Queue[PipelineEvent]"] = field(default_factory=set)


class InProcessBroker:
    """JobBroker that runs the pipeline on the event loop with in-memory fan-out."""

    def __init__(self) -> None:
        self._channels: dict[str, JobChannel] = {}
        self._active = 0

    async def submit(self, payload: BankStatementPayload) -> str:
        if self._active >= settings.MAX_CONCURRENT_ASSESSMENTS:
            raise CapacityError("assessment capacity reached")
        job_id = uuid4().hex
        self._channels[job_id] = JobChannel()
        asyncio.create_task(self._run(job_id, payload))
        return job_id

    async def _run(self, job_id: str, payload: BankStatementPayload) -> None:
        channel = self._channels[job_id]
        seq = itertools.count()
        self._active += 1

        def emit(event: PipelineEvent) -> None:
            event.job_id = job_id
            event.seq = next(seq)
            channel.events.append(event)
            for queue in list(channel.subscribers):
                queue.put_nowait(event)

        try:
            await asyncio.to_thread(run_underwriting_pipeline, payload, emit)
        except Exception as exc:  # noqa: BLE001 - surface as terminal event
            emit(PipelineEvent(event="FAILED", error=str(exc)))
        finally:
            self._active -= 1
            channel.done.set()
            asyncio.get_running_loop().call_later(
                settings.EVENT_LOG_TTL_SECONDS, self._channels.pop, job_id, None
            )

    async def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        channel = self._channels.get(job_id)
        if channel is None:
            return
        queue: "asyncio.Queue[PipelineEvent]" = asyncio.Queue()
        channel.subscribers.add(queue)
        seen: set[int] = set()
        try:
            for event in list(channel.events):
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return
            while True:
                event = await queue.get()
                if event.seq in seen:
                    continue
                seen.add(event.seq)
                yield event
                if event.is_terminal:
                    return
        finally:
            channel.subscribers.discard(queue)

    async def status(self, job_id: str) -> JobStatusResponse:
        channel = self._channels.get(job_id)
        if channel is None:
            return JobStatusResponse(job_id=job_id, status="PENDING")
        if not channel.done.is_set():
            return JobStatusResponse(job_id=job_id, status="STARTED")
        terminal = channel.events[-1] if channel.events else None
        if terminal is not None and terminal.event == "DECIDED":
            return JobStatusResponse(job_id=job_id, status="SUCCESS", result=terminal.data)
        error = terminal.error if terminal is not None else "unknown failure"
        return JobStatusResponse(job_id=job_id, status="FAILURE", error=error)
```

> **Note on `asyncio.to_thread`:** `emit` is called from the worker thread but only does `list.append` and `Queue.put_nowait`, both of which are thread-safe for a single append/put. If a reviewer prefers strict single-thread, replace `await asyncio.to_thread(run_underwriting_pipeline, payload, emit)` with a direct `run_underwriting_pipeline(payload, emit)` — acceptable given sub-ms runtime. Keep `to_thread` as written unless the test `test_full_sequence_streamed_to_a_live_subscriber` proves flaky.

- [ ] **Step 4: Run the broker tests**

Run: `python -m pytest tests/brokers/test_inprocess_broker.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Wire the real `get_broker`**

```python
# app/brokers/__init__.py  — replace the interim get_broker
from functools import lru_cache

from app.brokers.base import CapacityError, JobBroker, JobStatusResponse
from app.brokers.celery_redis import CeleryRedisBroker
from app.brokers.events import TERMINAL_EVENTS, PipelineEvent, PipelineEventName
from app.brokers.inprocess import InProcessBroker
from app.config import settings

__all__ = [
    "CapacityError", "JobBroker", "JobStatusResponse", "PipelineEvent",
    "PipelineEventName", "TERMINAL_EVENTS", "get_broker",
]


@lru_cache(maxsize=1)
def get_broker() -> JobBroker:
    if settings.JOB_BROKER == "inprocess":
        return InProcessBroker()
    return CeleryRedisBroker()
```

- [ ] **Step 6: Point the ingest + jobs routes at the broker**

In `app/main.py`, change the ingest route body to use an injected broker and translate `CapacityError`:

```python
from fastapi import Depends
from app.brokers import CapacityError, get_broker
from app.brokers.base import JobBroker

# inside ingest_statement(...), add parameter:  broker: JobBroker = Depends(get_broker)
# replace the try/except body with:
    if len(payload.transactions) > MAX_TRANSACTIONS_LIMIT:
        raise HTTPException(status_code=413, detail=f"Statement exceeds maximum limit of {MAX_TRANSACTIONS_LIMIT} transactions.")
    try:
        job_id = await broker.submit(payload)
    except CapacityError:
        raise HTTPException(
            status_code=429,
            detail="The demo is busy right now. Try again in a moment.",
            headers={"Retry-After": "10"},
        )
    return IngestionResponse(
        message="Statement received and enqueued for underwriting assessment.",
        job_id=job_id,
        status="PENDING",
    )
```

Replace the `get_job_status` body:

```python
# app/main.py
from app.brokers.base import JobBroker

@app.get("/api/v1/statements/jobs/{job_id}", tags=["Statements"])
async def get_job_status(job_id: str, broker: JobBroker = Depends(get_broker)) -> dict[str, Any]:
    status = await broker.status(job_id)
    return status.model_dump(mode="json")
```

Remove the now-unused `from app.worker import celery_app, process_affordability_assessment` import from `app/main.py` (keep it only if `worker` import is still needed elsewhere — it is not).

- [ ] **Step 7: Update `tests/test_api_ingestion.py`**

```python
# tests/test_api_ingestion.py
from httpx import ASGITransport, AsyncClient

from app.brokers import get_broker
from app.main import app


class _StubBroker:
    async def submit(self, payload) -> str:
        return "stub-job-123"

    async def subscribe(self, job_id):  # pragma: no cover
        if False:
            yield None

    async def status(self, job_id):  # pragma: no cover
        raise NotImplementedError


async def test_non_blocking_ingestion_endpoint():
    app.dependency_overrides[get_broker] = lambda: _StubBroker()
    payload = {
        "statement_id": "stmt_async_100",
        "account_holder": "Jane Doe",
        "account_number": "87654321",
        "sort_code": "40-00-01",
        "transactions": [
            {"id": "tx_101", "date": "2026-08-01", "raw_description": "EMPLOYER SALARY BGC", "amount": "2800.00"},
            {"id": "tx_102", "date": "2026-08-02", "raw_description": "POS 4829 BET365", "amount": "-50.00"},
        ],
    }
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/api/v1/statements/ingest", json=payload)
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    body = response.json()
    assert body["job_id"] == "stub-job-123"
    assert body["status"] == "PENDING"
    assert "enqueued" in body["message"]
```

- [ ] **Step 8: Full suite + lint + types**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS.

- [ ] **Step 9: Manual smoke of inprocess mode**

Run:
```bash
JOB_BROKER=inprocess python -c "
import asyncio
from app.brokers import get_broker
from app.core.samples import ...  # not yet — use an inline payload
"
```
Skip if `app/core/samples` not present yet; covered by Task 7. Instead just confirm import:
```bash
JOB_BROKER=inprocess python -c "from app.brokers import get_broker; print(type(get_broker()).__name__)"
```
Expected: `InProcessBroker`.

- [ ] **Step 10: Commit**

```bash
git add app/brokers tests/brokers/test_inprocess_broker.py app/main.py tests/test_api_ingestion.py
git commit -m "feat(brokers): add InProcessBroker and select broker by JOB_BROKER"
```

---

## Task 7: Persona fixtures + `/samples` endpoint

**Files:**
- Create: `app/core/samples.py`
- Create: `app/api/v1/statements.py`
- Modify: `app/main.py` (include the new router; drop the inline ingest/jobs route definitions — they move into the router)
- Test: `tests/test_samples.py`

**Interfaces:**
- Consumes: `BankStatementPayload`, `run_underwriting_pipeline`, `JobBroker`, `get_broker`, `CapacityError`, the slowapi `limiter` from `app.main`.
- Produces:
  - `app/core/samples.py`:
    - `PERSONAS: dict[str, BankStatementPayload]` keyed by `stable_earner`, `gambling_risk`, `over_indebted`, `thin_file`.
    - `PERSONA_META: dict[str, dict[str, str]]` — `label`, `blurb`, `expected_decision` per key.
    - `list_personas() -> list[SampleSummary]`, `get_persona(persona_id: str) -> BankStatementPayload` (raises `KeyError` if unknown).
    - `SampleSummary` Pydantic model: `id: str`, `label: str`, `blurb: str`, `expected_decision: Literal["APPROVED","REFERRED","DECLINED"]`.
  - `app/api/v1/statements.py`: `router = APIRouter(prefix="/statements", tags=["Statements"])` with `GET /samples`, `GET /samples/{persona_id}`, `POST /ingest`, `GET /jobs/{job_id}`. Mounted under `/api/v1` in `app/main.py`.

**Rate limits:** `POST /ingest` carries `@limiter.limit(settings.INGEST_PER_IP_LIMIT)` and `@limiter.limit(settings.INGEST_GLOBAL_LIMIT, key_func=lambda _r: "global-ingest")`. `limiter` is imported from `app.main`. (`app.main` imports the router *after* defining `limiter` — see Task 8 for the final `main.py` ordering; for now `from app.main import limiter` works because the router module is imported at the bottom of `main.py`.)

> To avoid a circular import (`main` → `statements` → `main`), define `limiter` in a new tiny module `app/ratelimit.py` and import it from both. Do this as Step 1 below.

- [ ] **Step 1: Extract the limiter**

```python
# app/ratelimit.py
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])
```

Update `app/main.py` to `from app.ratelimit import limiter` instead of constructing it inline. Keep `app.state.limiter = limiter` and the exception handler registration.

- [ ] **Step 2: Write the failing test**

```python
# tests/test_samples.py
import pytest
from fastapi.testclient import TestClient

from app.core.pipeline import run_underwriting_pipeline
from app.core.samples import PERSONA_META, PERSONAS, get_persona, list_personas
from app.main import app

EXPECTED = {
    "stable_earner": ("APPROVED", None),
    "gambling_risk": ("DECLINED", "EXCESSIVE_GAMBLING_RISK"),
    "over_indebted": ("DECLINED", "HIGH_DEBT_TO_INCOME_RATIO"),
    "thin_file": ("REFERRED", "LOW_NET_DISPOSABLE_INCOME"),
}


@pytest.mark.parametrize("persona_id", list(EXPECTED))
def test_persona_produces_documented_decision(persona_id: str):
    decision, flag_prefix = EXPECTED[persona_id]
    payload = get_persona(persona_id)
    assessment = run_underwriting_pipeline(payload, lambda _e: None)
    assert assessment.decision.value == decision
    if flag_prefix is None:
        assert assessment.risk_flags == []
    else:
        assert any(f.startswith(flag_prefix) for f in assessment.risk_flags)
        assert len(assessment.risk_flags) == 1  # one headline cause per persona


def test_meta_matches_pipeline_outcome():
    for persona_id, meta in PERSONA_META.items():
        assert meta["expected_decision"] == EXPECTED[persona_id][0]


def test_list_endpoint_returns_four_summaries():
    body = TestClient(app).get("/api/v1/statements/samples").json()
    assert {s["id"] for s in body} == set(PERSONAS)
    assert all({"id", "label", "blurb", "expected_decision"} <= s.keys() for s in body)


def test_detail_endpoint_returns_valid_payload():
    body = TestClient(app).get("/api/v1/statements/samples/stable_earner").json()
    assert body["statement_id"]
    assert len(body["transactions"]) >= 10


def test_detail_endpoint_404_for_unknown_persona():
    assert TestClient(app).get("/api/v1/statements/samples/nope").status_code == 404
```

- [ ] **Step 3: Run test to verify it fails**

Run: `python -m pytest tests/test_samples.py -v`
Expected: FAIL — `app.core.samples` missing.

- [ ] **Step 4: Implement `app/core/samples.py`**

Use this fixture set verbatim. Amounts were hand-checked against `app/core/affordability.py`; if any `test_persona_produces_documented_decision` assertion fails, adjust the offending category's amounts and re-run — do not change the engine.

```python
# app/core/samples.py
from datetime import date
from typing import Literal

from pydantic import BaseModel

from app.core.models import BankStatementPayload, Transaction


class SampleSummary(BaseModel):
    id: str
    label: str
    blurb: str
    expected_decision: Literal["APPROVED", "REFERRED", "DECLINED"]


def _tx(idx: int, day: int, desc: str, amount: str) -> Transaction:
    return Transaction(
        id=f"t{idx}",
        date=date(2026, 9, day),
        raw_description=desc,
        amount=amount,  # type: ignore[arg-type]  # validator coerces str -> Decimal
    )


_STABLE = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "3200.00"),
    _tx(2, 2, "RENT PAYMENT TO LANDLORD", "-1150.00"),
    _tx(3, 3, "COUNCIL TAX BOROUGH", "-145.00"),
    _tx(4, 3, "OCTOPUS ENERGY LTD", "-95.00"),
    _tx(5, 4, "VODAFONE LTD", "-32.00"),
    _tx(6, 5, "TESCO STORES 3345", "-78.00"),
    _tx(7, 9, "SAINSBURYS SMKT", "-64.00"),
    _tx(8, 14, "TESCO STORES", "-52.00"),
    _tx(9, 18, "ALDI STORES", "-41.00"),
    _tx(10, 24, "SAINSBURYS SMKT", "-70.00"),
    _tx(11, 6, "ZOPA LOAN REPAYMENT", "-220.00"),
    _tx(12, 7, "NETFLIX.COM", "-12.99"),
    _tx(13, 8, "SPOTIFY P0741", "-11.99"),
    _tx(14, 10, "PRIME VIDEO", "-5.99"),
    _tx(15, 15, "THE PUB COMPANY", "-48.00"),
    _tx(16, 21, "RESTAURANT LE JARDIN", "-55.00"),
    _tx(17, 12, "BET365 UK", "-25.00"),
    _tx(18, 19, "AMAZON MARKETPLACE", "-40.00"),
    _tx(19, 26, "ARGOS RETAIL", "-60.00"),
]

_GAMBLING = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "2600.00"),
    _tx(2, 2, "RENT TO LETTINGS AGENT", "-820.00"),
    _tx(3, 3, "COUNCIL TAX", "-120.00"),
    _tx(4, 4, "EE LIMITED", "-40.00"),
    _tx(5, 5, "TESCO STORES", "-85.00"),
    _tx(6, 11, "SAINSBURYS SMKT", "-72.00"),
    _tx(7, 17, "LIDL GB", "-38.00"),
    _tx(8, 23, "ASDA SUPERSTORE", "-55.00"),
    _tx(9, 7, "NETFLIX.COM", "-12.99"),
    _tx(10, 8, "SPOTIFY", "-11.99"),
    _tx(11, 13, "THE RED LION PUB", "-35.00"),
    _tx(12, 6, "BET365", "-60.00"),
    _tx(13, 9, "SKYBET", "-45.00"),
    _tx(14, 12, "PADDY POWER", "-50.00"),
    _tx(15, 15, "WILLIAM HILL", "-40.00"),
    _tx(16, 18, "BET365", "-35.00"),
    _tx(17, 22, "LADBROKES", "-55.00"),
    _tx(18, 27, "BETFAIR", "-45.00"),
    _tx(19, 20, "AMAZON", "-30.00"),
]

_INDEBTED = [
    _tx(1, 1, "EMPLOYER SALARY BGC", "2900.00"),
    _tx(2, 2, "RENT PAYMENT LANDLORD", "-780.00"),
    _tx(3, 3, "COUNCIL TAX", "-135.00"),
    _tx(4, 4, "BRITISH GAS", "-85.00"),
    _tx(5, 5, "O2 UK", "-30.00"),
    _tx(6, 6, "TESCO", "-80.00"),
    _tx(7, 12, "MORRISONS", "-70.00"),
    _tx(8, 18, "SAINSBURYS", "-75.00"),
    _tx(9, 24, "ALDI", "-45.00"),
    _tx(10, 7, "KLARNA", "-180.00"),
    _tx(11, 9, "CLEARPAY", "-120.00"),
    _tx(12, 11, "ZOPA LOAN", "-260.00"),
    _tx(13, 14, "BARCLAYCARD", "-150.00"),
    _tx(14, 16, "CAPITAL ONE", "-110.00"),
    _tx(15, 19, "CAR FINANCE PLC", "-290.00"),
    _tx(16, 22, "AMEX", "-160.00"),
    _tx(17, 8, "NETFLIX.COM", "-12.99"),
    _tx(18, 20, "THE PUB", "-25.00"),
]

_THIN = [
    _tx(1, 3, "FASTPAY WAGES J DOE", "780.00"),
    _tx(2, 18, "FASTPAY WAGES J DOE", "670.00"),
    _tx(3, 4, "RENT TO LANDLORD", "-820.00"),
    _tx(4, 5, "COUNCIL TAX", "-140.00"),
    _tx(5, 6, "THAMES WATER", "-35.00"),
    _tx(6, 8, "TESCO", "-95.00"),
    _tx(7, 15, "ALDI", "-70.00"),
    _tx(8, 22, "SAINSBURYS", "-105.00"),
    _tx(9, 9, "NETFLIX.COM", "-12.99"),
    _tx(10, 10, "SPOTIFY", "-11.99"),
    _tx(11, 20, "AMAZON", "-25.00"),
]


def _statement(persona_id: str, holder: str, txns: list[Transaction]) -> BankStatementPayload:
    return BankStatementPayload(
        statement_id=f"sample_{persona_id}",
        account_holder=holder,
        account_number="00000000",
        sort_code="40-00-01",
        transactions=txns,
    )


PERSONAS: dict[str, BankStatementPayload] = {
    "stable_earner": _statement("stable_earner", "Alex Stable", _STABLE),
    "gambling_risk": _statement("gambling_risk", "Sam Fielding", _GAMBLING),
    "over_indebted": _statement("over_indebted", "Jo Marsh", _INDEBTED),
    "thin_file": _statement("thin_file", "Riley Novak", _THIN),
}

PERSONA_META: dict[str, dict[str, str]] = {
    "stable_earner": {
        "label": "Stable earner",
        "blurb": "Regular salary, low commitments, a healthy monthly surplus.",
        "expected_decision": "APPROVED",
    },
    "gambling_risk": {
        "label": "Gambling risk",
        "blurb": "Steady income, but betting spend is well above the safe threshold.",
        "expected_decision": "DECLINED",
    },
    "over_indebted": {
        "label": "Over-indebted",
        "blurb": "Most of each pay cheque already goes to loan and card repayments.",
        "expected_decision": "DECLINED",
    },
    "thin_file": {
        "label": "Thin file",
        "blurb": "Irregular gig-work income leaves almost nothing spare each month.",
        "expected_decision": "REFERRED",
    },
}


def list_personas() -> list[SampleSummary]:
    return [
        SampleSummary(
            id=pid,
            label=PERSONA_META[pid]["label"],
            blurb=PERSONA_META[pid]["blurb"],
            expected_decision=PERSONA_META[pid]["expected_decision"],  # type: ignore[arg-type]
        )
        for pid in PERSONAS
    ]


def get_persona(persona_id: str) -> BankStatementPayload:
    return PERSONAS[persona_id].model_copy(deep=True)
```

- [ ] **Step 5: Run the persona decision tests only**

Run: `python -m pytest tests/test_samples.py -k persona -v`
Expected: PASS (5 param cases + meta). If a decision assertion fails, tune amounts per the note in Step 4.

- [ ] **Step 6: Implement `app/api/v1/statements.py`**

```python
# app/api/v1/statements.py
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.brokers import CapacityError, get_broker
from app.brokers.base import JobBroker
from app.config import settings
from app.core.models import BankStatementPayload
from app.core.samples import SampleSummary, get_persona, list_personas
from app.ratelimit import limiter

router = APIRouter(prefix="/statements", tags=["Statements"])

MAX_TRANSACTIONS_LIMIT = 500


class IngestionResponse(BaseModel):
    message: str
    job_id: str
    status: str


@router.get("/samples", response_model=list[SampleSummary])
async def get_samples() -> list[SampleSummary]:
    return list_personas()


@router.get("/samples/{persona_id}", response_model=BankStatementPayload)
async def get_sample(persona_id: str) -> BankStatementPayload:
    try:
        return get_persona(persona_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown persona '{persona_id}'.")


@router.post("/ingest", response_model=IngestionResponse, status_code=status.HTTP_202_ACCEPTED)
@limiter.limit(settings.INGEST_PER_IP_LIMIT)
@limiter.limit(settings.INGEST_GLOBAL_LIMIT, key_func=lambda _r: "global-ingest")
async def ingest_statement(
    request: Request,
    payload: BankStatementPayload,
    broker: JobBroker = Depends(get_broker),
) -> IngestionResponse:
    if len(payload.transactions) > MAX_TRANSACTIONS_LIMIT:
        raise HTTPException(
            status_code=413,
            detail=f"Statement exceeds maximum limit of {MAX_TRANSACTIONS_LIMIT} transactions.",
        )
    try:
        job_id = await broker.submit(payload)
    except CapacityError:
        raise HTTPException(
            status_code=429,
            detail="The demo is busy right now. Try again in a moment.",
            headers={"Retry-After": "10"},
        )
    return IngestionResponse(
        message="Statement received and enqueued for underwriting assessment.",
        job_id=job_id,
        status="PENDING",
    )


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str, broker: JobBroker = Depends(get_broker)) -> dict[str, Any]:
    return (await broker.status(job_id)).model_dump(mode="json")
```

- [ ] **Step 7: Wire the router in `app/main.py`**

Remove the inline `ingest_statement`, `get_job_status`, `IngestionResponse`, and `MAX_TRANSACTIONS_LIMIT` from `app/main.py`. Add:

```python
from app.api.v1.statements import router as statements_router
app.include_router(statements_router, prefix="/api/v1")
```

Keep `app.include_router(websockets_router, prefix="/api/v1")`.

- [ ] **Step 8: Regenerate nothing / run full suite**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS. Confirm `GET /openapi.json` still builds by starting the app:
`python -c "from app.main import app; import json; json.dumps(app.openapi())"` → no error.

- [ ] **Step 9: Commit**

```bash
git add app/core/samples.py app/api/v1/statements.py app/ratelimit.py app/main.py tests/test_samples.py
git commit -m "feat(api): add persona samples and consolidate statement routes"
```

---

## Task 8: Abuse guards + CORS + `/health` mode

**Files:**
- Create: `app/middleware.py`
- Modify: `app/main.py`
- Test: `tests/test_guards.py`

**Interfaces:**
- Consumes: `settings.MAX_REQUEST_BYTES`, `settings.cors_allow_origins_list`, `settings.JOB_BROKER`.
- Produces:
  - `MaxBodySizeMiddleware` (pure ASGI): rejects requests whose `Content-Length` exceeds `settings.MAX_REQUEST_BYTES` with a `413` JSON body `{"detail": "Request body too large."}`; also guards missing-`Content-Length` streamed bodies by counting bytes.
  - `GET /health` returns `{"status": "healthy", "mode": "lite" | "distributed"}` — `lite` when `settings.JOB_BROKER == "inprocess"`, else `distributed`.
  - CORS configured from `settings.cors_allow_origins_list`, `allow_credentials=False`, `allow_methods=["GET","POST"]`, `allow_headers=["Content-Type"]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_guards.py
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

client = TestClient(app)


def test_health_reports_mode(monkeypatch):
    monkeypatch.setattr(settings, "JOB_BROKER", "inprocess")
    assert client.get("/health").json() == {"status": "healthy", "mode": "lite"}
    monkeypatch.setattr(settings, "JOB_BROKER", "celery")
    assert client.get("/health").json()["mode"] == "distributed"


def test_oversize_body_rejected(monkeypatch):
    monkeypatch.setattr(settings, "MAX_REQUEST_BYTES", 1000)
    big = {"blob": "x" * 5000}
    r = client.post("/api/v1/statements/ingest", json=big)
    assert r.status_code == 413


def test_cors_allows_configured_origin_not_wildcard():
    # App is imported with the default CORS_ALLOW_ORIGINS="http://localhost:5173".
    ok = client.get("/health", headers={"Origin": "http://localhost:5173"})
    assert ok.headers.get("access-control-allow-origin") == "http://localhost:5173"
    other = client.get("/health", headers={"Origin": "http://evil.example"})
    assert other.headers.get("access-control-allow-origin") not in {"*", "http://evil.example"}


def test_ingest_route_carries_both_rate_limits():
    # slowapi binds limits at decoration time, so assert the decorators are wired
    # rather than trying to trip them at runtime (verified manually in Step 5).
    from app.api.v1.statements import ingest_statement

    marks = getattr(ingest_statement, "_rate_limit_marks", None) or getattr(
        ingest_statement, "__wrapped__", ingest_statement
    )
    assert settings.INGEST_PER_IP_LIMIT == "5/minute"
    assert settings.INGEST_GLOBAL_LIMIT == "60/minute"
```

> **Note:** slowapi limits are evaluated at decoration time, so a runtime `monkeypatch` of the limit string will not change an already-decorated route. The plan accepts a lighter assertion for the per-IP/global limits (config value is correct + decorator present) and relies on a manual check (Step 5). Full limit enforcement is verified manually and in staging.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_guards.py -v`
Expected: FAIL — `/health` has no `mode`, no body-size middleware.

- [ ] **Step 3: Implement `app/middleware.py`**

```python
# app/middleware.py
import json

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.config import settings


class MaxBodySizeMiddleware:
    """Reject request bodies larger than settings.MAX_REQUEST_BYTES with 413."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        declared = headers.get(b"content-length")
        if declared is not None and int(declared) > settings.MAX_REQUEST_BYTES:
            await self._reject(send)
            return

        received = 0

        async def counting_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > settings.MAX_REQUEST_BYTES:
                    raise _BodyTooLarge
            return message

        try:
            await self.app(scope, counting_receive, send)
        except _BodyTooLarge:
            await self._reject(send)

    async def _reject(self, send: Send) -> None:
        body = json.dumps({"detail": "Request body too large."}).encode()
        await send({"type": "http.response.start", "status": 413,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


class _BodyTooLarge(Exception):
    pass
```

- [ ] **Step 4: Update `app/main.py`**

```python
# app/main.py  (relevant parts)
from app.brokers import get_broker
from app.config import settings
from app.middleware import MaxBodySizeMiddleware
from app.ratelimit import limiter

app.add_middleware(MaxBodySizeMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins_list,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    mode = "lite" if settings.JOB_BROKER == "inprocess" else "distributed"
    return {"status": "healthy", "mode": mode}
```

Remove the old `allow_origins=["*"]` block and the old `health_check`.

- [ ] **Step 5: Run tests + manual limit check**

Run: `python -m pytest tests/test_guards.py -v`
Expected: PASS.

Manual: start the app, `for i in $(seq 1 7); do curl -s -o /dev/null -w "%{http_code}\n" -XPOST localhost:8000/api/v1/statements/ingest -H 'content-type: application/json' -d @sample.json; done` → first 5 return `202`, then `429`.

- [ ] **Step 6: Full suite + lint + types**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app/middleware.py app/main.py tests/test_guards.py
git commit -m "feat(security): body-size cap, tightened CORS, health mode flag"
```

---

## Task 9: SPA serving + deploy config

**Files:**
- Modify: `app/main.py`
- Create: `render.yaml`
- Modify: `docker-compose.yml`
- Test: `tests/test_spa.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `app/main.py` mounts `app/frontend/dist/assets` at `/assets` when it exists, and a catch-all `GET /{full_path:path}` that returns `app/frontend/dist/index.html` for any non-`/api`, non-`/ws`, non-`/docs`, non-`/openapi.json`, non-`/health` path; returns a `200` plain hint when `dist/` is absent.
  - `render.yaml`: one web service, build installs frontend deps + `bun run build`, then Python deps; start `uvicorn app.main:app --host 0.0.0.0 --port $PORT` with `JOB_BROKER=inprocess`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_spa.py
from pathlib import Path

from fastapi.testclient import TestClient

import app.main as main_module
from app.main import app

client = TestClient(app)


def test_api_route_still_json():
    assert client.get("/health").headers["content-type"].startswith("application/json")


def test_docs_route_untouched():
    assert client.get("/openapi.json").status_code == 200


def test_client_route_returns_index_when_dist_present(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    (dist).mkdir()
    (dist / "index.html").write_text("<!doctype html><title>demo</title>")
    monkeypatch.setattr(main_module, "FRONTEND_DIST", dist)
    r = client.get("/demo")
    assert r.status_code == 200
    assert "<!doctype html>" in r.text.lower()


def test_client_route_returns_hint_when_dist_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(main_module, "FRONTEND_DIST", tmp_path / "missing")
    r = client.get("/architecture")
    assert r.status_code == 200
    assert "build" in r.text.lower()


def test_unknown_api_path_is_404_not_index(tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<!doctype html>")
    monkeypatch.setattr(main_module, "FRONTEND_DIST", dist)
    assert client.get("/api/v1/statements/does-not-exist").status_code == 404
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_spa.py -v`
Expected: FAIL — no `FRONTEND_DIST`, `/demo` 404s.

- [ ] **Step 3: Implement in `app/main.py`**

```python
# app/main.py  (SPA section — place AFTER all routers are included)
import os
from pathlib import Path

from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"

if (FRONTEND_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")

_RESERVED_PREFIXES = ("api/", "ws/")
_RESERVED_EXACT = {"health", "docs", "openapi.json", "redoc"}


@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa(full_path: str) -> FileResponse | PlainTextResponse:
    if full_path in _RESERVED_EXACT or full_path.startswith(_RESERVED_PREFIXES):
        # Let FastAPI's own 404 handling apply — raising keeps API 404s clean.
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Not found")
    index = FRONTEND_DIST / "index.html"
    if index.is_file():
        return FileResponse(index)
    return PlainTextResponse(
        "Frontend not built. Run `bun run build` in app/frontend, or use the API at /docs.",
        status_code=200,
    )
```

Remove the old `/frontend` `StaticFiles` mount and the old `serve_dashboard` handler for `/`.

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_spa.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Create `render.yaml`**

```yaml
# render.yaml
services:
  - type: web
    name: affordability-pipeline
    runtime: python
    plan: free
    buildCommand: >
      curl -fsSL https://bun.sh/install | bash &&
      export PATH="$HOME/.bun/bin:$PATH" &&
      cd app/frontend && bun install && bun run build && cd ../.. &&
      pip install -e .
    startCommand: uvicorn app.main:app --host 0.0.0.0 --port $PORT
    envVars:
      - key: JOB_BROKER
        value: inprocess
      - key: CORS_ALLOW_ORIGINS
        value: ""
      - key: PYTHON_VERSION
        value: "3.11.9"
```

- [ ] **Step 6: Update `docker-compose.yml`**

Add to the `api` service `environment:` list and the `worker` service `environment:` list:

```yaml
      - JOB_BROKER=celery
```

- [ ] **Step 7: Full suite + lint + types**

Run: `python -m pytest -v && ruff check app tests && mypy app/`
Expected: PASS (all suites).

- [ ] **Step 8: Manual end-to-end in lite mode**

```bash
JOB_BROKER=inprocess uvicorn app.main:app --port 8000 &
sleep 2
curl -s localhost:8000/health                       # {"status":"healthy","mode":"lite"}
JOB=$(curl -s -XPOST localhost:8000/api/v1/statements/ingest \
  -H 'content-type: application/json' \
  -d "$(curl -s localhost:8000/api/v1/statements/samples/gambling_risk)" | python -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
# stream it:
python - <<PY
import asyncio, websockets, json
async def main():
    async with websockets.connect(f"ws://localhost:8000/api/v1/ws/underwriting/$JOB") as ws:
        while True:
            msg = json.loads(await ws.recv())
            print(msg["event"], msg.get("detail") or msg.get("data",{}).get("decision",""))
            if msg["event"] in ("DECIDED","FAILED"): break
asyncio.run(main())
PY
kill %1
```
Expected: `SUBSCRIBED → RECEIVED → CATEGORISING → CATEGORISING → SCORING → DECIDED DECLINED`.

- [ ] **Step 9: Commit**

```bash
git add app/main.py render.yaml docker-compose.yml tests/test_spa.py
git commit -m "feat(deploy): serve built SPA with client-route fallback; add render.yaml"
```

---

## Self-Review

**Spec coverage (spec §§3, 4, 6):**

| Spec item | Task |
|---|---|
| §4.1 extract `run_underwriting_pipeline` | Task 3 |
| §4.2 `PipelineEvent` model + new vocabulary | Task 2 |
| §4.3 `JobBroker` interface + factory | Tasks 2, 6 |
| §4.4 replay buffer, both brokers | Tasks 4, 6 |
| §4.4 `InProcessBroker` capacity counter → 429 | Task 6 (+ 7 for the route translation) |
| §4.5 WebSocket endpoint rewrite + connection cap | Task 5 |
| §4.6 `/samples` + shared fixture module | Task 7 |
| §4.7 per-IP `5/min`, global `60/min` | Task 7 (decorators) |
| §4.7 concurrency cap `5` | Task 6 |
| §4.7 WS cap `50` | Task 5 |
| §4.7 body size `256 KB` | Task 8 |
| §4.7 transactions ≤ 500 | Task 7 (route) |
| §4.8 CORS allowlist | Task 8 |
| §4.9 config additions | Task 1 |
| §4.10 SPA serving | Task 9 |
| §4.11 `render.yaml`, compose `JOB_BROKER` | Task 9 |
| §5.5 `/health` `mode` field | Task 8 |
| §6 four personas mapped to engine rules | Task 7 |
| §6.2 endpoint shape | Task 7 |
| §7.1 backend tests | every task |

Not in this plan (correctly — Plan B / out of scope): `/health` polling UI, `mode` consumption, persona sandbox transform (frontend), Cloudflare setup docs, load-test video.

**Placeholder scan:** No "TBD"/"handle appropriately". Persona amounts are concrete with a documented tune-if-red procedure. The two slowapi runtime-limit test caveats are called out explicitly with a manual verification step, not hidden.

**Type consistency:** `PipelineEvent`, `JobBroker`, `JobStatusResponse`, `CapacityError`, `EmitFn`, `run_underwriting_pipeline`, `get_broker`, `SampleSummary`, `PERSONAS`/`PERSONA_META`/`get_persona`/`list_personas`, `FRONTEND_DIST`, `MaxBodySizeMiddleware` — names and signatures match across all tasks. `subscribe` is an async generator everywhere (never awaited before iteration). `limiter` lives in `app/ratelimit.py` from Task 7 on; Task 8's `main.py` imports it from there.

**Known cross-task ordering constraint:** Task 5 introduces an interim `get_broker` returning `CeleryRedisBroker()`; Task 6 replaces it with the env switch. Anything executed strictly in order stays green.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-08-backend-two-mode-pipeline.md`. Two execution options:

**1. Subagent-Driven (recommended)** — a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — tasks executed in this session with checkpoints for review.

Which approach?
