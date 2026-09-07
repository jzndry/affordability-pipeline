# Frontend Demo & Showcase — Design

**Date:** 2026-09-08
**Branch:** `feature/frontend-basic`
**Status:** Approved design, ready for implementation planning

---

## 1. Goal

Ship a **$0, always-on portfolio demo** of the affordability pipeline that works for
two audiences at once:

- a **non-technical recruiter** who wants to understand what the project does in 60 seconds;
- a **technical reviewer** who wants to see the architecture, the code, and the numbers.

The demo proves the **individual underwriting cycle** honestly and live. A separate page
tells the **at-scale / production** story with a video, because a free single-instance
backend cannot demonstrate it directly.

---

## 2. Locked decisions (context, not up for revision)

1. **Hosting ($0):** frontend built by Vite; **FastAPI serves the built SPA at `/`**
   (single deploy on Render free tier), kept swappable to a separate static host.
   Backend behind Cloudflare (free) for bot/DDoS protection.
2. **Two run modes, one codebase**, selected by env var:
   - **Distributed** (`docker compose up`): Celery + Redis + worker. This is what the
     README, architecture page, and interviews describe.
   - **Lite** (Render deploy): pipeline runs **in-process** (FastAPI `BackgroundTasks`
     + in-process event fan-out), **no Celery/Redis**.
   - Implemented via a `JobBroker` interface with two implementations.
   - **Core domain logic (`app/core/categoriser.py`, `affordability.py`, `models.py`)
     is not touched.**
3. **Cold-start handling:** a lightweight landing page pre-warms the backend
   (`GET /health`), shows an engine status indicator, and disables "Launch demo" until
   healthy. Frontend is deploy-target-agnostic: API + WS base URL from `VITE_API_BASE`.
   Graceful "backend waking / reconnecting" states throughout.
4. **Abuse limits:** per-IP `/ingest` `5/min`, global `/ingest` `60/min`, an
   `asyncio.Semaphore` of `5` concurrent assessments, a global WS connection cap of `50`,
   a `256 KB` request body cap. Anonymous users get **preset personas + small slider
   edits only** — never a free-form JSON box.
5. **Four routes:** `/` Landing, `/demo` Demo, `/scale` At-scale, `/architecture`
   Architecture deep-dive.
6. **Demo UX:** guided persona flow (four pre-built customers) + a sandbox toggle
   (small slider edits). Live pipeline animation driven by WebSocket events. Progressive
   disclosure ("show API calls", "raw pipeline events", "view transactions") for
   technical viewers.
7. **Frontend stack:** React 19 + Vite + bun + TypeScript + oxlint (existing) plus
   Tailwind v4, shadcn/ui, React Router, Framer Motion. Generated OpenAPI types
   (`src/types/schema.d.ts`) are the source of truth; the hand-written
   `src/types/index.tsx` is deleted. `useReducer` state machine for pipeline status.
   Hand-rolled SVG for metric bars/gauges (no charting library).
8. **Visual direction:** *light editorial* — warm off-white ground, serif headings,
   generous whitespace, one restrained accent, the architecture drawn as a deliberate
   diagram. **No dark mode for now** (possible later addition).
9. **Reading toggle:** a **Plain / Technical** switch, default **Plain**, persisted in
   `localStorage`. It swaps **copy** and **disclosure visibility only** — never layout.

---

## 3. Deployment topology

```
                        ┌─────────────────────────────────────────┐
   Browser  ──HTTPS──▶   │  Cloudflare (free proxy: bot/DDoS)      │
                        └───────────────────┬─────────────────────┘
                                            ▼
                        ┌─────────────────────────────────────────┐
                        │  Render free web service                │
                        │  uvicorn app.main:app                   │
                        │  JOB_BROKER=inprocess                    │
                        │                                         │
                        │  GET /            → built SPA (dist/)    │
                        │  GET /assets/*    → static bundle        │
                        │  /api/v1/*, /ws/* → API + WebSocket      │
                        │  pipeline runs in BackgroundTasks        │
                        └─────────────────────────────────────────┘

   `docker compose up`  → same app.main:app with JOB_BROKER=celery,
                          plus redis + celery worker containers.
```

The frontend never assumes same-origin. `VITE_API_BASE` unset ⇒ same origin (Render
default). Set it to the Render URL to host the SPA separately on Vercel / Cloudflare
Pages with no code change.

---

## 4. Backend changes

### 4.1 Extract the pipeline (`app/core/pipeline.py`, new)

Move the parse → categorise → score → decide sequence out of the Celery task into one
transport-agnostic function:

```python
def run_underwriting_pipeline(
    payload: BankStatementPayload,
    emit: Callable[[PipelineEvent], None],
) -> AffordabilityAssessment:
    emit(PipelineEvent(event="RECEIVED"))
    statement = ...  # already a validated BankStatementPayload

    emit(PipelineEvent(event="CATEGORISING"))
    statement.transactions = TransactionCategoriser.process_statement(statement.transactions)
    emit(PipelineEvent(event="CATEGORISING",
                       detail=f"Sorted {n} transactions into {k} categories"))

    emit(PipelineEvent(event="SCORING"))
    assessment = AffordabilityEngine.evaluate(statement)

    emit(PipelineEvent(event="DECIDED", data=assessment.model_dump(mode="json")))
    return assessment
```

`emit` is the only thing that differs between run modes. It is a plain callback; the
pipeline has no knowledge of Celery, Redis, WebSockets, or asyncio.

### 4.2 `PipelineEvent` model (`app/brokers/events.py`, new)

| Field | Type | Notes |
|---|---|---|
| `seq` | `int` | Monotonic per `job_id`, assigned by the broker on publish. Used for ordering and dedupe by the client. |
| `event` | `str` | One of `RECEIVED`, `CATEGORISING`, `SCORING`, `DECIDED`, `FAILED`. (`SUBSCRIBED` is emitted only by the WS layer, not the pipeline.) |
| `job_id` | `str` | |
| `ts` | `float` | Epoch seconds. |
| `detail` | `str \| None` | Human-readable stage note. |
| `data` | `dict \| None` | The `AffordabilityAssessment` dict — **only on `DECIDED`**. |
| `error` | `str \| None` | **Only on `FAILED`**. |

This **replaces** the current ad-hoc wire vocabulary (`SUBSCRIBED` + `ASSESSMENT_COMPLETED`
/ `ASSESSMENT_FAILED`). It is a deliberate breaking change to the WebSocket contract; the
existing WS test suite is rewritten (§7).

### 4.3 `JobBroker` interface (`app/brokers/base.py`, new)

```python
class JobBroker(Protocol):
    def submit(self, payload: BankStatementPayload) -> str:
        """Accept a statement, start processing, return a job_id immediately."""

    def subscribe(self, job_id: str) -> AsyncIterator[PipelineEvent]:
        """Yield every event for job_id in order, replaying any already emitted,
        then tailing live, stopping after a terminal event (DECIDED / FAILED)."""

    async def status(self, job_id: str) -> JobStatusResponse:
        """Point-in-time status for the GET /jobs/{job_id} polling fallback."""
```

`get_broker()` factory reads `settings.JOB_BROKER` and returns a process-wide singleton.
Injected into routes as a FastAPI dependency (`Depends(get_broker)`).

### 4.4 Event delivery — replay buffer (both brokers)

The timing hazard (job finishes before the browser's WebSocket finishes connecting) is
solved uniformly: **every event is written to a short-lived ordered log *and* published
live.** A subscriber reads the log to catch up, then tails live, dedupes by `seq`, and
stops on the terminal event.

- **`InProcessBroker`** (`app/brokers/inprocess.py`, new)
  - Per-job `JobChannel`: `events: list[PipelineEvent]` (replay buffer),
    `done: asyncio.Event`, `subscribers: set[asyncio.Queue]`.
  - A process-wide `active_count` integer tracks in-flight assessments. `submit` rejects
    (raising `CapacityError`, which the ingest route turns into `429` + `Retry-After`)
    when `active_count >= settings.MAX_CONCURRENT_ASSESSMENTS`; otherwise it increments,
    sets `job_id = uuid4()`, creates the channel, and schedules
    `BackgroundTasks.add_task(_run, ...)`.
  - `_run` calls `run_underwriting_pipeline` with an `emit` that appends to `events` and
    puts to every subscriber queue; on exception it emits `FAILED`; `finally` decrements
    `active_count` and marks `done`.
  - `subscribe`: snapshot `events`, register a fresh queue, yield the snapshot, then yield
    from the queue until a terminal event.
  - Channels are dropped `EVENT_LOG_TTL_SECONDS` (900) after completion by a background
    reaper task.
- **`CeleryRedisBroker`** (`app/brokers/celery_redis.py`, new)
  - `submit`: `process_affordability_assessment.delay(payload)`, return `task.id`.
  - `emit` (runs in the worker): `RPUSH underwriting_events:{id}` + `EXPIRE 900` +
    `PUBLISH underwriting_jobs:{id}` (existing channel name).
  - `subscribe`: `LRANGE` the list, then `pubsub` tail on the channel, dedupe by `seq`.
  - `status`: `celery_app.AsyncResult(job_id)` as today.

### 4.5 WebSocket endpoint rewrite (`app/api/v1/websockets.py`)

Collapses to:

```python
@router.websocket("/underwriting/{job_id}")
async def ws_underwriting(websocket, job_id, broker = Depends(get_broker)):
    if ws_connection_count >= settings.MAX_WS_CONNECTIONS:
        await websocket.close(code=1013)  # try again later
        return
    await websocket.accept()
    ws_connection_count += 1
    try:
        await websocket.send_json({"event": "SUBSCRIBED", "job_id": job_id})
        async for event in broker.subscribe(job_id):
            await websocket.send_json(event.model_dump(mode="json"))
    except WebSocketDisconnect:
        ...
    finally:
        ws_connection_count -= 1
```

The `AsyncResult.ready()` race special-casing is deleted — the replay buffer handles it.

### 4.6 Persona samples endpoint (`app/api/v1/statements.py`, new router)

- **Canonical persona statements live in `app/core/samples.py`** (new) as a dict of four
  `BankStatementPayload` fixtures. The **pytest suite imports the same module** — single
  source of truth.
- `GET /api/v1/statements/samples` → `list[SampleSummary]` (`id`, `label`, `blurb`,
  `expected_decision`).
- `GET /api/v1/statements/samples/{persona_id}` → the full `BankStatementPayload`.
- The existing `POST /ingest` and `GET /jobs/{job_id}` move into this router unchanged in
  behaviour (rate limits reapplied, see §4.7).

### 4.7 Abuse guards

| Guard | Value (config key) | Implementation |
|---|---|---|
| Per-IP `/ingest` | `5/minute` (`INGEST_PER_IP_LIMIT`) | slowapi `@limiter.limit`, key = remote address → `429` |
| Global `/ingest` | `60/minute` (`INGEST_GLOBAL_LIMIT`) | second `@limiter.limit` with a constant key func → `429` |
| Concurrent assessments | `5` (`MAX_CONCURRENT_ASSESSMENTS`) | `InProcessBroker.submit` rejects when its `active_count` is at the cap → route returns `429` + `Retry-After` |
| Live WS connections | `50` (`MAX_WS_CONNECTIONS`) | process counter in the WS endpoint → close `1013` |
| Request body size | `262144` (`MAX_REQUEST_BYTES`) | ASGI middleware rejecting on `Content-Length` / streamed overflow → `413` |
| Transactions per statement | `500` (existing `MAX_TRANSACTIONS_LIMIT`) | keep the check in the ingest route → `413` |
| Bot / DDoS | — | Cloudflare proxy, deploy config only |

In distributed mode the concurrency cap is a soft check (worker concurrency is the real
limit); in lite mode it is the real safety valve.

### 4.8 CORS (`app/main.py`)

Replace `allow_origins=["*"], allow_credentials=True` (an invalid combination) with:

```python
CORSMiddleware(
    allow_origins=settings.cors_allow_origins_list,   # parsed from comma-separated env
    allow_credentials=False,                          # no cookies/auth anywhere
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)
```

Local default: `http://localhost:5173`. Production: the deployed origin, or empty when
same-origin only.

### 4.9 Config additions (`app/config.py`)

```python
JOB_BROKER: Literal["celery", "inprocess"] = "celery"
CORS_ALLOW_ORIGINS: str = "http://localhost:5173"
MAX_CONCURRENT_ASSESSMENTS: int = 5
MAX_WS_CONNECTIONS: int = 50
MAX_REQUEST_BYTES: int = 262_144
INGEST_PER_IP_LIMIT: str = "5/minute"
INGEST_GLOBAL_LIMIT: str = "60/minute"
EVENT_LOG_TTL_SECONDS: int = 900
```

### 4.10 SPA serving (`app/main.py`)

- Build output is `app/frontend/dist/`.
- Mount `dist/assets` as static; add a catch-all `GET /{path:path}` that returns
  `dist/index.html` for any non-API, non-WS path (client-side routing fallback).
- Remove the current `/frontend` `StaticFiles` mount and the `index.html`-from-source
  `serve_dashboard` handler.
- If `dist/` is absent (pure API dev), the catch-all returns a plain hint instead.

### 4.11 Deployment config

- `render.yaml`: build command installs bun deps and runs `bun run build` in
  `app/frontend`, then the Python service starts `uvicorn app.main:app` with
  `JOB_BROKER=inprocess`.
- `docker-compose.yml`: add `JOB_BROKER=celery` to the web service env (explicit default).
- Document the Cloudflare proxy step in the README / architecture page.

---

## 5. Frontend

### 5.1 Tooling additions

- **Runtime deps:** `react-router-dom` (v7), `tailwindcss` v4 + `@tailwindcss/vite`,
  `motion` (Framer Motion), shadcn/ui deps (`class-variance-authority`, `clsx`,
  `tailwind-merge`, `lucide-react`, required `@radix-ui/*` primitives, `tw-animate-css`).
- **Test deps:** `vitest`, `@testing-library/react`, `@testing-library/user-event`,
  `jsdom`. Add `"test": "vitest"` script.
- `oxlint` stays. `bun run generate-types` stays the way `schema.d.ts` is refreshed.

### 5.2 Source layout (`app/frontend/src/`)

```
main.tsx                 Router + providers
index.css                Tailwind entry + design tokens (light editorial)
routes/
  Landing.tsx            pitch, diagram preview, how-it-works, engine warm-up
  Demo.tsx               the living-architecture demo
  Scale.tsx              at-scale story + load-test video
  Architecture.tsx       deep-dive: snippets, ADRs, benchmarks, distributed-vs-lite
components/
  layout/     TopBar, ReadingToggle, EngineStatus, PageNav
  persona/    PersonaPicker, SandboxControls (4 sliders), StatementTable
  pipeline/   PipelineDiagram, PipelineNode, StageCaption, ModeAnnotation
  decision/   DecisionCard, MetricBar (SVG), MetricGauge (SVG), RiskFlagList
  disclosure/ ApiCallPanel, RawEventsPanel
  ui/         shadcn primitives
lib/
  env.ts              resolve VITE_API_BASE → api + ws base URLs
  api.ts              typed fetch client keyed off schema.d.ts `paths`
  ws.ts               WebSocket client: connect, reconnect w/ backoff, seq dedupe
  pipelineMachine.ts  useReducer state + reducer + transitions
  pacing.ts           minimum-stage-display-time queue
  reading.tsx         Plain/Technical context + persisted preference
  personas.ts         persona typing + sandbox slider → transaction transform
content/
  copy.ts             every Plain vs Technical string, one keyed table
  architecture.ts     ADR summaries, code snippets, benchmark figures
types/
  schema.d.ts          generated — kept
                       (src/types/index.tsx is deleted)
```

### 5.3 Env & clients

- `env.ts`: `API_BASE = import.meta.env.VITE_API_BASE ?? ""` (same origin when empty).
  `WS_BASE` = `API_BASE` with `http→ws` / `https→wss`; when empty, derived from
  `window.location`.
- `api.ts`: thin typed wrapper — `getSamples()`, `getSample(id)`, `ingest(payload)`,
  `getJob(id)`, `health()`. Request/response types come from `schema.d.ts`
  (`components["schemas"][...]`).
- `ws.ts`: opens `WS_BASE + /api/v1/ws/underwriting/{job_id}`, parses frames into
  `PipelineEvent`, drops duplicates by `seq`, emits to a listener. On unexpected close
  before a terminal event: reconnect with exponential backoff (0.5s → 8s, ~6 tries),
  surface a `reconnecting` status; the replay buffer refills the gap on reconnect.

### 5.4 Pipeline state machine (`pipelineMachine.ts`)

Reducer states: `idle → warming → submitting → queued → categorising → scoring →
decided → error`.

| From | Event | To |
|---|---|---|
| `idle` | `RUN` (health confirmed) | `submitting` |
| `idle` | `RUN` (health unknown/cold) | `warming` |
| `warming` | `HEALTHY` | `submitting` |
| `warming` | `WARM_TIMEOUT` | `error` |
| `submitting` | `ACCEPTED` (202 + job_id) | `queued` |
| `submitting` | `HTTP_ERROR` / `429` | `error` |
| `queued` | ws `RECEIVED` or `CATEGORISING` | `categorising` |
| `categorising` | ws `SCORING` | `scoring` |
| `scoring` | ws `DECIDED` | `decided` |
| any active | ws `FAILED` / `WS_GIVEUP` / timeout | `error` |
| `decided` / `error` | `RESET` (try another / re-run) | `idle` |

**Presentation pacing is a separate layer.** `pacing.ts` wraps the raw event stream: when
events arrive faster than a floor (~700 ms/stage) it releases stage transitions on a
timer so `categorising` and `scoring` are each visible. The reducer consumes the paced
stream. `decided`'s payload (the assessment) is delivered immediately underneath but the
card reveal waits for the pacing queue to drain.

### 5.5 Diagram ↔ state mapping (`PipelineDiagram`)

Six nodes: **Client → FastAPI → Redis queue → Worker → Live feed → Decision**.

| Reducer state | Node states |
|---|---|
| `submitting` | Client *active* |
| `queued` | Client, FastAPI *done*; Live feed *connecting* |
| `categorising` | + Redis queue *done*; Worker *active* (caption: "Sorting transactions…") |
| `scoring` | Worker *active* (caption: "Applying affordability rules…") |
| `decided` | Worker, Live feed *done*; Decision *complete* |
| `error` | last-reached node *error*; rest dimmed |

**Lite-mode annotation:** in lite mode `ModeAnnotation` draws a bracket around Redis
queue + Worker labelled *"on the free demo these run in one process — `docker compose up`
runs them as separate services."* The nodes still light in sequence. Which mode is
active is read from a `GET /health` field (add `"mode": "lite" | "distributed"` to the
health response).

### 5.6 Plain / Technical toggle (`reading.tsx`)

- React context exposes `reading: "plain" | "technical"` and `setReading`, persisted to
  `localStorage` (`afford.reading`), default `plain`. Wrapped in try/catch for private
  windows.
- `content/copy.ts` is a keyed table: `copy["node.worker"] = { plain: "Assessor",
  technical: "Worker" }`, etc. Every user-facing string in the demo/diagram/decision goes
  through it.
- Disclosure panels (`ApiCallPanel`, `RawEventsPanel`, `StatementTable` trigger) render
  only when `reading === "technical"`; in plain mode they are absent, not collapsed.
- Layout, spacing, and component tree are identical in both modes.

### 5.7 Demo page composition (`routes/Demo.tsx`)

Top to bottom, no step is hidden behind a scroll before it is needed:

1. **TopBar** — title, `EngineStatus` chip, `ReadingToggle`.
2. **PersonaPicker** — four cards (Stable earner / Gambling risk / Over-indebted /
   Thin file), always visible; selecting one re-arms the diagram.
3. **SandboxControls** — a disclosure toggle ("Customise this statement") revealing four
   sliders: monthly income, gambling spend, rent, loan repayments. Each slider rescales
   its category's transactions in the persona payload (see §6.3). `StatementTable` (the
   "view transactions" disclosure) shows the resulting list.
4. **PipelineDiagram** — the six-node living architecture; `StageCaption` beneath it.
   A **Run the check** button sits on the diagram in `idle`.
5. **DecisionCard** — animates in beneath the diagram on `decided`; the fully-lit diagram
   stays on screen as the backdrop. Contents:
   - decision badge (APPROVED / REFERRED / DECLINED) + one-line plain or technical gloss;
   - three hand-rolled SVG metrics: **net disposable vs £150 line** (`MetricBar`),
     **debt-to-income** (`MetricGauge`, 40% marker), **gambling share** (`MetricGauge`,
     5% and 10% markers);
   - `RiskFlagList` — the exact `risk_flags` strings from the assessment;
   - actions: *Try another persona*, *Tweak & re-run*, *Full metric breakdown*.
6. **Disclosure (technical only)** — `ApiCallPanel` (the real `GET /samples/{id}` →
   `POST /ingest` → `WS` sequence with payloads), `RawEventsPanel` (the `PipelineEvent`
   frames as received).
7. **CTA** — "See how this runs for thousands of users →" links to `/scale`.

### 5.8 Landing page (`routes/Landing.tsx`)

- One-paragraph plain-English pitch (from the README).
- A **static preview** of the pipeline diagram (non-interactive).
- "How it works" — three or four short steps.
- **Engine warm-up:** on mount, `GET /health` every 3 s (backing off to 10 s), up to
  ~90 s. `EngineStatus`: *starting…* → *ready*. **Launch demo** disabled until healthy,
  then routes to `/demo`.
- Link row to `/scale` and `/architecture`.

### 5.9 At-scale page (`routes/Scale.tsx`)

- Narrative: why one instance can't show this; what changes under load.
- **Load-test video slot** — split screen (Locust request graph ∥ a wall of decision
  cards resolving) recorded from `docker compose up` under `benchmarks/locustfile.py`.
  The page ships with a placeholder poster; the actual recording is produced separately
  (§9).
- Throughput / latency figures from `benchmarks/benchmark_engine.py` (per-statement ms,
  sustained statements/sec), rendered as plain callouts.
- Distributed-vs-lite explanation with the two-mode diagram.
- Concurrency & backpressure: the semaphore, the queue, the WS cap — what each protects.

### 5.10 Architecture deep-dive (`routes/Architecture.tsx`)

- Annotated code snippets (categoriser regex, the policy matrix, the `emit` seam, the WS
  loop) from `content/architecture.ts`.
- ADR summaries (001–006) with links to `docs/adr/`.
- The `JobBroker` two-implementation design.
- Real-time delivery walkthrough (replay buffer + Pub/Sub).
- Benchmark methodology and numbers.

### 5.11 Visual design system

- **Ground:** warm off-white (`#fbfaf7`), ink `#1c1b19`, muted `#6b6a63`.
- **Headings:** serif (Georgia / system serif stack). **Body & UI:** system sans.
- **Accent:** a single warm amber for the live stage and REFERRED; green for
  done/APPROVED; a restrained red for DECLINED/errors. Reserve saturation for signal.
- Generous whitespace, thin rules, the diagram as a deliberately drafted object.
- Tokens defined once in `index.css`; Tailwind v4 `@theme` maps them to utilities.
- shadcn/ui components restyled to these tokens.

---

## 6. Personas (`app/core/samples.py`)

Four `BankStatementPayload` fixtures, one month of transactions each. Amounts below are
**illustrative targets**; implementation tunes exact transaction lists so each metric
lands clearly clear of its threshold (never on the boundary). Each persona maps to one
engine rule.

| Persona | `id` | Income | Key spend | Metrics | Decision | Flag |
|---|---|---|---|---|---|---|
| **Stable earner** | `stable_earner` | £3,200 salary | rent £1,150, bills £180, groceries £460, car loan £220, entertainment £240, gambling £30 | net ≈ £720, DTI ≈ 7%, gambling ≈ 1% | **APPROVED** | none |
| **Gambling risk** | `gambling_risk` | £2,600 | rent £820, bills £160, groceries £300, entertainment £120, **gambling ≈ £330** across many bookmakers | gambling ≈ 12.7%, net ≈ £870 | **DECLINED** | `EXCESSIVE_GAMBLING_RISK (12.7% of income)` |
| **Over-indebted** | `over_indebted` | £2,900 | rent £780, bills £150, groceries £330, **loan repayments ≈ £1,270** (Klarna, personal loan, card minimum, car finance), entertainment £90 | DTI ≈ 43.8%, net ≈ £280 | **DECLINED** | `HIGH_DEBT_TO_INCOME_RATIO (43.8%)` |
| **Thin file** | `thin_file` | ≈ £1,450 across two irregular gig-work deposits | rent £780, bills £140, groceries £300, entertainment £90; no loans, no gambling; ~15 transactions | net ≈ £140 | **REFERRED** | `LOW_NET_DISPOSABLE_INCOME (£140.00)` |

### 6.1 Design notes

- **Gambling risk** and **Over-indebted** keep net disposable comfortably above £150 so
  each has a **single** headline flag — the demo teaches one rule at a time. **Thin file**
  is the deliberate REFERRED case.
- Transaction descriptions use realistic raw bank strings (e.g. `POS 4829 14SEP26 BET365
  UK`, `DD KLARNA*ORDER`, `FASTER PAYMENT UPWORK ESCROW`) so the categoriser does real
  work and the `RawEventsPanel` / `StatementTable` look authentic.

### 6.2 Endpoint shape

```
GET /api/v1/statements/samples
 → [{ "id": "stable_earner", "label": "Stable earner",
      "blurb": "Regular salary, low commitments.",
      "expected_decision": "APPROVED" }, ...]

GET /api/v1/statements/samples/stable_earner
 → full BankStatementPayload
```

### 6.3 Sandbox slider transform (`lib/personas.ts`)

- Each of the four sliders owns one category group in the loaded payload
  (income → positive transactions; gambling / rent / loan → the matching categorised
  negatives).
- The slider's base value is that group's current summed magnitude. On change, every
  transaction in the group is scaled by `slider / base` (amounts re-rounded to the penny;
  the largest transaction absorbs any rounding remainder so the group total is exact).
- Non-owned transactions are untouched. The full modified list is what `POST /ingest`
  receives, and what `StatementTable` renders — no synthetic "adjustment" line.
- Slider ranges: income £0–£6,000; gambling £0–£1,200; rent £0–£2,500; loan £0–£2,500.
  Income at £0 is allowed (demonstrates `NO_VERIFIABLE_INCOME`).

---

## 7. Testing

### 7.1 Backend

- **Rewrite the WebSocket suite** for the new `PipelineEvent` vocabulary and the broker
  seam.
- **`InProcessBroker`:** `submit` → `subscribe` yields the full ordered sequence
  (`RECEIVED … DECIDED`); a subscriber that connects *after* completion still receives the
  full replayed sequence; `FAILED` is emitted when the pipeline raises; a 6th concurrent
  `submit` is rejected with `CapacityError` while five are in flight.
- **`CeleryRedisBroker`:** same sequence guarantees against a real Redis (existing test
  infra); `LRANGE` replay + Pub/Sub tail dedupe by `seq`.
- **`run_underwriting_pipeline`:** emits stages in order; produces the same
  `AffordabilityAssessment` as the current worker for a fixed payload (regression pin).
- **`/samples`:** returns four valid payloads; running the pipeline on each yields the
  documented decision and flag (guards against threshold drift).
- **Guards:** body-size middleware → `413`; per-IP and global ingest limits → `429`;
  CORS headers reflect the configured allowlist.

### 7.2 Frontend (Vitest)

- `pipelineMachine` reducer: every transition in the table, including error paths.
- `pacing.ts`: bursts of simultaneous events are released no faster than the floor;
  ordering preserved.
- `personas.ts`: slider transform scales the right group, preserves the exact target
  total, leaves other transactions untouched.
- `ws.ts`: dedupes by `seq`; reconnect fires on premature close and stops after a terminal
  event.
- `DecisionCard`: renders each of APPROVED / REFERRED / DECLINED with correct metric and
  flag display.
- `reading.tsx`: toggle persists; disclosure panels absent in plain mode.

---

## 8. Error handling

| Situation | Behaviour |
|---|---|
| Backend cold on `/demo` | `warming` state; poll `/health`; Run disabled with "waking the engine…" |
| Warm-up exceeds ~90 s | `error` with a retry button; link to `/docs` and GitHub as a fallback |
| `POST /ingest` → `429` | `error`: "The demo is busy right now — try again in a moment." + `Retry-After` countdown |
| `POST /ingest` → 4xx/5xx | `error` with the server message (technical) or a plain apology (plain) |
| WS closes before terminal event | `reconnecting` badge; backoff retries; replay buffer refills on reconnect; give up → `error` |
| WS `FAILED` event | `error` with the `error` string (technical) / plain message; diagram marks the worker node |
| `localStorage` unavailable | reading toggle still works in-memory, defaults to plain |
| `dist/` missing on the server | catch-all returns a plain hint; API and `/docs` still work |

---

## 9. Out of scope

- **Load-test video production** — the `/scale` page ships with a placeholder; recording
  the split-screen Locust run is a separate task.
- **Plaid** — the adapter stays unwired and disabled.
- **Dark mode** — deferred; tokens are structured to allow it later.
- **Auth, persistence, accounts, real multi-user streaming on the deployed demo.**
- Changes to `app/core/categoriser.py`, `affordability.py`, `models.py`.

---

## 10. Implementation order (high level)

1. Backend: config + `PipelineEvent` + `run_underwriting_pipeline` extraction (behaviour-preserving) + rewrite worker as a thin wrapper; update existing tests.
2. Backend: `JobBroker` base + `CeleryRedisBroker` + replay buffer; rewrite WS endpoint; rewrite WS tests.
3. Backend: `InProcessBroker` + semaphore; broker factory + DI; `JOB_BROKER` wiring in compose.
4. Backend: `app/core/samples.py` + `/samples` router; move ingest/jobs routes; persona decision tests.
5. Backend: abuse guards (body-size middleware, global ingest limit, WS cap) + CORS + `/health` mode field.
6. Backend: SPA-serving catch-all + `render.yaml`.
7. Frontend: tooling (Tailwind v4, Router, Framer Motion, shadcn, Vitest); delete `src/types/index.tsx`; `env.ts` / `api.ts` / `ws.ts`.
8. Frontend: `pipelineMachine` + `pacing` + reducer/pacing tests.
9. Frontend: `reading.tsx` + `content/copy.ts`; `TopBar` / `EngineStatus` / `ReadingToggle`.
10. Frontend: `PersonaPicker` / `SandboxControls` / `StatementTable` + `personas.ts` transform.
11. Frontend: `PipelineDiagram` + `StageCaption` + `ModeAnnotation`; wire to the machine.
12. Frontend: `DecisionCard` + SVG `MetricBar` / `MetricGauge` / `RiskFlagList`.
13. Frontend: disclosure panels (`ApiCallPanel`, `RawEventsPanel`).
14. Frontend: `Demo.tsx` assembly; end-to-end against a local backend.
15. Frontend: `Landing.tsx` + warm-up.
16. Frontend: `Scale.tsx` (placeholder video) + `Architecture.tsx` + `content/architecture.ts`.
17. Deploy: Render + Cloudflare; smoke-test lite mode cold start.

---

## 11. Open items (non-blocking)

- Exact transaction lists for the four personas — finalised during step 4, verified by the
  decision tests.
- Benchmark figures for `/scale` and `/architecture` — captured during step 16 from a
  clean `benchmarks/` run.
- Final Cloudflare setup steps — documented during step 17.
