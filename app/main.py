from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.v1.statements import router as statements_router
from app.api.v1.websockets import router as websockets_router
from app.config import settings
from app.middleware import MaxBodySizeMiddleware
from app.ratelimit import limiter

app = FastAPI(
    title="Open Banking Affordability Engine",
    version="1.0.0",
    description="Asynchronous credit underwriting and transaction categorisation API.",
)

# Attach rate limiter state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# Cap request body size before routing/validation. Added first so that, since
# add_middleware prepends, CORSMiddleware ends up outermost (a 413 still gets CORS headers).
app.add_middleware(MaxBodySizeMiddleware)

# Allow CORS for the configured frontend origins only. Wildcard origins are incompatible
# with credentialed requests, so we use an env-driven allowlist and disable credentials.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins_list,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# Mount the statement + WebSocket routers under /api/v1
app.include_router(statements_router, prefix="/api/v1")
app.include_router(websockets_router, prefix="/api/v1")


@app.get("/health", tags=["Health"])
async def health_check() -> dict[str, str]:
    """Basic health check endpoint; reports the active job-processing mode."""
    mode = "lite" if settings.JOB_BROKER == "inprocess" else "distributed"
    return {"status": "healthy", "mode": mode}


# --- Single-page-app serving -------------------------------------------------
# Serve the built React frontend (app/frontend/dist/) with a client-side-routing
# fallback. The catch-all below MUST stay the last route registered so it never
# shadows /api/..., /health, or the docs routes.
FRONTEND_DIST = Path(__file__).parent / "frontend" / "dist"

if (FRONTEND_DIST / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets")

_RESERVED_PREFIXES = ("api/", "ws/")
_RESERVED_EXACT = {"health", "docs", "openapi.json", "redoc"}


@app.get("/{full_path:path}", include_in_schema=False, response_model=None)
async def serve_spa(full_path: str) -> FileResponse | PlainTextResponse:
    if full_path in _RESERVED_EXACT or full_path.startswith(_RESERVED_PREFIXES):
        raise HTTPException(status_code=404, detail="Not found")
    index = FRONTEND_DIST / "index.html"
    if index.is_file():
        return FileResponse(index)
    return PlainTextResponse(
        "Frontend not built. Run `bun run build` in app/frontend, or use the API at /docs.",
        status_code=200,
    )
