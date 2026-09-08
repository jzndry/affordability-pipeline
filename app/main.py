import os
from typing import Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.v1.statements import router as statements_router
from app.api.v1.websockets import router as websockets_router
from app.ratelimit import limiter

app = FastAPI(
    title="Open Banking Affordability Engine",
    version="1.0.0",
    description="Asynchronous credit underwriting and transaction categorisation API.",
)

# Attach rate limiter state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# Allow CORS for cross-origin frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the statement + WebSocket routers under /api/v1
app.include_router(statements_router, prefix="/api/v1")
app.include_router(websockets_router, prefix="/api/v1")

# Mount Static Frontend Assets
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/frontend", StaticFiles(directory=frontend_dir), name="frontend")


@app.get("/", include_in_schema=False)
async def serve_dashboard() -> FileResponse:
    """Serves the real-time underwriter dashboard frontend."""
    index_file = os.path.join(frontend_dir, "index.html")
    return FileResponse(index_file)


@app.get("/health", tags=["Health"])
async def health_check() -> Dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy"}
