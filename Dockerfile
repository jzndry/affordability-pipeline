FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable immediate unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install minimal OS utilities for network health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency specifications first to leverage Docker layer caching
COPY pyproject.toml environment.yml ./

# Install Python packages
RUN pip install --no-cache-dir \
    fastapi>=0.110.0 \
    "uvicorn[standard]>=0.28.0" \
    pydantic>=2.6.0 \
    pydantic-settings>=2.2.0 \
    celery>=5.3.6 \
    "redis>=5.0.0" \
    websockets>=12.0 \
    python-multipart>=0.0.9 \
    httpx>=0.27.0

# Copy application source code into the container
COPY app/ ./app/

# Expose default FastAPI HTTP port
EXPOSE 8000