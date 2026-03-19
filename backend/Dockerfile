FROM python:3.11-slim AS base

# System deps for oracledb thin mode (no Oracle Instant Client needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Dependencies ──────────────────────────────────────────────────────────────
FROM base AS deps
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]"

# ── Production image ──────────────────────────────────────────────────────────
FROM deps AS production
COPY . .

# Run Alembic migrations then start server
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2"]

EXPOSE 8000
