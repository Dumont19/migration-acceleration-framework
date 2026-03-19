# Migration Acceleration Framework v4.0

Oracle → Snowflake migration orchestration platform.

**Stack:** FastAPI + PostgreSQL (backend) · Next.js 14 (frontend) · Docker Compose

---

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your Oracle, Snowflake, S3 and PostgreSQL credentials

# 2. Start PostgreSQL
docker-compose up -d postgres

# 3. Run DB migrations
cd backend && alembic upgrade head && cd ..

# 4. Start everything
docker-compose up
```

Frontend: http://localhost:3000  
API docs:  http://localhost:8000/docs

---

## Structure

```
migration-acceleration-framework/
├── backend/          # FastAPI — REST + WebSocket API
│   ├── app/
│   │   ├── api/      # Route handlers (thin)
│   │   ├── core/     # Config, DB, Oracle, Snowflake, S3 clients
│   │   ├── models/   # ORM models (PostgreSQL) + Pydantic schemas
│   │   └── services/ # Business logic
│   ├── migrations/   # Alembic migrations
│   └── tests/        # pytest unit + integration tests
│
├── frontend/         # Next.js 14 App Router
│   └── src/
│       ├── app/      # Pages (8 routes)
│       ├── components/
│       ├── lib/      # API client + WebSocket hook
│       └── styles/   # CSS design system
│
├── docker-compose.yml
└── .env.example
```

---

## Pages

| Route         | Feature                          |
|---------------|----------------------------------|
| `/`           | Dashboard — system health + jobs |
| `/migration`  | Partitioned migration (S3)       |
| `/dblink`     | DB Link migration (direct)       |
| `/gaps`       | Volumetric gap analysis          |
| `/docs`       | DataStage XML documentation      |
| `/lineage`    | SOURCE → JOB → TARGET graph      |
| `/validation` | Oracle vs Snowflake comparison   |
| `/logs`       | Persistent audit trail           |
| `/settings`   | Connection configuration         |

---

## Log Database

All execution events are stored in PostgreSQL (`maf_logs`):

- `migration_jobs` — one row per migration run, full lifecycle tracking
- `job_logs` — append-only audit trail, queryable via `/logs` page
- `job_partitions` — partition-level granularity (retry individual partitions)
- `validation_runs` — historical validation results

---

## Development

```bash
# Backend
cd backend
pip install -e ".[dev]"
uvicorn app.main:app --reload

# Frontend
cd frontend
npm install
npm run dev

# Tests
cd backend && pytest

# Lint
cd backend && ruff check . && ruff format .
cd frontend && npm run lint
```
