# MAF — Migration Acceleration Framework

> Oracle → Snowflake migration orchestration platform  
> Oracle → Snowflake migration acceleration tool · DataStage 11.5+

![Stack](https://img.shields.io/badge/backend-FastAPI-009688?style=flat-square&logo=fastapi)
![Stack](https://img.shields.io/badge/frontend-Next.js%2014-000000?style=flat-square&logo=next.js)
![Stack](https://img.shields.io/badge/database-PostgreSQL-336791?style=flat-square&logo=postgresql)
![Stack](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python)
![Stack](https://img.shields.io/badge/license-MIT-green?style=flat-square)

---

## Overview

MAF is a full-stack internal tooling platform that accelerates and documents Oracle-to-Snowflake data warehouse migrations. It provides:

- **Partitioned migrations** with resumable state, parallel workers and real-time WebSocket progress
- **DataStage XML documentation** — parses `.dsx` export files and generates HTML reports with SQL syntax highlighting, transformer column derivations, and dependency maps
- **Gap analysis** — volumetric row count comparison between Oracle and Snowflake per date partition
- **Validation** — schema + count + sample-level reconciliation between source and target
- **Persistent audit trail** — all execution events stored in PostgreSQL, queryable via the UI
- **Lineage graph** — interactive SOURCE → JOB → TARGET visualization from DataStage XML

---

## Stack

| Layer | Technology |
|---|---|
| Backend API | FastAPI 0.111+, Python 3.11+ |
| Frontend | Next.js 14 (App Router), React 18 |
| Audit database | PostgreSQL 15+ (via asyncpg + SQLAlchemy 2.0) |
| Migrations | Alembic |
| Source DB | Oracle (via oracledb thin mode) |
| Target DW | Snowflake (via snowflake-connector-python) |
| File staging | AWS S3 (via boto3) |
| Real-time | WebSocket (Starlette) |
| XML parsing | BeautifulSoup4 + lxml |

---

## Prerequisites

Before running locally, make sure you have:

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11 or 3.12 | 3.13 works, avoid 3.14 |
| Node.js | 20+ | |
| PostgreSQL | 15+ | Local or remote |
| Git | any | |

> Oracle, Snowflake and S3 credentials are optional for local development —  
> the app starts without them and shows connection errors only in the Settings page.

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/Dumont19/migration-acceleration-framework.git
cd migration-acceleration-framework
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and fill in your credentials. The only **required** section to get started is PostgreSQL:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=maf_logs
DB_USER=maf_user
DB_PASSWORD=your_password
```

Everything else (Oracle, Snowflake, S3) can be left as placeholder — the app will start and show connection status in the Settings page.

### 3. Create the PostgreSQL database

```bash
psql -U postgres -c "CREATE DATABASE maf_logs;"
psql -U postgres -c "CREATE USER maf_user WITH PASSWORD 'your_password';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE maf_logs TO maf_user;"
psql -U postgres -c "ALTER DATABASE maf_logs OWNER TO maf_user;"
```

> **Windows users:** if `psql` is not recognized, add `C:\Program Files\PostgreSQL\<version>\bin` to your PATH,  
> or use pgAdmin 4 (installed with PostgreSQL) to create the database via the GUI.

### 4. Install and start the backend

```bash
cd backend

# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Create database tables
alembic upgrade head

# Start the API server
uvicorn app.main:app --reload --port 8000
```

API is available at **http://localhost:8000**  
Interactive docs at **http://localhost:8000/docs**

### 5. Install and start the frontend

Open a **new terminal** in the project root:

```bash
cd frontend
npm install
npm run dev
```

Application is available at **http://localhost:3000**

---

## Project Structure

```
migration-acceleration-framework/
│
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── routes/
│   │   │   │   ├── migration.py     # POST /api/migration/start, job CRUD
│   │   │   │   ├── datastage.py     # POST /api/datastage/analyze, /report, /lineage
│   │   │   │   ├── logs.py          # GET /api/logs (paginated audit trail)
│   │   │   │   └── health.py        # GET /api/health (parallel connection check)
│   │   │   └── ws/
│   │   │       └── progress.py      # WebSocket: ws://localhost:8000/ws/progress/{job_id}
│   │   ├── core/
│   │   │   ├── config.py            # Pydantic Settings (all env vars)
│   │   │   ├── database.py          # Async SQLAlchemy engine + session
│   │   │   ├── logging.py           # structlog + PostgreSQL sink
│   │   │   ├── oracle_client.py     # Oracle async connection pool
│   │   │   ├── snowflake_client.py  # Snowflake SQLAlchemy engine
│   │   │   └── s3_client.py         # boto3 async wrapper
│   │   ├── models/
│   │   │   ├── logs.py              # ORM: migration_jobs, job_logs, job_partitions, validation_runs
│   │   │   └── schemas.py           # Pydantic request/response schemas
│   │   └── services/
│   │       ├── migration/
│   │       │   ├── partitioned.py   # Core migration engine (Oracle → CSV.GZ → S3 → Snowflake → MERGE)
│   │       │   └── state.py         # Job state management (replaces scattered JSON files)
│   │       ├── validation/
│   │       │   └── comparator.py    # Oracle vs Snowflake: counts + schema + sample
│   │       └── datastage/
│   │           ├── xml_analyzer.py  # DataStage XML parser (ported from MAF v1)
│   │           └── xml_parser.py    # Lightweight lineage graph builder
│   ├── migrations/
│   │   └── versions/
│   │       └── 0001_initial.py      # Alembic: creates all 4 tables
│   ├── tests/
│   │   ├── conftest.py
│   │   └── unit/
│   │       └── test_services.py
│   └── pyproject.toml
│
├── frontend/
│   └── src/
│       ├── app/                     # Next.js App Router pages
│       │   ├── page.tsx             # /  → Dashboard
│       │   ├── migration/page.tsx   # /migration
│       │   ├── dblink/page.tsx      # /dblink
│       │   ├── gaps/page.tsx        # /gaps
│       │   ├── docs/page.tsx        # /docs
│       │   ├── lineage/page.tsx     # /lineage
│       │   ├── validation/page.tsx  # /validation
│       │   ├── logs/page.tsx        # /logs
│       │   └── settings/page.tsx   # /settings
│       ├── components/
│       │   ├── layout/
│       │   │   ├── Sidebar.tsx
│       │   │   └── TopBar.tsx       # Theme toggle (dark/light)
│       │   └── ui/
│       │       └── index.tsx        # StatCard, SectionLabel, CodeBlock, etc.
│       ├── context/
│       │   └── ThemeContext.tsx     # Dark/light theme (persisted in localStorage)
│       ├── lib/
│       │   ├── api.ts               # Typed fetch client
│       │   └── useJobProgress.ts    # WebSocket hook with reconnect
│       └── styles/
│           └── globals.css          # CSS design system (light default, dark via data-theme)
│
├── .env.example
├── docker-compose.yml
└── README.md
```

---

## Features

### Migration Engine (`/migration`, `/dblink`)

Supports two modes:

**Partitioned (via S3)** — for large tables
1. Extracts data from Oracle in daily partitions using parallel workers
2. Compresses each partition to `.csv.gz` and uploads to S3
3. Loads into Snowflake staging via `COPY INTO`
4. Applies changes to the target table via `MERGE`
5. Real-time progress streamed via WebSocket

**DB Link (direct)** — for smaller tables or when S3 is unavailable  
Migrates directly from Oracle to Snowflake without S3 staging.

Both modes persist job state to PostgreSQL — migrations are **resumable** after failure.

### DataStage Documentation (`/docs`)

Upload any DataStage `.dsx` or `.xml` export file and get:

- Structured preview in the UI (job list, stage breakdown, SQL preview)
- Full standalone HTML report with:
  - SQL syntax highlighting via Prism.js
  - Auto-indented SQL blocks
  - Transformer column derivations with DataStage → CASE WHEN conversion
  - Dependency map (SOURCE → JOB → TARGET)
  - Dark/light theme toggle synced with the app

Supports: OracleConnectorPX, CTransformerStage, CHashedFileStage, CSeqFileStage, CCustomStage, SnowflakeConnectorPX.

### Gap Analysis (`/gaps`)

Compares row counts between Oracle and Snowflake per date partition. Outputs:
- Total periods analyzed
- Periods with gap, total diff rows, max single-period gap
- Visual bar chart (Oracle vs Snowflake)
- Per-date table with diff and status

### Validation (`/validation`)

Three-level reconciliation:
- **Count check** — total row counts match
- **Schema check** — column names and types match
- **Sample check** — N random rows match field-by-field

### Audit Logs (`/logs`)

All events from all operations are stored in PostgreSQL and queryable via:
- Level filter (INFO / WARN / ERROR)
- Table name filter
- Free-text message search
- Date range

### Lineage Graph (`/lineage`)

Interactive D3.js force-directed graph from DataStage XML showing source tables, jobs, and target tables with drag, zoom, and click interactions.

---

## Environment Variables

Full reference — see `.env.example`:

```env
APP_ENV=development
APP_DEBUG=true
LOG_LEVEL=INFO

DB_HOST=localhost
DB_PORT=5432
DB_NAME=maf_logs
DB_USER=maf_user
DB_PASSWORD=changeme

ORACLE_HOST=oracle.internal.corp
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=dw_reader
ORACLE_PASSWORD=changeme
ORACLE_SCHEMA=DWADM

SNOWFLAKE_ACCOUNT=xy12345.snowflakecomputing.com
SNOWFLAKE_USER=dw_user@corp.com
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=WH_COMPUTE
SNOWFLAKE_DATABASE=DW_PROD
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_AUTHENTICATOR=externalbrowser

AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
S3_BUCKET=corp-dw-migration
S3_PREFIX=migration/
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | All connection statuses |
| `POST` | `/api/migration/start` | Start a migration job |
| `GET` | `/api/migration/jobs` | List all jobs |
| `GET` | `/api/migration/jobs/{id}` | Job progress |
| `POST` | `/api/migration/jobs/{id}/cancel` | Cancel job |
| `WS` | `/ws/progress/{job_id}` | Real-time progress stream |
| `POST` | `/api/datastage/analyze` | Parse XML → JSON |
| `POST` | `/api/datastage/report` | Parse XML → HTML report |
| `POST` | `/api/datastage/lineage` | Parse XML → lineage graph |
| `GET` | `/api/logs` | Paginated audit log query |
| `GET` | `/api/logs/stats` | Log statistics |

Full interactive docs at `http://localhost:8000/docs` (Swagger UI).

---

## Development

### Running tests

```bash
cd backend
pytest
pytest --cov=app tests/          # with coverage
```

### Linting

```bash
# Backend
ruff check .
ruff format .

# Frontend
npm run lint
```

### Adding a new migration route

1. Create the service logic in `backend/app/services/`
2. Add the route in `backend/app/api/routes/`
3. Register the router in `backend/app/main.py`
4. Add the page in `frontend/src/app/<route>/page.tsx`
5. Add the route to `NAV_ITEMS` in `Sidebar.tsx` and `ROUTE_LABELS` in `TopBar.tsx`

---

## Theme

The application ships with a **light theme by default** and a dark theme toggle in the top bar. Theme preference is persisted in `localStorage` and synced between the app and generated HTML reports.

Design system tokens are defined in `frontend/src/styles/globals.css` as CSS custom properties — both themes share the same component markup.

---

## Background

MAF was developed internally during the large-scale telecom DataStage 11.5 → Snowflake migration project. The DataStage XML parser (`xml_analyzer.py`) is a port of a standalone Python script built iteratively against real CDR, Interconnection and ODS pipeline exports.

The framework replaced a fragmented set of 43 standalone Python scripts with a single cohesive platform, estimated to save 78+ engineer-days of manual migration work.

---

## License

MIT — see [LICENSE](LICENSE) for details.