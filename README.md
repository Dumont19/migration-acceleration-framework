# MAF — Migration Acceleration Framework

> Oracle → Snowflake migration acceleration tool · DataStage 11.5+

![Stack](https://img.shields.io/badge/backend-FastAPI-009688?style=flat-square&logo=fastapi)
![Stack](https://img.shields.io/badge/frontend-Next.js%2014-000000?style=flat-square&logo=next.js)
![Stack](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python)
![Stack](https://img.shields.io/badge/license-MIT-green?style=flat-square)

---

## Overview

MAF is a full-stack internal tooling platform that accelerates and documents Oracle-to-Snowflake data warehouse migrations. It provides:

- **Partitioned migrations** — resumable state, parallel workers and real-time WebSocket progress
- **DB Link migrations** — direct Oracle → Snowflake without S3 staging
- **DataStage XML documentation** — parses `.dsx` export files, generates HTML reports with SQL highlighting, transformer derivations and dependency maps
- **Gap analysis** — volumetric row count comparison between Oracle and Snowflake per date partition
- **Validation** — schema, count and sample-level reconciliation between source and target
- **Lineage graph** — interactive SOURCE → JOB → TARGET visualization from DataStage XML
- **Dimension migration** — análise automática de DSX, geração dos 6 SQLs SCD2 e queries de homologação MINUS DEV vs PROD
- **Standalone tools** — extract Oracle metadata, create Snowflake tables, run COPY INTO and MERGE
- **Persistent audit trail** — all execution events stored in SQLite (local) or PostgreSQL (production)

---

## Stack

| Layer | Technology |
|---|---|
| Backend API | FastAPI 0.111+, Python 3.11+ |
| Frontend | Next.js 14 (App Router), React 18 |
| Audit database | SQLite (dev, zero config) · PostgreSQL (production) |
| Migrations | Alembic |
| Source DB | Oracle (oracledb thin mode — no Oracle Instant Client) |
| Target DW | Snowflake (snowflake-connector-python) |
| File staging | AWS S3 (boto3) |
| Real-time | WebSocket (Starlette) |
| XML parsing | BeautifulSoup4 + lxml |

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.11 or 3.12 |
| Node.js | 20+ |
| npm | 10+ (bundled with Node.js) |

Nothing else. No Docker, no PostgreSQL installation required for local development (SQLite is used by default).

> Oracle, Snowflake and S3 credentials are optional for local development.
> The app starts without them — connection errors appear only in the Settings page.

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/Dumont19/migration-acceleration-framework.git
cd migration-acceleration-framework
cp .env.example .env   # edit with your credentials if needed
```

### 2. Start the backend

```bash
cd backend
python -m venv venv

# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

API at **http://localhost:8000** · Docs at **http://localhost:8000/docs**

### 3. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Application at **http://localhost:3000**

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
│   │   │   │   ├── dimension.py     # POST /api/dimension/analyze, /generate, /homologate; GET /jobs
│   │   │   │   ├── tools.py         # GET /api/tools/metadata, POST /create-table, /copy-into, /merge
│   │   │   │   ├── logs.py          # GET /api/logs (paginated audit trail)
│   │   │   │   └── health.py        # GET /api/health (parallel connection check)
│   │   │   └── ws/
│   │   │       └── progress.py      # WebSocket: ws://localhost:8000/ws/progress/{job_id}
│   │   ├── core/
│   │   │   ├── config.py            # Pydantic Settings (all env vars)
│   │   │   ├── database.py          # Async SQLAlchemy — SQLite (dev) or PostgreSQL (prod)
│   │   │   ├── logging.py           # structlog + database sink
│   │   │   ├── oracle_client.py     # Oracle async connection pool
│   │   │   ├── snowflake_client.py  # Snowflake SQLAlchemy engine
│   │   │   └── s3_client.py         # boto3 async wrapper
│   │   ├── models/
│   │   │   ├── logs.py              # ORM: migration_jobs, job_logs, job_partitions,
│   │   │   │                        #       validation_runs, dimension_jobs
│   │   │   └── schemas.py           # Pydantic request/response schemas
│   │   └── services/
│   │       ├── migration/
│   │       │   ├── partitioned.py   # Oracle → CSV.GZ → S3 → Snowflake → MERGE
│   │       │   └── state.py         # Job state management
│   │       ├── validation/
│   │       │   └── comparator.py    # Oracle vs Snowflake: counts + schema + sample
│   │       ├── tools/
│   │       │   └── ddl_service.py   # oracle_to_snowflake_type, build_ddl, run_copy_into, run_merge
│   │       ├── datastage/
│   │       │   ├── xml_analyzer.py  # DataStage XML parser + HTML report
│   │       │   └── xml_parser.py    # Lineage graph builder
│   │       └── dimension/           # Módulo de migração de dimensões SCD2
│   │           ├── schemas.py       # DimensionSpec, ColumnSpec, LookupSpec
│   │           ├── extractor.py     # DimensionSpecExtractor (parse DSX)
│   │           ├── generator.py     # DimensionSqlGenerator (gera 6 SQLs)
│   │           └── homologator.py   # DimensionHomologator (MINUS DEV vs PROD)
│   ├── migrations/
│   │   └── versions/
│   │       ├── 0001_initial.py
│   │       └── 0002_dimension_jobs.py
│   └── pyproject.toml
│
├── frontend/
│   └── src/
│       ├── app/                     # Next.js App Router pages
│       │   ├── page.tsx             # /00  → Dashboard
│       │   ├── migration/page.tsx   # /01  → Migration (partitioned + S3)
│       │   ├── dblink/page.tsx      # /02  → DB Link migration
│       │   ├── gaps/page.tsx        # /03  → Gap analysis
│       │   ├── docs/page.tsx        # /04  → DataStage documentation
│       │   ├── lineage/page.tsx     # /05  → Lineage graph
│       │   ├── validation/page.tsx  # /06  → Oracle vs Snowflake validation
│       │   ├── tools/page.tsx       # /07  → Standalone tools
│       │   ├── logs/page.tsx        # /08  → Audit logs
│       │   ├── dimension/page.tsx   # /09  → Dimension migration (SCD2)
│       │   └── settings/page.tsx    # /10  → Connection configuration
│       ├── components/
│       │   ├── layout/Sidebar.tsx
│       │   └── ui/index.tsx         # StatCard, SectionLabel, ProgressBar, etc.
│       └── lib/
│           ├── api.ts               # Typed fetch client (per-domain: migrationApi, dimensionApi…)
│           └── useJobProgress.ts    # WebSocket hook with reconnect
│
├── .env.example
├── ARCHITECTURE.md
└── README.md
```

---

## Pages

### /00 Dashboard
System health overview — active jobs, failed jobs, total log entries. Live connection status for Oracle, Snowflake, S3 and the local database.

### /01 Migration (Partitioned)
Partitioned Oracle → Snowflake migration via S3 staging. Configure table name, date range, batch size, max workers and schemas. Real-time progress via WebSocket. State persisted — fully resumable.

### /02 DB Link
Direct Oracle → Snowflake migration without S3. Suitable for smaller tables.

### /03 Gap Analysis
Per-day row count comparison between Oracle and Snowflake — pinpoints exactly which partitions are out of sync.

### /04 Job Docs (DataStage)
Upload any DataStage `.dsx` or `.xml` export. Generates a structured JSON preview and a standalone HTML report with SQL syntax highlighting, transformer derivations as CASE WHEN, and a SOURCE → JOB → TARGET dependency map.

### /05 Lineage
Interactive force-directed graph from DataStage XML. Nodes color-coded: source (blue), job (green), target (yellow).

### /06 Validation
Three-level Oracle vs Snowflake reconciliation: count check, schema check and sample check (N random rows field-by-field). Results stored for historical comparison.

### /07 Tools
Standalone utilities:
- **extract_metadata** — column structure and comments from Oracle
- **create_snowflake_table** — DDL from Oracle structure, creates `{TABLE}` + `{TABLE}_RAW`
- **copy_into_snowflake** — loads CSV.GZ from S3 into Snowflake RAW
- **run_merge** — MERGE from RAW → Final, optionally filtered by partition date

### /08 Audit Logs
Persistent execution history. Filter by level, table name, free-text search and date range.

### /09 Dimension Migration (SCD2)
Full pipeline for migrating DataStage dimension jobs to Snowflake SCD2:

**Aba 01 — Upload:** Upload `.dsx`/`.xml`. Extracted automatically:
- Job name and target table
- `fl_mn` detected from column names (`IDT_RGT_ATU` → `1`, `RECORD_STATUS` → `0`)
- Full ODS source SELECT
- Column derivations DataStage → Snowflake SQL
- Surrogate key (SEQUENCE) and business key
- `CHashedFileStage` lookups → `LEFT JOIN … COALESCE(…, -1)`

**Aba 02 — Gerar SQL:** Generates 6 SQLs in order:
1. `CREATE OR REPLACE TRANSIENT TABLE … _RAW`
2. `CREATE OR REPLACE SEQUENCE SEQ_…`
3. `CREATE TABLE IF NOT EXISTS …` (DIM with SCD2 columns)
4. `MERGE INTO DWDEV.HUGOA.DW_VERSIONA` (job config)
5. `CREATE OR REPLACE PROCEDURE PRO_…` (TRUNCATE + INSERT + CALL PRO_DW_VERSIONA)
6. `CREATE OR REPLACE TASK TSK_…` + `ALTER TASK SUSPEND`

**Aba 03 — Homologação:** MINUS, COUNT by date, and field divergence queries for DEV vs PROD validation. Supports Time Travel (`AT OFFSET -N`).

**Aba 04 — Histórico:** Paginated list of all generated dimension jobs.

### /10 Settings
Live connection health check for all services with latency display.

---

## Environment Variables

```env
# App
APP_ENV=development
LOG_LEVEL=INFO

# Database (SQLite by default — set DB_PATH to change location)
DB_PATH=./maf.db

# Oracle
ORACLE_HOST=oracle.internal.corp
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=dw_reader
ORACLE_PASSWORD=changeme
ORACLE_SCHEMA=DWADM

# Snowflake
SNOWFLAKE_ACCOUNT=xy12345.snowflakecomputing.com
SNOWFLAKE_USER=dw_user@corp.com
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=WH_COMPUTE
SNOWFLAKE_DATABASE=DWDEV
SNOWFLAKE_SCHEMA=MATHEUSDR
SNOWFLAKE_AUTHENTICATOR=externalbrowser

# AWS S3
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
| `POST` | `/api/dimension/analyze` | Parse DSX → DimensionSpec JSON |
| `POST` | `/api/dimension/generate` | DimensionSpec → 6 SQLs SCD2 |
| `POST` | `/api/dimension/homologate` | DimensionSpec → MINUS queries DEV vs PROD |
| `GET` | `/api/dimension/jobs` | Histórico de jobs de dimensão |
| `GET` | `/api/tools/metadata` | Extract Oracle table structure |
| `POST` | `/api/tools/create-table` | Create table in Snowflake from Oracle DDL |
| `POST` | `/api/tools/copy-into` | COPY INTO Snowflake from S3 |
| `POST` | `/api/tools/merge` | MERGE RAW → Final |
| `GET` | `/api/logs` | Paginated audit log query |
| `GET` | `/api/logs/stats` | Log statistics |

Full interactive docs at `http://localhost:8000/docs`.

---

## Development

```bash
# Backend tests
cd backend
pytest
pytest --cov=app tests/

# Lint + format
ruff check .
ruff format .

# Frontend type check + lint
cd frontend
npm run type-check
npm run lint
```

---

## Theme

Light theme by default, dark theme via the toggle in the top bar. Preference persisted in `localStorage` and synced automatically with generated DataStage HTML reports.

---

## License

MIT — see [LICENSE](LICENSE) for details.
