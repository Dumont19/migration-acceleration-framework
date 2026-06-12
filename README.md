# MAF — Migration Acceleration Framework

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
- **DB Link migrations** — direct Oracle → Snowflake without S3 staging
- **DataStage XML documentation** — parses `.dsx` export files and generates HTML reports with SQL highlighting, transformer derivations and dependency maps
- **Gap analysis** — volumetric row count comparison between Oracle and Snowflake per date partition
- **Validation** — schema, count and sample-level reconciliation between source and target
- **Lineage graph** — interactive SOURCE → JOB → TARGET visualization from DataStage XML
- **Standalone tools** — extract Oracle metadata, create Snowflake tables, run COPY INTO and MERGE independently
- **Persistent audit trail** — all execution events stored in PostgreSQL, queryable via the UI
- **Dimension migration** — análise automática de DSX DataStage, geração dos 6 SQLs SCD2 e queries de homologação MINUS DEV vs PROD

---

## Stack

| Layer | Technology |
|---|---|
| Backend API | FastAPI 0.111+, Python 3.11+ |
| Frontend | Next.js 14 (App Router), React 18 |
| Audit database | PostgreSQL 15+ (asyncpg + SQLAlchemy 2.0) |
| Migrations | Alembic |
| Source DB | Oracle (oracledb thin mode) |
| Target DW | Snowflake (snowflake-connector-python) |
| File staging | AWS S3 (boto3) |
| Real-time | WebSocket (Starlette) |
| XML parsing | BeautifulSoup4 + lxml |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11 or 3.12 | 3.13 works, avoid 3.14 |
| Node.js | 20+ | |
| PostgreSQL | 15+ | Local or remote |
| Git | any | |

> Oracle, Snowflake and S3 credentials are optional for local development.
> The app starts without them and shows connection errors only in the Settings page.
>
> **Note on PostgreSQL:** This is the only dependency that requires a separate installation.
> Download the installer at [postgresql.org/download](https://www.postgresql.org/download/) —
> it includes pgAdmin 4 (a GUI client) for those who prefer not to use the terminal.
> On Windows, the installer sets up everything automatically including the service.

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

Open `.env` and set your credentials. Only PostgreSQL is required to start:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=maf_logs
DB_USER=maf_user
DB_PASSWORD=changeme
```

### 3. Create the PostgreSQL database

```bash
psql -U postgres -c "CREATE DATABASE maf_logs;"
psql -U postgres -c "CREATE USER maf_user WITH PASSWORD 'changeme';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE maf_logs TO maf_user;"
psql -U postgres -c "ALTER DATABASE maf_logs OWNER TO maf_user;"
```

> **Windows:** if `psql` is not recognized, add `C:\Program Files\PostgreSQL\<version>\bin` to PATH,
> or use pgAdmin 4 to create the database via the GUI.

### 4. Start the backend

```bash
cd backend

# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate

pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

API available at **http://localhost:8000**  
Interactive docs at **http://localhost:8000/docs**

### 5. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

Application available at **http://localhost:3000**

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
│   │   │   │   ├── tools.py         # GET /api/tools/metadata, POST /create-table, /copy-into, /merge
│   │   │   │   ├── dimension.py     # POST /api/dimension/analyze, /generate, /homologate; GET /jobs
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
│   │       │   └── state.py         # Job state management
│   │       ├── validation/
│   │       │   └── comparator.py    # Oracle vs Snowflake: counts + schema + sample
│   │       ├── tools/
│   │       │   └── ddl_service.py   # oracle_to_snowflake_type, build_ddl, run_copy_into, run_merge
│   │       ├── datastage/
│   │       │   ├── xml_analyzer.py  # DataStage XML parser
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

## Pages

### /00 Dashboard
System health overview — active jobs, failed jobs, total log entries, error events. Live connection status for Oracle, Snowflake, S3 and PostgreSQL.

### /01 Migration (Partitioned)
Partitioned Oracle → Snowflake migration via S3 staging. Configure table name, date range, batch size, max workers and schemas. Real-time progress via WebSocket. State persisted to PostgreSQL — fully resumable after interruption.

### /02 DB Link
Direct Oracle → Snowflake migration without S3. Uses a database link to transfer data in a single operation. Suitable for smaller tables or when S3 is unavailable.

### /03 Gap Analysis
Volumetric comparison between Oracle and Snowflake per date partition. Outputs a per-day table with Oracle count, Snowflake count, absolute diff and percentage — pinpoints exactly which partitions are out of sync.

### /04 Job Docs (DataStage)
Upload any DataStage `.dsx` or `.xml` export file. Generates a structured preview in the UI and a full standalone HTML report with SQL syntax highlighting (Prism.js), auto-indented queries, transformer column derivations reconstructed as CASE WHEN, and a SOURCE → JOB → TARGET dependency map. Report theme syncs with the app toggle.

Supported stage types: `OracleConnectorPX`, `CTransformerStage`, `CHashedFileStage`, `CSeqFileStage`, `CCustomStage`, `SnowflakeConnectorPX`.

### /05 Lineage
Interactive force-directed graph built from DataStage XML. Nodes are color-coded: blue for source tables, green for jobs, yellow for targets. Drag, zoom and click to inspect.

### /06 Validation
Three-level Oracle vs Snowflake reconciliation: count check, schema check (column names and types), and sample check (N random rows field-by-field). Results stored in `validation_runs` table for historical comparison.

### /07 Tools
Standalone operational utilities, independent of the migration pipeline:
- **extract_metadata** — column structure and comments from Oracle
- **create_snowflake_table** — generates DDL from Oracle structure and creates `{TABLE}` + `{TABLE}_RAW` in Snowflake
- **copy_into_snowflake** — loads a CSV.GZ file from S3 into Snowflake RAW via COPY INTO
- **run_merge** — executes MERGE from RAW → Final, optionally filtered by partition date

### /08 Audit Logs
Persistent execution history stored in PostgreSQL. Filter by level (INFO / WARN / ERROR), table name, free-text search and date range.

### /09 Dimension Migration (SCD2)
Módulo completo para migração de tabelas de dimensão DataStage → Snowflake:

**Aba 01 — Upload:** Upload de arquivo `.dsx`/`.xml` exportado do DataStage. O sistema extrai automaticamente:
- Nome do job e tabela alvo
- `fl_mn` detectado via presença de `IDT_RGT_ATU` (Marcia) ou `RECORD_STATUS` (legacy)
- SELECT completo da ODS source
- Colunas com derivações DataStage → SQL
- Surrogate key (SEQUENCE) e business key
- Lookups `CHashedFileStage` convertidos para `LEFT JOIN ... COALESCE(..., -1)`

**Aba 02 — Gerar SQL:** Gera os 6 SQLs na ordem correta:
1. `CREATE OR REPLACE TRANSIENT TABLE ... _RAW`
2. `CREATE OR REPLACE SEQUENCE SEQ_...`
3. `CREATE TABLE IF NOT EXISTS ...` (tabela DIM final com colunas SCD2)
4. `MERGE INTO DWDEV.HUGOA.DW_VERSIONA` (config do job)
5. `CREATE OR REPLACE PROCEDURE PRO_...` (TRUNCATE + INSERT + CALL PRO_DW_VERSIONA)
6. `CREATE OR REPLACE TASK TSK_...` + `ALTER TASK SUSPEND`

Cada SQL tem botão de cópia individual.

**Aba 03 — Homologação:** Gera queries MINUS, COUNT por data e divergência de campos para validar DEV vs PROD. Suporte a Time Travel (`AT OFFSET -N`).

**Aba 04 — Histórico:** Lista todos os jobs de dimensão gerados com paginação.

### /10 Settings
Live connection health check for all four services (Oracle, Snowflake, S3, PostgreSQL) with latency display. Configuration is managed via the `.env` file.

---

## Environment Variables

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
| `GET` | `/api/tools/metadata` | Extract Oracle table structure |
| `POST` | `/api/tools/create-table` | Create table in Snowflake from Oracle DDL |
| `POST` | `/api/tools/copy-into` | COPY INTO Snowflake from S3 |
| `POST` | `/api/tools/merge` | MERGE RAW → Final |
| `GET` | `/api/logs` | Paginated audit log query |
| `GET` | `/api/logs/stats` | Log statistics |
| `POST` | `/api/dimension/analyze` | Parse DSX → DimensionSpec JSON |
| `POST` | `/api/dimension/generate` | DimensionSpec → 6 SQLs SCD2 |
| `POST` | `/api/dimension/homologate` | DimensionSpec → MINUS queries DEV vs PROD |
| `GET` | `/api/dimension/jobs` | Histórico de jobs de dimensão |

Full interactive docs at `http://localhost:8000/docs`.

---

## Development

```bash
# Backend tests
cd backend
pytest
pytest --cov=app tests/

# Lint
ruff check .
ruff format .

# Frontend lint
cd frontend
npm run lint
```

---

## Theme

Light theme by default, dark theme via the toggle in the top bar. Preference persisted in `localStorage` and synced automatically with generated DataStage HTML reports — opening a report inherits the current app theme.

---

## License

MIT — see [LICENSE](LICENSE) for details.