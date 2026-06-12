# MAF — Migration Acceleration Framework

![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python)
![Node.js](https://img.shields.io/badge/node.js-20.19%2B-339933?style=flat-square&logo=nodedotjs)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111%2B-009688?style=flat-square&logo=fastapi)
![Next.js](https://img.shields.io/badge/Next.js-16-000000?style=flat-square&logo=next.js)
![SQLite](https://img.shields.io/badge/SQLite-dev-003B57?style=flat-square&logo=sqlite)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)

---

## What is MAF

MAF is an internal Oracle → Snowflake migration acceleration platform built for data warehouse modernization projects based on DataStage 11.5. It automates the most time-consuming steps for a migration engineer: Oracle metadata extraction, Snowflake DDL generation, partitioned loads via S3, gap analysis, and post-load homologation.

The core module — **Dimension Migration** — reads a `.dsx` file exported from DataStage and automatically generates all 6 SQLs needed to deploy an SCD2 dimension in Snowflake: RAW table, SEQUENCE, DIM table, `DW_VERSIONA` config, load PROCEDURE, and scheduled TASK. It also generates MINUS DEV vs PROD homologation queries.

---

## Prerequisites

| Tool | Minimum version |
|---|---|
| Python | 3.11 |
| Node.js | 20.19.0 |
| Git | any |

No other dependencies. No Docker, no local PostgreSQL (SQLite is used by default in development). Oracle, Snowflake, and S3 credentials are optional to get started — connection errors only appear on the Settings page.

---

## Quick Start

```bash
# Terminal 1 — Backend
cd backend
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS / Linux
pip install -e ".[dev]"
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

API at **http://localhost:8000** · Interactive docs at **http://localhost:8000/docs**

```bash
# Terminal 2 — Frontend
cd frontend
npm install
npm run dev
```

App at **http://localhost:3000**

---

## Available Pages

| Route | Description |
|---|---|
| `/` | Dashboard — active jobs, recent logs, connection status |
| `/migration` | Partitioned Oracle → S3 → Snowflake migration with WebSocket progress |
| `/dblink` | Direct Oracle → Snowflake migration (no S3) |
| `/gaps` | Gap analysis — row count comparison by date partition |
| `/docs` | DataStage documentation — `.dsx` parsing, HTML report with SQL highlighting |
| `/lineage` | Lineage graph SOURCE → JOB → TARGET |
| `/validation` | Oracle vs Snowflake validation: count, schema, and sample |
| `/tools` | Standalone utilities: DDL, COPY INTO, MERGE |
| `/logs` | Paginated audit trail with filters |
| `/dimension` | SCD2 dimension migration — upload DSX, generate 6 SQLs, homologate |
| `/settings` | Live connection health check with latency |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in the required credentials.

| Variable | Description | Required |
|---|---|---|
| `DB_PATH` | SQLite file path | No (default: `./maf.db`) |
| `ORACLE_HOST` | Oracle hostname | No |
| `ORACLE_PORT` | Oracle listener port | No (default: 1521) |
| `ORACLE_SERVICE` | Oracle service name | No |
| `ORACLE_USER` | Oracle username | No |
| `ORACLE_PASSWORD` | Oracle password | No |
| `ORACLE_SCHEMA` | Default source schema | No (default: DWADM) |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | No |
| `SNOWFLAKE_USER` | Snowflake user (SSO email) | No |
| `SNOWFLAKE_ROLE` | Snowflake role | No (default: SYSADMIN) |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse | No (default: WH_COMPUTE) |
| `SNOWFLAKE_DATABASE` | Target database | No (default: DWDEV) |
| `SNOWFLAKE_SCHEMA` | Target schema | No (default: DWADM) |
| `SNOWFLAKE_AUTHENTICATOR` | Auth method | No (default: externalbrowser) |
| `AWS_ACCESS_KEY_ID` | S3 access key | No |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | No |
| `AWS_REGION` | AWS region | No (default: us-east-1) |
| `S3_BUCKET` | Staging bucket | No |
| `S3_PREFIX` | S3 key prefix | No (default: migration/) |

---

## Dimension Module — How to Use

### 1. Upload

On the `/dimension` page, tab **01_upload**: upload a `.dsx` or `.xml` file exported from DataStage.

The system automatically detects:
- Job name and target table
- `fl_mn` (1 = Marcia: `IDT_RGT_ATU`, 0 = legacy: `RECORD_STATUS`)
- Full ODS Oracle SELECT
- Column derivations from the transformer
- Surrogate key (via `SEQ_*.NEXTVAL`) and business key (via `BSK_*`)
- `CHashedFileStage` lookups → `LEFT JOIN … COALESCE(…, -1)`
- Job name passed to `PRO_DW_VERSIONA` in AfterSQL

### 2. Generate SQL

Tab **02_generate**: review the detected spec and click **gerar_sql**.

Generates 6 SQLs in order:

| # | Generated SQL |
|---|---|
| 01 | `CREATE OR REPLACE TRANSIENT TABLE … _RAW` |
| 02 | `CREATE OR REPLACE SEQUENCE SEQ_…` |
| 03 | `CREATE TABLE IF NOT EXISTS …` (DIM with SCD2 columns) |
| 04 | `MERGE INTO DWDEV.HUGOA.DW_VERSIONA` (job configuration) |
| 05 | `CREATE OR REPLACE PROCEDURE PRO_…` (TRUNCATE + INSERT + CALL) |
| 06 | `CREATE OR REPLACE TASK TSK_…` + `ALTER TASK SUSPEND` |

### 3. Homologation

Tab **03_homologate**: provide the PROD table (`SCHEMA.TABLE`) and optional Time Travel offset (e.g. `-3600` = 1 hour ago).

Generates:
- **MINUS** DEV vs PROD — rows in DEV not present in PROD
- **Count by date** — DEV vs PROD volumes by load date
- **Field divergence** — field-by-field comparison using `IS DISTINCT FROM`

### 4. History

Tab **04_history**: lists all generated dimension jobs with fl_mn, schema, and date.

---

## License

MIT — see [LICENSE](LICENSE) for details.
