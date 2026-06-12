# MAF — Architecture Document

> Migration Acceleration Framework · Oracle/DataStage 11.5 → Snowflake · Algar Telecom (CTBC)

---

## 1. Arquitetura Atual

### 1.1 Estrutura de Módulos

```
backend/
├── app/
│   ├── main.py                        # App factory, lifespan, middleware, routers
│   ├── core/
│   │   ├── config.py                  # Pydantic settings (Oracle, Snowflake, S3, DB, App)
│   │   ├── database.py                # SQLAlchemy async engine + session factory
│   │   ├── logging.py                 # structlog configurado + DatabaseLogSink
│   │   ├── oracle_client.py           # Pool oracledb assíncrono
│   │   ├── snowflake_client.py        # SQLAlchemy async engine para Snowflake
│   │   └── s3_client.py               # Wrapper boto3 com run_in_executor
│   ├── api/
│   │   ├── routes/
│   │   │   ├── health.py              # GET /api/health, /oracle, /snowflake, /s3, /database
│   │   │   ├── migration.py           # POST /api/migration/start, GET /jobs, WS /ws/progress
│   │   │   ├── logs.py                # GET /api/logs, /stats, /job/{id}
│   │   │   ├── datastage.py           # POST /api/datastage/analyze, /report, /lineage
│   │   │   └── tools.py              # GET /api/tools/metadata, POST /create-table, /copy-into, /merge
│   │   └── ws/
│   │       └── progress.py            # WebSocket manager (broadcast de progresso por job_id)
│   ├── models/
│   │   ├── logs.py                    # ORM: MigrationJob, JobLog, JobPartition, ValidationRun
│   │   └── schemas.py                 # Pydantic request/response schemas
│   └── services/
│       ├── datastage/
│       │   ├── xml_analyzer.py        # DataStagePrecisionMapper — parse XML + HTML report
│       │   └── xml_parser.py          # DataStageXMLParser — grafo de linhagem
│       ├── migration/
│       │   ├── partitioned.py         # PartitionedMigrationService — Oracle→S3→Snowflake
│       │   └── state.py               # JobStateService — lifecycle + partition tracking
│       └── validation/
│           └── comparator.py          # ValidationService — count/schema/sample comparison

frontend/
├── src/
│   ├── app/                           # Next.js 14 App Router (pages)
│   │   ├── page.tsx                   # Dashboard
│   │   ├── migration/page.tsx         # Formulário + progress em tempo real
│   │   ├── validation/page.tsx        # Formulário de validação
│   │   ├── logs/page.tsx              # Tabela paginada de logs
│   │   ├── datastage/page.tsx         # Upload XML + análise
│   │   ├── lineage/page.tsx           # Grafo de linhagem
│   │   ├── gaps/page.tsx              # Gap analysis
│   │   ├── tools/page.tsx             # Ferramentas pontuais
│   │   ├── settings/page.tsx          # Configurações
│   │   └── docs/page.tsx              # Documentação
│   ├── components/
│   │   ├── layout/Sidebar.tsx
│   │   ├── layout/TopBar.tsx
│   │   └── ui/index.tsx               # Componentes compartilhados (ProgressBar, StatusBadge, etc.)
│   ├── lib/
│   │   ├── api.ts                     # Clientes de API tipados por domínio
│   │   └── useJobProgress.ts          # Hook WebSocket de progresso
│   ├── context/ThemeContext.tsx
│   └── styles/globals.css
```

### 1.2 Padrões de Código Identificados

**Backend**
- Async-first: `oracledb.AsyncConnectionPool`, `sqlalchemy.ext.asyncio`, `asyncio.create_task`
- Structured logging: `structlog` com `DatabaseLogSink` que persiste INFO+ no PostgreSQL
- Injeção de dependência FastAPI: `Depends(get_db_session)`
- Pydantic v2 para validação de entrada e saída
- Lifespan para inicialização ordenada de conexões

**Frontend**
- Next.js 14 App Router, TypeScript estrito
- Clientes de API agrupados por domínio (`migrationApi`, `validationApi`, `datastageApi`)
- Hook customizado `useJobProgress` para WebSocket
- CSS Variables para temas claro/escuro

---

## 2. Diagnóstico

### 2.1 O que está bem

| Área | Ponto positivo |
|------|---------------|
| Logging | structlog estruturado + sink de DB — rastreabilidade completa |
| Conexões | Pools com health check, recycle, shutdown graceful |
| Progresso | WebSocket + polling REST para rastrear partições individualmente |
| Config | Pydantic Settings com prefixos de env — zero magic strings |
| DataStage parser | Lógica de parse robusta, suporte a CASE/WHEN, InterVar, PL/SQL |
| Tipagem frontend | Interfaces TypeScript espelhando schemas Python |

### 2.2 Problemas Identificados

#### BUG CRÍTICO — `database.py` referencia campos inexistentes

```python
# database.py linha 13–18
def _create_engine():
    settings = get_settings()
    return create_async_engine(
        settings.db.url,
        pool_size=settings.db.pool_size,       # ← NÃO EXISTE em DatabaseSettings
        max_overflow=settings.db.max_overflow,  # ← NÃO EXISTE em DatabaseSettings
        echo=settings.db.echo_sql,              # ← NÃO EXISTE em DatabaseSettings
        ...
    )
```
`DatabaseSettings` tem apenas `db_path`. O app crasha na inicialização.

#### BUG — `tools.router` não registrado no `main.py`

```python
# main.py — routers registrados
app.include_router(health.router)
app.include_router(migration.router)
app.include_router(logs.router)
app.include_router(datastage.router)
# tools.router FALTANDO — /api/tools/* retorna 404
```

#### BUG — `partitioned.py` usa `__import__` hacky

```python
# partitioned.py linha 195
settings_s3 = __import__(
    "app.core.config", fromlist=["get_settings"]
).get_settings().s3
```
Deveria simplesmente ser `from app.core.config import get_settings`.

#### BUG — `ValidationService._compare_counts` hardcoda schema

```python
# comparator.py linhas 95, 106
await cur.execute(f"SELECT COUNT(*) FROM DWADM.{table} {where}")
result = await conn.execute(text(f"SELECT COUNT(*) FROM DWADM.{table} {where}"))
```
O `request.table_name` não inclui schema, e os métodos `_get_oracle_columns` / `_get_snowflake_columns` também hardcodam `DWADM`.

#### BUG — Frontend chama rotas inexistentes

```typescript
// api.ts linhas 230, 278
request<ValidationResult>('/api/validation/run', ...)    // ← Rota não registrada
request<GapResult[]>('/api/analysis/gaps', ...)          // ← Rota não registrada
```

#### BUG — `migration.py:list_jobs` — contagem total errada

```python
# migration.py linha 93
total = len(jobs) + offset  # Approximate — conta errada; deveria ser SELECT COUNT(*)
```

#### Acoplamento — lógica de negócio em route handler

`tools.py` contém `_oracle_to_snowflake_type()` e `_build_ddl()` diretamente no arquivo de rota. O route handler `create_snowflake_table` chama outro route handler `extract_metadata` diretamente — acoplamento interno indevido.

#### Duplicação — credenciais AWS embutidas em SQL

`_copy_into_snowflake` em `partitioned.py` e `copy_into_snowflake` em `tools.py` ambos constroem a mesma string SQL com credenciais AWS inline — duplicação e risco de segurança.

#### Mismatch — ORM PostgreSQL + DB SQLite

`models/logs.py` usa `JSONB` e `UUID` do dialeto `postgresql`, mas `config.py` configura `sqlite+aiosqlite`. SQLite não suporta esses tipos nativos.

#### Frontend — lógica de API misturada com componentes

Páginas como `migration/page.tsx` fazem chamadas diretas via `migrationApi.*` no corpo do componente sem hooks isolados, dificultando teste e reuso.

---

## 3. Arquitetura Proposta

### 3.1 Estrutura Refatorada

```
backend/
├── app/
│   ├── main.py                        # Registra TODOS os routers explicitamente
│   ├── core/
│   │   ├── config.py                  # DatabaseSettings com pool_size, max_overflow, echo_sql
│   │   ├── database.py                # Engine factory com suporte SQLite/PostgreSQL
│   │   ├── logging.py                 # Sem mudança
│   │   ├── oracle_client.py           # Sem mudança
│   │   ├── snowflake_client.py        # Extrai build_copy_into_sql() para reuso
│   │   └── s3_client.py               # Sem mudança
│   ├── api/
│   │   └── routes/
│   │       ├── health.py
│   │       ├── migration.py
│   │       ├── logs.py
│   │       ├── datastage.py
│   │       ├── tools.py              # Delegates para ToolsService
│   │       └── dimension.py          # NOVO — /api/dimension/*
│   ├── models/
│   │   ├── logs.py                    # JSONB → JSON, UUID → String para compat SQLite/PG
│   │   └── schemas.py
│   └── services/
│       ├── datastage/
│       │   ├── xml_analyzer.py
│       │   └── xml_parser.py
│       ├── migration/
│       │   ├── partitioned.py         # Remove __import__ hack
│       │   └── state.py
│       ├── tools/
│       │   └── ddl_service.py         # NOVO — _oracle_to_snowflake_type, _build_ddl, copy_into_sql
│       ├── validation/
│       │   └── comparator.py          # Aceita schema como parâmetro
│       └── dimension/                 # NOVO — módulo completo
│           ├── __init__.py
│           ├── extractor.py           # DimensionSpecExtractor
│           ├── generator.py           # DimensionSqlGenerator
│           ├── homologator.py         # DimensionHomologator
│           └── schemas.py             # DimensionSpec, ColumnSpec, LookupSpec

frontend/
└── src/
    ├── app/
    │   └── dimension/
    │       └── page.tsx               # NOVO — Upload XML, preview spec, gerar SQL, homologação
    └── lib/
        ├── api.ts                     # Adiciona dimensionApi
        └── useDimension.ts            # NOVO — hook para estado da página dimension
```

### 3.2 Separação de Responsabilidades

| Camada | Responsabilidade |
|--------|-----------------|
| `core/` | Infraestrutura: conexões, logging, config. **Zero lógica de negócio.** |
| `services/` | Lógica de negócio pura. **Zero imports de FastAPI.** Recebe e retorna tipos Python/Pydantic. |
| `api/routes/` | Tradução HTTP→Service. Validação de entrada, serialização de saída, HTTPException. |
| `models/schemas.py` | Contratos de API (request/response). Não mistura com ORM. |
| `models/logs.py` | Modelos ORM. Não conhece schemas de API. |

### 3.3 Convenções de Código

- Type hints completos em todo Python — sem `Any` nu, sem `dict` sem tipagem
- `logger = get_logger(__name__)` no topo de cada módulo
- `HTTPException` com `status_code` e `detail` descritivo — nunca `str(exc)` nu
- Services recebem clientes via construtor (testabilidade), nunca chamam `get_oracle_pool()` internamente
- Nomes de tabelas sempre `.upper()` antes de queries
- SQL com parâmetros bind (`:param`) — nunca f-string com valores de usuário
- Frontend: lógica de API em hooks (`use*.ts`), componentes apenas apresentam estado

---

## 4. Módulo de Dimensões (novo)

### 4.1 Contexto de Negócio

Jobs de dimensão no DataStage seguem o padrão SCD2 via `PRO_DW_VERSIONA` (em `DWDEV.HUGOA`). Dois padrões de nomenclatura coexistem controlados por `fl_mn`:

| fl_mn | Coluna de atividade | Data início vigência | Sistema origem | Data carga |
|-------|--------------------|--------------------|---------------|------------|
| `'0'` | `RECORD_STATUS` | `START_DATE` | `SRC_SYS_NAME` | `D_TIMESTAMP` |
| `'1'` | `IDT_RGT_ATU` | `DAT_INI_VIG_RGT` | `NOM_SIS_ORI` | `DAT_CAR_RGT` |

### 4.2 Pipeline de Geração

```
DSX file upload
      ↓
DimensionSpecExtractor
  · job_name, target_table
  · fl_mn (0 ou 1)
  · SELECT ODS source
  · colunas + derivações
  · surrogate_key, business_key
  · lookups → LEFT JOIN + COALESCE
  · after_sql → nome do job versiona
  · has_row_number
      ↓
DimensionSpec (Pydantic)
      ↓
DimensionSqlGenerator
  · SQL 1: CREATE TRANSIENT TABLE _RAW
  · SQL 2: CREATE SEQUENCE SEQ_
  · SQL 3: CREATE TABLE IF NOT EXISTS (DIM final)
  · SQL 4: INSERT INTO DW_VERSIONA (config)
  · SQL 5: CREATE PROCEDURE PRO_ (TRUNCATE+INSERT+CALL PRO_DW_VERSIONA)
  · SQL 6: CREATE TASK TSK_ + ALTER TASK SUSPEND
      ↓
DimensionHomologator
  · MINUS DEV vs PROD
  · COUNT por partição
  · Query de divergência de campos
  · Time Travel AT OFFSET -N
```

### 4.3 Regras críticas de geração SQL

- Comparações de NULLs: `IS DISTINCT FROM` (nunca `!=`)
- Colunas NUMBER: sem `TRY_CAST` (remover o cast direto)
- `NOM_SIS_ORI` padrão jobs SOM: `'ALGAR SOM'`
- Delta load obrigatório para tabelas > 100k registros
- Lookups `CHashedFileStage` → `LEFT JOIN ... ON ... COALESCE(..., -1)`
- Schema de trabalho: `DWDEV.MATHEUSDR`
- Config table: `DWDEV.HUGOA.DW_VERSIONA`
