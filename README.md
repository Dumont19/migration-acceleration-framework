# MAF — Migration Acceleration Framework

![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?style=flat-square&logo=python)
![Node.js](https://img.shields.io/badge/node.js-20.19%2B-339933?style=flat-square&logo=nodedotjs)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111%2B-009688?style=flat-square&logo=fastapi)
![Next.js](https://img.shields.io/badge/Next.js-16-000000?style=flat-square&logo=next.js)
![SQLite](https://img.shields.io/badge/SQLite-dev-003B57?style=flat-square&logo=sqlite)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)

---

## O que é o MAF

MAF é uma plataforma interna de aceleração de migração Oracle → Snowflake desenvolvida para projetos de modernização de data warehouse baseados em DataStage 11.5. Ela automatiza as etapas que consomem mais tempo de um engenheiro: extração de metadados Oracle, geração de DDL Snowflake, carga particionada via S3, análise de gaps e homologação pós-carga.

O módulo central — **Dimension Migration** — lê um arquivo `.dsx` exportado do DataStage e gera automaticamente os 6 SQLs necessários para implantar uma dimensão SCD2 no Snowflake: tabela RAW, SEQUENCE, tabela DIM, configuração do `DW_VERSIONA`, PROCEDURE de carga e TASK agendada. Também gera as queries de homologação MINUS DEV vs PROD.

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|---|---|
| Python | 3.11 |
| Node.js | 20.19.0 |
| Git | qualquer |

Nenhuma outra dependência. Sem Docker, sem PostgreSQL local (SQLite é usado por padrão em desenvolvimento). Credenciais Oracle, Snowflake e S3 são opcionais para iniciar — erros de conexão aparecem apenas na página Settings.

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

API em **http://localhost:8000** · Docs interativos em **http://localhost:8000/docs**

```bash
# Terminal 2 — Frontend
cd frontend
npm install
npm run dev
```

Aplicação em **http://localhost:3000**

---

## Páginas disponíveis

| Rota | Descrição |
|---|---|
| `/` | Dashboard — jobs ativos, logs recentes, status de conexões |
| `/migration` | Migração particionada Oracle → S3 → Snowflake com WebSocket |
| `/dblink` | Migração direta Oracle → Snowflake (sem S3) |
| `/gaps` | Gap analysis — comparação de contagens por data de partição |
| `/docs` | Documentação DataStage — parse de `.dsx`, HTML com SQL destacado |
| `/lineage` | Grafo de linhagem SOURCE → JOB → TARGET |
| `/validation` | Validação Oracle vs Snowflake: contagem, schema e amostra |
| `/tools` | Ferramentas standalone: DDL, COPY INTO, MERGE |
| `/logs` | Trilha de auditoria paginada com filtros |
| `/dimension` | Migração de dimensões SCD2 — upload DSX, gerar 6 SQLs, homologar |
| `/settings` | Verificação de conexões com latência |

---

## Variáveis de ambiente

Copie `.env.example` para `.env` e preencha as credenciais necessárias.

| Variável | Descrição | Obrigatório |
|---|---|---|
| `DB_PATH` | Caminho do arquivo SQLite | Não (default: `./maf.db`) |
| `ORACLE_HOST` | Hostname do Oracle | Não |
| `ORACLE_PORT` | Porta do listener Oracle | Não (default: 1521) |
| `ORACLE_SERVICE` | Service name Oracle | Não |
| `ORACLE_USER` | Usuário Oracle | Não |
| `ORACLE_PASSWORD` | Senha Oracle | Não |
| `ORACLE_SCHEMA` | Schema fonte padrão | Não (default: DWADM) |
| `SNOWFLAKE_ACCOUNT` | Account identifier Snowflake | Não |
| `SNOWFLAKE_USER` | Usuário Snowflake (e-mail SSO) | Não |
| `SNOWFLAKE_ROLE` | Role Snowflake | Não (default: SYSADMIN) |
| `SNOWFLAKE_WAREHOUSE` | Warehouse Snowflake | Não (default: WH_COMPUTE) |
| `SNOWFLAKE_DATABASE` | Database alvo | Não (default: DWDEV) |
| `SNOWFLAKE_SCHEMA` | Schema alvo | Não (default: DWADM) |
| `SNOWFLAKE_AUTHENTICATOR` | Método de autenticação | Não (default: externalbrowser) |
| `AWS_ACCESS_KEY_ID` | Chave de acesso S3 | Não |
| `AWS_SECRET_ACCESS_KEY` | Chave secreta S3 | Não |
| `AWS_REGION` | Região AWS | Não (default: us-east-1) |
| `S3_BUCKET` | Bucket de staging | Não |
| `S3_PREFIX` | Prefixo de chave S3 | Não (default: migration/) |

---

## Módulo de Dimensões — como usar

### 1. Upload

Na página `/dimension`, aba **01_upload**: faça upload de um arquivo `.dsx` ou `.xml` exportado do DataStage.

O sistema detecta automaticamente:
- Nome do job e tabela alvo
- `fl_mn` (1 = Marcia: `IDT_RGT_ATU`, 0 = legado: `RECORD_STATUS`)
- SELECT completo da ODS Oracle
- Colunas com derivações do transformer
- Surrogate key (via `SEQ_*.NEXTVAL`) e business key (via `BSK_*`)
- Lookups `CHashedFileStage` → `LEFT JOIN … COALESCE(…, -1)`
- Nome do job passado ao `PRO_DW_VERSIONA` no AfterSQL

### 2. Gerar SQL

Aba **02_generate**: revise o spec detectado e clique em **gerar_sql**.

Gera 6 SQLs em ordem:

| # | SQL gerado |
|---|---|
| 01 | `CREATE OR REPLACE TRANSIENT TABLE … _RAW` |
| 02 | `CREATE OR REPLACE SEQUENCE SEQ_…` |
| 03 | `CREATE TABLE IF NOT EXISTS …` (DIM com colunas SCD2) |
| 04 | `MERGE INTO DWDEV.HUGOA.DW_VERSIONA` (configuração do job) |
| 05 | `CREATE OR REPLACE PROCEDURE PRO_…` (TRUNCATE + INSERT + CALL) |
| 06 | `CREATE OR REPLACE TASK TSK_…` + `ALTER TASK SUSPEND` |

### 3. Homologação

Aba **03_homologate**: informe a tabela PROD (`SCHEMA.TABELA`) e opcional offset de Time Travel (ex: `-3600` = 1h atrás).

Gera:
- **MINUS** DEV vs PROD — linhas em DEV não presentes em PROD
- **Contagem por data** — volumes DEV vs PROD por data de carga
- **Divergência de campos** — comparação campo a campo usando `IS DISTINCT FROM`

### 4. Histórico

Aba **04_history**: lista todos os jobs de dimensão gerados com fl_mn, schema e data.

---

## Licença

MIT — veja [LICENSE](LICENSE) para detalhes.
