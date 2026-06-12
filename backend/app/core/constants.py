"""
app/core/constants.py
---------------------
Constantes de domínio compartilhadas entre todos os módulos do MAF.
Sem imports de terceiros — apenas Python stdlib.
"""

# ── SCD2 — mapeamentos de papéis → nomes de colunas ──────────────────────────

#: Colunas SCD2 no padrão fl_mn=1 (Marcia/Hugoa)
SCD_FL_MN1_MAP: dict[str, str] = {
    "active_flag": "IDT_RGT_ATU",
    "start_date":  "DAT_INI_VIG_RGT",
    "sys_name":    "NOM_SIS_ORI",
    "load_date":   "DAT_CAR_RGT",
}

#: Colunas SCD2 no padrão fl_mn=0 (legado)
SCD_FL_MN0_MAP: dict[str, str] = {
    "active_flag": "RECORD_STATUS",
    "start_date":  "START_DATE",
    "sys_name":    "SRC_SYS_NAME",
    "load_date":   "D_TIMESTAMP",
}

#: Conjunto de colunas fl_mn=1 — usado para detecção automática no DSX
SCD_FL_MN1_COLS: frozenset[str] = frozenset(SCD_FL_MN1_MAP.values())

#: Conjunto de colunas fl_mn=0 — usado para detecção automática no DSX
SCD_FL_MN0_COLS: frozenset[str] = frozenset(SCD_FL_MN0_MAP.values())

# ── Snowflake — schemas e objetos de controle ─────────────────────────────────

#: Schema padrão para tabelas de dimensão geradas
DEFAULT_DIM_SCHEMA: str = "DWDEV.MATHEUSDR"

#: Schema padrão da ODS Oracle / Snowflake de origem
DEFAULT_SRC_SCHEMA: str = "DWDEV"

#: Tabela de controle SCD2 (DW_VERSIONA)
DW_VERSIONA_TABLE: str = "DWDEV.HUGOA.DW_VERSIONA"

#: Procedure de versionamento SCD2
PRO_DW_VERSIONA: str = "DWDEV.HUGOA.PRO_DW_VERSIONA"

# ── Chaves padrão quando não detectadas no DSX ────────────────────────────────

#: Nome padrão da surrogate key quando não encontrado via SEQUENCE no DSX
DEFAULT_SURROGATE_KEY: str = "TABLE_KEY"

#: Nome padrão da business key quando não encontrado via BSK_ no DSX
DEFAULT_BUSINESS_KEY: str = "TABLE_SRC_KEY"

# ── Valores de negócio ────────────────────────────────────────────────────────

#: Sistema de origem padrão para jobs SOM
DEFAULT_NOM_SIS_ORI: str = "ALGAR SOM"

# ── Snowflake Task ────────────────────────────────────────────────────────────

#: Warehouse padrão para execução de Tasks
TASK_WAREHOUSE: str = "WH_COMPUTE"

#: Expressão CRON padrão (segunda a sexta, 06h, fuso Brasília)
TASK_SCHEDULE_CRON: str = "USING CRON 0 6 * * * America/Sao_Paulo"

# ── Queries de homologação ────────────────────────────────────────────────────

#: Limite de linhas no relatório de contagem por data
HOMO_COUNT_LIMIT: int = 90

#: Limite de linhas no relatório de divergência de campos
HOMO_DIVERGENCE_LIMIT: int = 500

#: Número de colunas de negócio amostradas na query de divergência
HOMO_DIVERGENCE_COL_SAMPLE: int = 5

# ── Mapeamento de tipos Oracle → Snowflake ────────────────────────────────────

#: Comprimento padrão de VARCHAR quando não especificado na fonte Oracle
VARCHAR_DEFAULT_LENGTH: int = 255

#: Comprimento máximo de VARCHAR para tipos sem mapeamento direto
VARCHAR_MAX_LENGTH: int = 4000

# ── DataStage — heurísticas de parsing ───────────────────────────────────────

#: Prefixos de link DataStage que devem ser ignorados no transformer
IGNORED_LINK_PREFIXES: tuple[str, ...] = (
    "rowrej", "nullset", "intervar", "stagevar"
)
