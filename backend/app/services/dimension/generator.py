"""
services/dimension/generator.py
---------------------------------
DimensionSqlGenerator — gera os 5 SQLs para migração de dimensão SCD2.

REGRA ABSOLUTA: colunas para DDL vêm do Oracle (spec.oracle_columns).
Se oracle_columns estiver vazio, o DDL usa derivações DataStage com aviso.

Ordem de geração (sem TASK):
  01. CREATE OR REPLACE TRANSIENT TABLE {SCHEMA}.{TABLE}_RAW      — colunas Oracle
  02. CREATE OR REPLACE TRANSIENT TABLE {SCHEMA}.{TABLE}          — Time Travel PROD
  03. CREATE OR REPLACE SEQUENCE {SCHEMA}.SEQ_{TABLE}
  04. INSERT INTO DW_VERSIONA ... WHERE NOT EXISTS
  05. CREATE OR REPLACE PROCEDURE {SCHEMA}.PRO_{TABLE}(WH VARCHAR)
"""
from __future__ import annotations

import re

from app.core.constants import (
    DW_VERSIONA_TABLE,
    PRO_DW_VERSIONA,
    SCD2_ALL_CONTROL_COLS,
    VARCHAR_MAX_LENGTH,
)
from .schemas import ColumnSpec, DimensionSpec, OracleColumnInfo


# ── Oracle → Snowflake type mapping ──────────────────────────────────────────

def _oracle_to_sf_type(col: OracleColumnInfo) -> str | None:
    """Maps an Oracle data type to Snowflake DDL type string.

    Returns None for BLOB columns (omitted from Snowflake DDL).

    Args:
        col: Oracle column metadata.

    Returns:
        Snowflake type string, or None if the column should be omitted.
    """
    raw = col.data_type.upper()
    t = raw.split("(")[0].strip()

    if t == "BLOB":
        return None
    if t in ("VARCHAR2", "NVARCHAR2"):
        return f"VARCHAR({col.data_length or VARCHAR_MAX_LENGTH})"
    if t in ("CHAR", "NCHAR"):
        return f"CHAR({col.data_length or 1})"
    if t == "NUMBER":
        if col.data_precision is not None and col.data_scale is not None:
            return f"NUMBER({col.data_precision},{col.data_scale})"
        if col.data_precision is not None:
            return f"NUMBER({col.data_precision},0)"
        return "NUMBER"
    if t == "DATE":
        return "DATE"
    if t.startswith("TIMESTAMP"):
        scale = col.data_scale if col.data_scale is not None else 6
        if "WITH TIME ZONE" in raw and "LOCAL" not in raw:
            return f"TIMESTAMP_TZ({scale})"
        if "WITH LOCAL TIME ZONE" in raw:
            return f"TIMESTAMP_LTZ({scale})"
        return f"TIMESTAMP_NTZ({scale})"
    if t in ("CLOB", "NCLOB", "XMLTYPE", "LONG"):
        return "VARCHAR(16777216)"
    if t in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE", "REAL"):
        return "FLOAT"
    if t in ("INTEGER", "INT", "SMALLINT"):
        return "NUMBER(38,0)"
    if t == "RAW":
        return f"VARCHAR({(col.data_length or 1) * 2})"
    return f"VARCHAR({VARCHAR_MAX_LENGTH})"


def _infer_sf_type_from_ds_col(col: ColumnSpec) -> str:
    """Infers a Snowflake type from a DataStage ColumnSpec (fallback only).

    Used when Oracle is not available and spec.oracle_columns is empty.
    """
    name_up = col.name.upper()
    deriv_up = col.derivation.upper()
    if any(k in name_up for k in ("_DAT", "DAT_", "_DATE", "DATE_", "DT_", "_DT")):
        return "TIMESTAMP_NTZ"
    if any(k in name_up for k in ("_ID", "IDT_", "_QTD", "_VLR", "_NRO", "NRO_", "_NUM")):
        return "NUMBER(38)"
    if "CURRENT_TIMESTAMP" in deriv_up or "SYSDATE" in deriv_up:
        return "TIMESTAMP_NTZ"
    return f"VARCHAR({VARCHAR_MAX_LENGTH})"


# ── DataStage → Snowflake derivation translation ──────────────────────────────

_FUNC_SUBS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r'\bCurrentTimestamp\s*\(\s*\)', re.I), 'CURRENT_TIMESTAMP()'),
    (re.compile(r'\bCurrentDate\s*\(\s*\)', re.I), 'CURRENT_DATE()'),
    (re.compile(r'\bDateFromComponents\s*\(', re.I), 'DATE_FROM_PARTS('),
    (re.compile(r'\bDecimalToString\s*\(', re.I), 'TO_VARCHAR('),
    (re.compile(r'\bStringToDecimal\s*\(', re.I), 'TRY_TO_NUMBER('),
    (re.compile(r'\bNVL\s*\(', re.I), 'COALESCE('),
    (re.compile(r'\bLength\s*\(', re.I), 'LEN('),
    (re.compile(r'\bSubstring\s*\(', re.I), 'SUBSTR('),
    # TRY_CAST(col AS NUMBER...) → col  (NUMBER não precisa de cast)
    (re.compile(r'TRY_CAST\s*\(\s*(\w+)\s+AS\s+NUMBER[^)]*\)', re.I), r'\1'),
    # set_null() / setnull() → NULL
    (re.compile(r'\bset_null\s*\(\s*\)|\bsetnull\s*\(\s*\)', re.I), 'NULL'),
]

# If IsNull(x) Then y Else x → COALESCE(x, y)
_IF_ISNULL_COALESCE = re.compile(
    r'If\s+IsNull\s*\(([^)]+)\)\s+Then\s+(.+?)\s+Else\s+\1\s*$',
    re.I | re.DOTALL,
)

# IsNull(x) → x IS NULL
_ISNULL_FUNC = re.compile(r'\bIsNull\s*\(([^)]+)\)', re.I)

# LINK.COLUMN (not followed by '(') — for link-prefix stripping
_LINK_COL = re.compile(r'\b([A-Za-z_]\w*)\.([A-Za-z_]\w+)\b(?!\s*\()')

# Reserved SQL prefixes that should NOT be replaced with SRC.
_SQL_PREFIXES = frozenset({
    "SRC", "O", "TGT", "CASE", "WHEN", "AND", "OR", "NOT",
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
    "COALESCE", "NULLIF", "IFF", "TO_VARCHAR", "TO_DATE",
})


def _translate_derivation(
    deriv: str,
    proc_name: str = "",
    lookup_aliases: set[str] | None = None,
) -> str:
    """Translates a DataStage derivation expression to valid Snowflake SQL.

    Transformations applied (in order):
    1. DSJobName → literal procedure name string
    2. If IsNull(x) Then y Else x → COALESCE(x, y)
    3. IsNull(x) → x IS NULL
    4. Function name substitutions (CurrentTimestamp, NVL, etc.)
    5. Link.Column → SRC.Column (for non-lookup, non-SQL prefixes)

    Args:
        deriv: DataStage derivation expression.
        proc_name: Snowflake procedure name for DSJobName substitution.
        lookup_aliases: Set of lookup alias names (e.g. {'LKP_DIM_PROD'}).

    Returns:
        Snowflake-compatible SQL expression.
    """
    if lookup_aliases is None:
        lookup_aliases = set()
    upper_aliases = {a.upper() for a in lookup_aliases}

    if proc_name:
        deriv = re.sub(r'\bDSJobName\b', f"'{proc_name}'", deriv, flags=re.I)

    m = _IF_ISNULL_COALESCE.match(deriv.strip())
    if m:
        deriv = f"COALESCE({m.group(1).strip()}, {m.group(2).strip()})"

    deriv = _ISNULL_FUNC.sub(lambda mo: f"{mo.group(1)} IS NULL", deriv)

    for pat, replacement in _FUNC_SUBS:
        deriv = pat.sub(replacement, deriv)

    def _strip_link(mo: re.Match[str]) -> str:
        prefix = mo.group(1).upper()
        col = mo.group(2)
        if prefix in upper_aliases or prefix in _SQL_PREFIXES:
            return mo.group(0)
        return f"SRC.{col}"

    deriv = _LINK_COL.sub(_strip_link, deriv)
    return deriv.strip()


# ── Generator ─────────────────────────────────────────────────────────────────

class DimensionSqlGenerator:
    """Gera os 5 SQLs Snowflake para um job de dimensão SCD2.

    REGRA ABSOLUTA: colunas para DDL vêm de spec.oracle_columns (buscadas do
    Oracle antes da chamada). Se oracle_columns estiver vazio, usa derivações
    DataStage com aviso embutido no SQL.

    Attributes:
        spec: Especificação completa do job de dimensão.

    Exemplo::

        gen = DimensionSqlGenerator(spec)
        sqls = gen.generate_all()
        print(sqls["05_procedure"])
    """

    def __init__(self, spec: DimensionSpec) -> None:
        self.spec = spec
        self._proc_name = f"{spec.schema}.PRO_{spec.target_table}"
        self._pk_cols: frozenset[str] = frozenset(
            c.column_name.upper()
            for c in spec.oracle_constraints
            if c.constraint_type == "P"
        )
        self._lookup_aliases: set[str] = {
            f"LKP_{lkp.stage_name}" for lkp in spec.lookups
        }
        self._lookup_by_output: dict[str, object] = {
            lkp.output_col.upper(): lkp for lkp in spec.lookups
        }

    def generate_all(self) -> dict[str, str]:
        """Gera todos os 5 SQLs e retorna como dicionário ordenado.

        Returns:
            Dicionário com chaves '01_raw_table' a '05_procedure', valores = SQL.
        """
        return {
            "01_raw_table":       self._sql_raw_table(),
            "02_dim_table":       self._sql_dim_table(),
            "03_sequence":        self._sql_sequence(),
            "04_versiona_config": self._sql_versiona_config(),
            "05_procedure":       self._sql_procedure(),
        }

    # ── SQL 1: RAW TABLE ──────────────────────────────────────────────────────

    def _sql_raw_table(self) -> str:
        """CREATE TRANSIENT TABLE _RAW usando colunas Oracle + DAT_CRG_RGT_SNW."""
        s = self.spec
        lines: list[str] = []

        if not s.oracle_columns:
            lines.append(
                "    -- WARNING: oracle_columns vazio — DDL inferido do DataStage (revisar manualmente)"
            )
            for col in s.columns:
                col_up = col.name.upper()
                if col_up == s.surrogate_key.upper():
                    lines.append(f"    {col.name:<40} NUMBER(38) NOT NULL")
                    continue
                if col.is_scd_col:
                    continue
                sf_type = _infer_sf_type_from_ds_col(col)
                lines.append(f"    {col.name:<40} {sf_type}")
        else:
            for col in s.oracle_columns:
                sf_type = _oracle_to_sf_type(col)
                if sf_type is None:
                    lines.append(f"    -- {col.name} BLOB omitido")
                    continue
                nullable = "" if self._is_not_null(col) else ""
                not_null = " NOT NULL" if self._is_not_null(col) else ""
                comment = f" COMMENT '{col.comment}'" if col.comment else ""
                lines.append(f"    {col.name:<40} {sf_type}{not_null}{comment}")

        lines.append(
            f"    {'DAT_CRG_RGT_SNW':<40} TIMESTAMP_NTZ(9)"
            f" COMMENT 'Timestamp de carga no Snowflake — controle MAF'"
        )

        col_block = ",\n".join(lines)
        return (
            f"CREATE OR REPLACE TRANSIENT TABLE {s.schema}.{s.target_table}_RAW (\n"
            + col_block
            + "\n);"
        )

    # ── SQL 2: DIM TABLE (Time Travel) ────────────────────────────────────────

    def _sql_dim_table(self) -> str:
        """CREATE TRANSIENT TABLE via Time Travel de PROD (DWDEV.DWADM)."""
        s = self.spec
        prod = f"{s.prod_sf_schema}.{s.target_table}"
        return (
            f"CREATE OR REPLACE TRANSIENT TABLE {s.schema}.{s.target_table} AS\n"
            f"SELECT *\n"
            f"FROM {prod}\n"
            f"    AT(TIMESTAMP => DATEADD(HOUR, 23, CURRENT_DATE() - 1)::TIMESTAMP_LTZ);"
        )

    # ── SQL 3: SEQUENCE ───────────────────────────────────────────────────────

    def _sql_sequence(self) -> str:
        """CREATE SEQUENCE para surrogate key."""
        s = self.spec
        return (
            f"CREATE OR REPLACE SEQUENCE {s.schema}.SEQ_{s.target_table}\n"
            f"    START 1\n"
            f"    INCREMENT 1\n"
            f"    COMMENT '{s.target_table} surrogate key sequence — MAF generated';"
        )

    # ── SQL 4: DW_VERSIONA INSERT (WHERE NOT EXISTS) ──────────────────────────

    def _sql_versiona_config(self) -> str:
        """INSERT INTO DW_VERSIONA idempotente via WHERE NOT EXISTS."""
        s = self.spec
        proc = self._proc_name
        prod_dim = f"{s.prod_sf_schema}.{s.target_table}"
        raw_table = f"{s.schema}.{s.target_table}_RAW"
        bsk2 = f"'{s.business_key2}'" if s.business_key2 else "NULL"
        bsk3 = f"'{s.business_key3}'" if s.business_key3 else "NULL"
        qtd = self._qtd_dif_cam()

        return (
            f"INSERT INTO {DW_VERSIONA_TABLE} (\n"
            f"    NOM_JOB,\n"
            f"    NOM_TAB_DIM,\n"
            f"    NOM_TAB_RAW,\n"
            f"    NOM_SIS_ORI,\n"
            f"    COD_TAB_KEY,\n"
            f"    COD_TAB_BSK,\n"
            f"    COD_TAB_BSK2,\n"
            f"    COD_TAB_BSK3,\n"
            f"    QTD_DIF_CAM,\n"
            f"    FL_MN,\n"
            f"    FL_ATU_REG\n"
            f")\n"
            f"SELECT\n"
            f"    '{proc}',\n"
            f"    '{prod_dim}',\n"
            f"    '{raw_table}',\n"
            f"    '{s.nom_sis_ori}',\n"
            f"    '{s.surrogate_key}',\n"
            f"    '{s.business_key}',\n"
            f"    {bsk2},\n"
            f"    {bsk3},\n"
            f"    {qtd},\n"
            f"    '{s.fl_mn}',\n"
            f"    '1'\n"
            f"WHERE NOT EXISTS (\n"
            f"    SELECT 1 FROM {DW_VERSIONA_TABLE}\n"
            f"    WHERE NOM_JOB = '{proc}'\n"
            f");"
        )

    # ── SQL 5: PROCEDURE ──────────────────────────────────────────────────────

    def _sql_procedure(self) -> str:
        """CREATE PROCEDURE com WH VARCHAR, -1 surrogate, EXCEPTION block."""
        s = self.spec
        proc = self._proc_name
        raw = f"{s.schema}.{s.target_table}_RAW"

        col_list, select_list = self._build_insert_section()
        lookup_joins = self._lookup_join_clauses()
        dedup = self._dedup_qualify_clause()

        return (
            f"CREATE OR REPLACE PROCEDURE {proc}(WH VARCHAR)\n"
            f"RETURNS VARCHAR\n"
            f"LANGUAGE SQL\n"
            f"AS\n"
            f"$$\n"
            f"BEGIN\n"
            f"    TRUNCATE TABLE {raw};\n"
            f"\n"
            f"    INSERT INTO {raw} (\n"
            f"{col_list}\n"
            f"    )\n"
            f"    SELECT\n"
            f"{select_list}\n"
            f"    FROM (\n"
            f"        {s.source_select}\n"
            f"    ) SRC{lookup_joins}{dedup};\n"
            f"\n"
            f"    CALL {PRO_DW_VERSIONA}('{proc}');\n"
            f"\n"
            f"    RETURN 'SUCCESS: {proc} ' || CURRENT_TIMESTAMP()::VARCHAR;\n"
            f"\n"
            f"EXCEPTION\n"
            f"    WHEN OTHER THEN\n"
            f"        RETURN 'ERROR: ' || SQLERRM;\n"
            f"END;\n"
            f"$$;\n"
            f"\n"
            f"-- Execução: CALL {proc}('WH_NOME');"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _is_not_null(self, col: OracleColumnInfo) -> bool:
        if col.name.upper() in self._pk_cols:
            return True
        return not col.nullable

    def _qtd_dif_cam(self) -> int:
        """Conta colunas de negócio para QTD_DIF_CAM (exclui SK, SCD2, MAF)."""
        s = self.spec
        sk = s.surrogate_key.upper()
        if s.oracle_columns:
            return sum(
                1 for c in s.oracle_columns
                if c.name.upper() not in SCD2_ALL_CONTROL_COLS
                and c.name.upper() != sk
                and c.name.upper() != "DAT_CRG_RGT_SNW"
                and _oracle_to_sf_type(c) is not None
            )
        return sum(
            1 for c in s.columns
            if not c.is_scd_col and c.name.upper() != sk
        )

    def _build_insert_section(self) -> tuple[str, str]:
        """Gera (col_list, select_list) para INSERT INTO _RAW da procedure.

        Ordem:
          1. Surrogate key → -1
          2. Colunas de negócio (ordem Oracle ou DataStage se fallback)
          3. Colunas SCD2 de controle (hardcoded por fl_mn)
          4. DAT_CRG_RGT_SNW → CURRENT_TIMESTAMP()
        """
        s = self.spec
        scd = s.scd_cols
        sk_up = s.surrogate_key.upper()
        cols: list[str] = []
        vals: list[str] = []

        cols.append(f"        {s.surrogate_key}")
        vals.append(f"        -1")

        if s.oracle_columns:
            for oc in s.oracle_columns:
                col_up = oc.name.upper()
                if col_up == sk_up:
                    continue
                if col_up in SCD2_ALL_CONTROL_COLS:
                    continue
                if _oracle_to_sf_type(oc) is None:
                    continue
                expr = self._get_select_expr(oc.name)
                cols.append(f"        {oc.name}")
                vals.append(f"        {expr} AS {oc.name}")
        else:
            for col in s.columns:
                if col.name.upper() == sk_up or col.is_scd_col:
                    continue
                expr = self._translate(col.derivation)
                cols.append(f"        {col.name}")
                vals.append(f"        {expr} AS {col.name}")

        # SCD2 control columns
        if s.fl_mn == "1":
            cols.append(f"        {scd['active_flag']}")
            vals.append(f"        1 AS {scd['active_flag']}")
            cols.append(f"        {scd['start_date']}")
            vals.append(f"        CURRENT_DATE() AS {scd['start_date']}")
            cols.append(f"        {scd['end_date']}")
            vals.append(f"        TO_DATE('9999-12-31','YYYY-MM-DD') AS {scd['end_date']}")
            cols.append(f"        {scd['sys_name']}")
            vals.append(f"        '{s.nom_sis_ori}' AS {scd['sys_name']}")
            cols.append(f"        {scd['load_date']}")
            vals.append(f"        CURRENT_DATE() AS {scd['load_date']}")
        else:
            cols.append(f"        {scd['active_flag']}")
            vals.append(f"        'A' AS {scd['active_flag']}")
            cols.append(f"        {scd['start_date']}")
            vals.append(f"        CURRENT_DATE() AS {scd['start_date']}")
            cols.append(f"        {scd['end_date']}")
            vals.append(f"        TO_DATE('9999-12-31','YYYY-MM-DD') AS {scd['end_date']}")
            cols.append(f"        {scd['sys_name']}")
            vals.append(f"        '{s.nom_sis_ori}' AS {scd['sys_name']}")
            cols.append(f"        {scd['load_date']}")
            vals.append(f"        CURRENT_DATE() AS {scd['load_date']}")

        cols.append(f"        DAT_CRG_RGT_SNW")
        vals.append(f"        CURRENT_TIMESTAMP()")

        return ",\n".join(cols), ",\n".join(vals)

    def _get_select_expr(self, col_name: str) -> str:
        """Retorna a expressão SELECT para uma coluna de negócio.

        Prioridade:
          1. Lookup output → COALESCE(alias.col, default)
          2. Derivação DataStage → traduzida
          3. Default → SRC.col_name
        """
        col_up = col_name.upper()

        lkp = self._lookup_by_output.get(col_up)
        if lkp is not None:
            alias = f"LKP_{lkp.stage_name}"
            return f"COALESCE({alias}.{lkp.output_col}, {lkp.default_value})"

        for ds_col in self.spec.columns:
            if ds_col.name.upper() == col_up:
                return self._translate(ds_col.derivation)

        return f"SRC.{col_name}"

    def _translate(self, deriv: str) -> str:
        return _translate_derivation(
            deriv,
            proc_name=self._proc_name,
            lookup_aliases=self._lookup_aliases,
        )

    def _lookup_join_clauses(self) -> str:
        """Gera LEFT JOINs para todos os lookups com filtros SCD2."""
        if not self.spec.lookups:
            return ""
        s = self.spec
        lines: list[str] = []
        for lkp in s.lookups:
            alias = f"LKP_{lkp.stage_name}"
            if lkp.join_keys:
                cond = " AND ".join(
                    f"TO_VARCHAR(SRC.{k}) = TO_VARCHAR({alias}.{k})"
                    for k in lkp.join_keys
                )
            else:
                cond = (
                    f"TO_VARCHAR(SRC.{lkp.hash_key}) = TO_VARCHAR({alias}.{lkp.hash_key})"
                )
            if s.fl_mn == "1":
                scd_filter = f"\n        AND {alias}.IDT_RGT_ATU = 1"
                sys_filter = f"\n        AND {alias}.NOM_SIS_ORI = '{s.nom_sis_ori}'"
            else:
                scd_filter = f"\n        AND {alias}.RECORD_STATUS = 'A'"
                sys_filter = f"\n        AND {alias}.SRC_SYS_NAME = '{s.nom_sis_ori}'"
            lines.append(
                f"\n    LEFT JOIN {lkp.lookup_table} AS {alias}"
                f"\n        ON {cond}{scd_filter}{sys_filter}"
            )
        return "".join(lines)

    def _dedup_qualify_clause(self) -> str:
        """Gera QUALIFY ROW_NUMBER se spec.has_row_number for True."""
        if not self.spec.has_row_number:
            return ""
        s = self.spec
        return (
            f"\n    QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {s.business_key} ORDER BY {s.surrogate_key} DESC) = 1"
        )
