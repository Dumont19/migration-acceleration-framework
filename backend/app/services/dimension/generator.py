"""
services/dimension/generator.py
---------------------------------
DimensionSqlGenerator — gera os 5 SQLs para migração de dimensão SCD2.

REGRA ABSOLUTA: colunas para DDL vêm do Oracle (spec.oracle_columns).
Se oracle_columns estiver vazio, o DDL usa derivações DataStage com aviso.

Ordem de geração:
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
    """Infers a Snowflake type from a DataStage ColumnSpec (fallback only)."""
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
    (re.compile(r'TRY_CAST\s*\(\s*(\w+)\s+AS\s+NUMBER[^)]*\)', re.I), r'\1'),
    (re.compile(r'\bset_null\s*\(\s*\)|\bsetnull\s*\(\s*\)', re.I), 'NULL'),
]

_IF_ISNULL_COALESCE = re.compile(
    r'If\s+IsNull\s*\(([^)]+)\)\s+Then\s+(.+?)\s+Else\s+\1\s*$',
    re.I | re.DOTALL,
)
_ISNULL_FUNC = re.compile(r'\bIsNull\s*\(([^)]+)\)', re.I)
_LINK_COL = re.compile(r'\b([A-Za-z_]\w*)\.([A-Za-z_]\w+)\b(?!\s*\()')

_SQL_PREFIXES = frozenset({
    "SRC", "O", "TGT", "CASE", "WHEN", "AND", "OR", "NOT",
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
    "COALESCE", "NULLIF", "IFF", "TO_VARCHAR", "TO_DATE",
})


def _translate_derivation(
    deriv: str,
    proc_name: str = "",
    lookup_aliases: set[str] | None = None,
    src_alias: str = "O",
) -> str:
    """Translates a DataStage derivation expression to valid Snowflake SQL."""
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
        return f"{src_alias}.{col}"

    deriv = _LINK_COL.sub(_strip_link, deriv)
    return deriv.strip()


# ── Generator ─────────────────────────────────────────────────────────────────

class DimensionSqlGenerator:
    """Gera os 5 SQLs Snowflake para um job de dimensão SCD2.

    REGRA ABSOLUTA: colunas para DDL vêm de spec.oracle_columns.
    Se oracle_columns estiver vazio, usa derivações DataStage com aviso.
    """

    def __init__(self, spec: DimensionSpec) -> None:
        self.spec = spec
        self._proc_full = f"{spec.schema}.PRO_{spec.target_table}"
        self._proc_short = f"PRO_{spec.target_table}"
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
        return {
            "01_raw_table":       self._sql_raw_table(),
            "02_dim_table":       self._sql_dim_table(),
            "03_sequence":        self._sql_sequence(),
            "04_versiona_config": self._sql_versiona_config(),
            "05_procedure":       self._sql_procedure(),
        }

    # ── SQL 1: RAW TABLE ──────────────────────────────────────────────────────

    def _sql_raw_table(self) -> str:
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
            oracle_col_names = {c.name.upper() for c in s.oracle_columns}
            for col in s.oracle_columns:
                sf_type = _oracle_to_sf_type(col)
                if sf_type is None:
                    lines.append(f"    -- {col.name} BLOB omitido")
                    continue
                not_null = " NOT NULL" if self._is_not_null(col) else ""
                comment = f" COMMENT '{col.comment}'" if col.comment else ""
                lines.append(f"    {col.name:<40} {sf_type}{not_null}{comment}")
            if "DAT_CRG_RGT" not in oracle_col_names:
                lines.append(f"    {'DAT_CRG_RGT':<40} TIMESTAMP_NTZ")

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
        s = self.spec
        return (
            f"CREATE OR REPLACE SEQUENCE {s.schema}.SEQ_{s.target_table}\n"
            f"    START 1\n"
            f"    INCREMENT 1\n"
            f"    COMMENT '{s.target_table} surrogate key sequence — MAF generated';"
        )

    # ── SQL 4: DW_VERSIONA INSERT (WHERE NOT EXISTS) ──────────────────────────

    def _sql_versiona_config(self) -> str:
        s = self.spec
        prod_dim = f"{s.prod_sf_schema}.{s.target_table}"
        seq_name = f"{s.schema}.SEQ_{s.target_table}"
        proc_short = self._proc_short

        if s.fl_mn == "1":
            table_filter = f"AND NOM_SIS_ORI = ''{s.nom_sis_ori}''"
        else:
            table_filter = f"AND SRC_SYS_NAME = ''{s.nom_sis_ori}''"

        fields_diff = self._compute_fields_diff()

        return (
            f"INSERT INTO {DW_VERSIONA_TABLE} (\n"
            f"    JOB_NAME,\n"
            f"    TABLE_NAME,\n"
            f"    TABLE_FILTER,\n"
            f"    TABLE_KEY,\n"
            f"    TABLE_SRC_KEY,\n"
            f"    FIELDS_DIFF,\n"
            f"    SEQ_NAME,\n"
            f"    FL_MN\n"
            f")\n"
            f"SELECT\n"
            f"    '{proc_short}',\n"
            f"    '{prod_dim}',\n"
            f"    '{table_filter}',\n"
            f"    '{s.surrogate_key}',\n"
            f"    '{s.business_key}',\n"
            f"    '{fields_diff}',\n"
            f"    '{seq_name}',\n"
            f"    '{s.fl_mn}'\n"
            f"WHERE NOT EXISTS (\n"
            f"    SELECT 1 FROM {DW_VERSIONA_TABLE}\n"
            f"    WHERE JOB_NAME = '{proc_short}'\n"
            f");"
        )

    # ── SQL 5: PROCEDURE ──────────────────────────────────────────────────────

    def _sql_procedure(self) -> str:
        s = self.spec
        proc_full = self._proc_full
        proc_short = self._proc_short
        raw = f"{s.schema}.{s.target_table}_RAW"
        ods_ref = self._get_ods_ref()

        col_list, select_list = self._build_insert_section()
        lookup_joins = self._lookup_join_clauses()
        dedup = self._dedup_qualify_clause()

        return (
            f"CREATE OR REPLACE PROCEDURE {proc_full}(WH VARCHAR)\n"
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
            f"    FROM {ods_ref} O{lookup_joins}{dedup};\n"
            f"\n"
            f"    CALL {PRO_DW_VERSIONA}('{proc_short}', 'VRS');\n"
            f"\n"
            f"    RETURN 'OK: {proc_short} ' || CURRENT_TIMESTAMP()::VARCHAR;\n"
            f"\n"
            f"EXCEPTION\n"
            f"    WHEN OTHER THEN\n"
            f"        RETURN 'ERRO: ' || SQLERRM;\n"
            f"END;\n"
            f"$$;\n"
            f"\n"
            f"-- Execução: CALL {proc_full}('WH_NOME');"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _is_not_null(self, col: OracleColumnInfo) -> bool:
        if col.name.upper() in self._pk_cols:
            return True
        return not col.nullable

    def _get_ods_ref(self) -> str:
        s = self.spec
        if s.source_table:
            return f"{s.source_schema}.{s.source_table}"
        m = re.search(r'\bFROM\s+(?:[\w.]+\.)?(\w+)\b', s.source_select, re.I)
        tbl = m.group(1) if m else "ODS_TABLE"
        return f"{s.source_schema}.{tbl}"

    def _compute_fields_diff(self) -> str:
        """Comma-separated business column names for FIELDS_DIFF in DW_VERSIONA."""
        s = self.spec
        sk = s.surrogate_key.upper()
        bk = s.business_key.upper()
        exclude = SCD2_ALL_CONTROL_COLS | {"DAT_CRG_RGT_SNW", "DAT_CRG_RGT"} | {sk, bk}
        if s.oracle_columns:
            cols = [
                c.name for c in s.oracle_columns
                if c.name.upper() not in exclude
                and _oracle_to_sf_type(c) is not None
            ]
        else:
            cols = [
                c.name for c in s.columns
                if c.name.upper() not in exclude and not c.is_scd_col
            ]
        return ", ".join(cols)

    def _qtd_dif_cam(self) -> int:
        """Conta colunas de negócio (não usado em DW_VERSIONA mas mantido para compatibilidade)."""
        return len(self._compute_fields_diff().split(", ")) if self._compute_fields_diff() else 0

    def _build_insert_section(self) -> tuple[str, str]:
        """Gera (col_list, select_list) para INSERT INTO _RAW da procedure.

        Ordem:
          1. Colunas de negócio (Oracle ou DataStage fallback) — sem surrogate key
          2. Colunas SCD2 de controle hardcoded por fl_mn + NOM_JOB_CRG
          3. DAT_CRG_RGT → CURRENT_TIMESTAMP()
          4. DAT_CRG_RGT_SNW → CURRENT_TIMESTAMP()
        """
        s = self.spec
        scd = s.scd_cols
        sk_up = s.surrogate_key.upper()
        cols: list[str] = []
        vals: list[str] = []

        if s.oracle_columns:
            for oc in s.oracle_columns:
                col_up = oc.name.upper()
                if col_up == sk_up:
                    continue
                if col_up in SCD2_ALL_CONTROL_COLS:
                    continue
                if col_up in {"DAT_CRG_RGT", "DAT_CRG_RGT_SNW"}:
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
            vals.append(f"        3 AS {scd['active_flag']}")
            cols.append(f"        {scd['start_date']}")
            vals.append(f"        CURRENT_DATE() AS {scd['start_date']}")
            cols.append(f"        {scd['end_date']}")
            vals.append(f"        TO_DATE('9999-12-31','YYYY-MM-DD') AS {scd['end_date']}")
            cols.append(f"        {scd['sys_name']}")
            vals.append(f"        '{s.nom_sis_ori}' AS {scd['sys_name']}")
            cols.append(f"        {scd['load_date']}")
            vals.append(f"        CURRENT_TIMESTAMP() AS {scd['load_date']}")
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

        cols.append(f"        {scd['job_name']}")
        vals.append(f"        '{self._proc_short}' AS {scd['job_name']}")

        cols.append(f"        DAT_CRG_RGT")
        vals.append(f"        CURRENT_TIMESTAMP() AS DAT_CRG_RGT")

        cols.append(f"        DAT_CRG_RGT_SNW")
        vals.append(f"        CURRENT_TIMESTAMP() AS DAT_CRG_RGT_SNW")

        return ",\n".join(cols), ",\n".join(vals)

    def _get_select_expr(self, col_name: str) -> str:
        col_up = col_name.upper()

        lkp = self._lookup_by_output.get(col_up)
        if lkp is not None:
            alias = f"LKP_{lkp.stage_name}"
            return f"COALESCE({alias}.{lkp.output_col}, {lkp.default_value})"

        for ds_col in self.spec.columns:
            if ds_col.name.upper() == col_up:
                return self._translate(ds_col.derivation)

        return f"O.{col_name}"

    def _translate(self, deriv: str) -> str:
        return _translate_derivation(
            deriv,
            proc_name=self._proc_full,
            lookup_aliases=self._lookup_aliases,
            src_alias="O",
        )

    def _lookup_join_clauses(self) -> str:
        if not self.spec.lookups:
            return ""
        s = self.spec
        lines: list[str] = []
        for lkp in s.lookups:
            alias = f"LKP_{lkp.stage_name}"
            if lkp.join_keys:
                cond = " AND ".join(
                    f"TO_VARCHAR(O.{k}) = TO_VARCHAR({alias}.{k})"
                    for k in lkp.join_keys
                )
            else:
                cond = (
                    f"TO_VARCHAR(O.{lkp.hash_key}) = TO_VARCHAR({alias}.{lkp.hash_key})"
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
        if not self.spec.has_row_number:
            return ""
        s = self.spec
        return (
            f"\n    QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {s.business_key} ORDER BY {s.surrogate_key} DESC) = 1"
        )
