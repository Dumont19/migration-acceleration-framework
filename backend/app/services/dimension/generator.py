"""
services/dimension/generator.py
---------------------------------
DimensionSqlGenerator — gera os 6 SQLs para migração de dimensão SCD2.

Ordem de geração:
  1. CREATE OR REPLACE TRANSIENT TABLE {SCHEMA}.{TABLE}_RAW
  2. CREATE OR REPLACE SEQUENCE {SCHEMA}.SEQ_{TABLE}
  3. CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE}
  4. INSERT INTO DWDEV.HUGOA.DW_VERSIONA (config do job)
  5. CREATE OR REPLACE PROCEDURE {SCHEMA}.PRO_{TABLE}
  6. CREATE OR REPLACE TASK {SCHEMA}.TSK_{TABLE} + ALTER TASK SUSPEND
"""
from __future__ import annotations

import re

from .schemas import ColumnSpec, DimensionSpec

_DW_VERSIONA_TABLE = "DWDEV.HUGOA.DW_VERSIONA"
_PRO_DW_VERSIONA = "DWDEV.HUGOA.PRO_DW_VERSIONA"


class DimensionSqlGenerator:
    def __init__(self, spec: DimensionSpec) -> None:
        self.spec = spec

    def generate_all(self) -> dict[str, str]:
        """Retorna dicionário ordenado com os 6 SQLs."""
        return {
            "01_raw_table": self._sql_raw_table(),
            "02_sequence": self._sql_sequence(),
            "03_dim_table": self._sql_dim_table(),
            "04_versiona_config": self._sql_versiona_config(),
            "05_procedure": self._sql_procedure(),
            "06_task": self._sql_task(),
        }

    # ── SQL 1: RAW table ──────────────────────────────────────────────────────

    def _sql_raw_table(self) -> str:
        s = self.spec
        col_defs = self._column_definitions(include_surrogate=False, include_scd=False)
        return (
            f"CREATE OR REPLACE TRANSIENT TABLE {s.schema}.{s.raw_table} (\n"
            + col_defs
            + "\n);"
        )

    # ── SQL 2: SEQUENCE ───────────────────────────────────────────────────────

    def _sql_sequence(self) -> str:
        s = self.spec
        return f"CREATE OR REPLACE SEQUENCE {s.schema}.SEQ_{s.target_table} START = 1 INCREMENT = 1;"

    # ── SQL 3: DIM table ──────────────────────────────────────────────────────

    def _sql_dim_table(self) -> str:
        s = self.spec
        scd = s.scd_cols
        col_defs = self._column_definitions(include_surrogate=True, include_scd=True)
        return (
            f"CREATE TABLE IF NOT EXISTS {s.schema}.{s.target_table} (\n"
            + col_defs
            + f",\n    {scd['active_flag']}        NUMBER(1),"
            + f"\n    {scd['start_date']}     TIMESTAMP_NTZ,"
            + f"\n    {scd['sys_name']}       VARCHAR(100),"
            + f"\n    {scd['load_date']}      TIMESTAMP_NTZ"
            + "\n);"
        )

    # ── SQL 4: DW_VERSIONA config ─────────────────────────────────────────────

    def _sql_versiona_config(self) -> str:
        s = self.spec
        scd = s.scd_cols
        return f"""-- Configuração do job na tabela de controle SCD2
MERGE INTO {_DW_VERSIONA_TABLE} AS tgt
USING (
    SELECT
        '{s.versiona_job_name or s.job_name}'  AS NOM_JOB,
        '{s.schema}.{s.target_table}'           AS NOM_TAB_DIM,
        '{s.surrogate_key}'                     AS NOM_COL_SRG,
        '{s.business_key}'                      AS NOM_COL_BSK,
        '{scd["active_flag"]}'                  AS NOM_COL_STA,
        '{scd["start_date"]}'                   AS NOM_COL_DAT,
        '{scd["sys_name"]}'                     AS NOM_COL_SIS,
        '{scd["load_date"]}'                    AS NOM_COL_CAR,
        '{s.fl_mn}'                             AS FL_MN,
        '{s.nom_sis_ori}'                       AS NOM_SIS_ORI
) AS src ON tgt.NOM_JOB = src.NOM_JOB
WHEN MATCHED THEN UPDATE SET
    tgt.NOM_TAB_DIM = src.NOM_TAB_DIM,
    tgt.NOM_COL_SRG = src.NOM_COL_SRG,
    tgt.NOM_COL_BSK = src.NOM_COL_BSK,
    tgt.NOM_COL_STA = src.NOM_COL_STA,
    tgt.NOM_COL_DAT = src.NOM_COL_DAT,
    tgt.NOM_COL_SIS = src.NOM_COL_SIS,
    tgt.NOM_COL_CAR = src.NOM_COL_CAR,
    tgt.FL_MN        = src.FL_MN,
    tgt.NOM_SIS_ORI  = src.NOM_SIS_ORI
WHEN NOT MATCHED THEN INSERT (
    NOM_JOB, NOM_TAB_DIM, NOM_COL_SRG, NOM_COL_BSK,
    NOM_COL_STA, NOM_COL_DAT, NOM_COL_SIS, NOM_COL_CAR,
    FL_MN, NOM_SIS_ORI
) VALUES (
    src.NOM_JOB, src.NOM_TAB_DIM, src.NOM_COL_SRG, src.NOM_COL_BSK,
    src.NOM_COL_STA, src.NOM_COL_DAT, src.NOM_COL_SIS, src.NOM_COL_CAR,
    src.FL_MN, src.NOM_SIS_ORI
);"""

    # ── SQL 5: PROCEDURE ──────────────────────────────────────────────────────

    def _sql_procedure(self) -> str:
        s = self.spec
        insert_cols, insert_vals = self._insert_cols_and_vals()
        lookup_joins = self._lookup_join_clauses()
        dedup_clause = self._dedup_qualify_clause()

        return f"""CREATE OR REPLACE PROCEDURE {s.schema}.PRO_{s.target_table}()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- 1. Truncar RAW
    TRUNCATE TABLE {s.schema}.{s.raw_table};

    -- 2. Carregar RAW a partir da ODS
    INSERT INTO {s.schema}.{s.raw_table} (
{insert_cols}
    )
    SELECT
{insert_vals}
    FROM (
        {s.source_select}
    ) SRC{lookup_joins}{dedup_clause};

    -- 3. Versionar (SCD2)
    CALL {_PRO_DW_VERSIONA}('{s.versiona_job_name or s.job_name}');

    RETURN 'OK';
END;
$$;"""

    # ── SQL 6: TASK ───────────────────────────────────────────────────────────

    def _sql_task(self) -> str:
        s = self.spec
        task_name = f"{s.schema}.TSK_{s.target_table}"
        proc_call = f"{s.schema}.PRO_{s.target_table}()"
        return (
            f"CREATE OR REPLACE TASK {task_name}\n"
            f"    WAREHOUSE = WH_COMPUTE\n"
            f"    SCHEDULE  = 'USING CRON 0 6 * * * America/Sao_Paulo'\n"
            f"AS\n"
            f"    CALL {proc_call};\n\n"
            f"-- Task criada pausada — ativar com: ALTER TASK {task_name} RESUME;\n"
            f"ALTER TASK {task_name} SUSPEND;"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _column_definitions(
        self, include_surrogate: bool, include_scd: bool
    ) -> str:
        s = self.spec
        scd_names = set(s.scd_cols.values())
        lines: list[str] = []

        if include_surrogate:
            lines.append(f"    {s.surrogate_key:40s} NUMBER(38) NOT NULL PRIMARY KEY")

        for col in s.columns:
            if col.is_scd_col and not include_scd:
                continue
            if col.name.upper() == s.surrogate_key.upper():
                continue
            sf_type = self._infer_snowflake_type(col)
            lines.append(f"    {col.name:40s} {sf_type}")

        return ",\n".join(lines)

    def _insert_cols_and_vals(self) -> tuple[str, str]:
        s = self.spec
        scd_names = set(s.scd_cols.values())
        cols: list[str] = []
        vals: list[str] = []

        # Surrogate via SEQUENCE
        cols.append(f"        {s.surrogate_key}")
        vals.append(f"        {s.schema}.SEQ_{s.target_table}.NEXTVAL")

        for col in s.columns:
            if col.is_scd_col:
                continue
            if col.name.upper() == s.surrogate_key.upper():
                continue
            cols.append(f"        {col.name}")
            deriv = self._clean_derivation(col.derivation)
            vals.append(f"        {deriv} AS {col.name}")

        return ",\n".join(cols), ",\n".join(vals)

    def _lookup_join_clauses(self) -> str:
        if not self.spec.lookups:
            return ""
        lines: list[str] = []
        for lkp in self.spec.lookups:
            join_cond = " AND ".join(
                f"SRC.{k} = LKP_{lkp.stage_name}.{k}" for k in lkp.join_keys
            ) or f"SRC.{lkp.hash_key} = LKP_{lkp.stage_name}.{lkp.hash_key}"
            lines.append(
                f"\n    LEFT JOIN {lkp.lookup_table} AS LKP_{lkp.stage_name}"
                f"\n        ON {join_cond}"
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

    @staticmethod
    def _clean_derivation(deriv: str) -> str:
        """
        Converte derivações DataStage para SQL Snowflake:
        - Remove TRY_CAST de colunas NUMBER
        - Substitui != por IS DISTINCT FROM em comparações com NULL
        - Converte set_null() / setnull() → NULL
        """
        # TRY_CAST em NUMBER → remover cast
        deriv = re.sub(
            r"TRY_CAST\s*\(\s*(\w+)\s+AS\s+NUMBER[^)]*\)",
            r"\1",
            deriv,
            flags=re.IGNORECASE,
        )
        # set_null / setnull → NULL
        deriv = re.sub(r"(?i)set_null\(\)|setnull\(\)", "NULL", deriv)
        return deriv.strip()

    @staticmethod
    def _infer_snowflake_type(col: ColumnSpec) -> str:
        """Inferência de tipo Snowflake a partir do nome/derivação da coluna."""
        name_up = col.name.upper()
        deriv_up = col.derivation.upper()

        if any(k in name_up for k in ("_DAT", "DAT_", "_DATE", "DATE_", "DT_", "_DT")):
            return "TIMESTAMP_NTZ"
        if any(k in name_up for k in ("_ID", "IDT_", "_QTD", "_VLR", "_NRO", "NRO_", "_NUM")):
            return "NUMBER(38)"
        if "SYSDATE" in deriv_up or "CURRENT_TIMESTAMP" in deriv_up:
            return "TIMESTAMP_NTZ"
        if "NEXTVAL" in deriv_up:
            return "NUMBER(38) NOT NULL"
        return "VARCHAR(4000)"
