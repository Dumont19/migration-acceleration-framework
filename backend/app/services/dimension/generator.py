"""
services/dimension/generator.py
---------------------------------
DimensionSqlGenerator — gera os 6 SQLs para migração de dimensão SCD2.

Ordem de geração:
  01. CREATE OR REPLACE TRANSIENT TABLE {SCHEMA}.{TABLE}_RAW
  02. CREATE OR REPLACE SEQUENCE {SCHEMA}.SEQ_{TABLE}
  03. CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE}
  04. MERGE INTO DWDEV.HUGOA.DW_VERSIONA (config do job)
  05. CREATE OR REPLACE PROCEDURE {SCHEMA}.PRO_{TABLE}
  06. CREATE OR REPLACE TASK {SCHEMA}.TSK_{TABLE} + ALTER TASK SUSPEND
"""
from __future__ import annotations

import re

from app.core.constants import (
    DW_VERSIONA_TABLE,
    PRO_DW_VERSIONA,
    TASK_SCHEDULE_CRON,
    TASK_WAREHOUSE,
    VARCHAR_MAX_LENGTH,
)
from .schemas import ColumnSpec, DimensionSpec


class DimensionSqlGenerator:
    """Gera os 6 SQLs Snowflake para um job de dimensão SCD2.

    Recebe um :class:`DimensionSpec` (extraído pelo :class:`DimensionSpecExtractor`
    ou construído manualmente) e produz DDL e DML prontos para execução.

    Attributes:
        spec: Especificação do job de dimensão.

    Exemplo::

        gen = DimensionSqlGenerator(spec)
        sqls = gen.generate_all()
        print(sqls["03_dim_table"])
    """

    def __init__(self, spec: DimensionSpec) -> None:
        """Inicializa o gerador com um :class:`DimensionSpec`.

        Args:
            spec: Metadados completos do job de dimensão.
        """
        self.spec = spec

    def generate_all(self) -> dict[str, str]:
        """Gera todos os 6 SQLs e retorna como dicionário ordenado.

        Returns:
            Dicionário com chaves ``01_raw_table`` a ``06_task``, valores = SQL pronto.
        """
        return {
            "01_raw_table":      self._sql_raw_table(),
            "02_sequence":       self._sql_sequence(),
            "03_dim_table":      self._sql_dim_table(),
            "04_versiona_config": self._sql_versiona_config(),
            "05_procedure":      self._sql_procedure(),
            "06_task":           self._sql_task(),
        }

    # ── SQL 1: RAW table ──────────────────────────────────────────────────────

    def _sql_raw_table(self) -> str:
        """Gera CREATE TRANSIENT TABLE para a tabela de staging _RAW."""
        s = self.spec
        col_defs = self._column_definitions(include_surrogate=False, include_scd=False)
        return (
            f"CREATE OR REPLACE TRANSIENT TABLE {s.schema}.{s.raw_table} (\n"
            + col_defs
            + "\n);"
        )

    # ── SQL 2: SEQUENCE ───────────────────────────────────────────────────────

    def _sql_sequence(self) -> str:
        """Gera CREATE SEQUENCE para geração da surrogate key."""
        s = self.spec
        return f"CREATE OR REPLACE SEQUENCE {s.schema}.SEQ_{s.target_table} START = 1 INCREMENT = 1;"

    # ── SQL 3: DIM table ──────────────────────────────────────────────────────

    def _sql_dim_table(self) -> str:
        """Gera CREATE TABLE para a tabela DIM com colunas SCD2."""
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
        """Gera MERGE INTO DW_VERSIONA para registrar o job na tabela de controle SCD2."""
        s = self.spec
        scd = s.scd_cols
        return f"""-- Configuração do job na tabela de controle SCD2
MERGE INTO {DW_VERSIONA_TABLE} AS tgt
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
        """Gera CREATE PROCEDURE com TRUNCATE + INSERT + CALL PRO_DW_VERSIONA."""
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
    CALL {PRO_DW_VERSIONA}('{s.versiona_job_name or s.job_name}');

    RETURN 'OK';
END;
$$;"""

    # ── SQL 6: TASK ───────────────────────────────────────────────────────────

    def _sql_task(self) -> str:
        """Gera CREATE TASK agendada + ALTER TASK SUSPEND (criada pausada por segurança)."""
        s = self.spec
        task_name = f"{s.schema}.TSK_{s.target_table}"
        proc_call = f"{s.schema}.PRO_{s.target_table}()"
        return (
            f"CREATE OR REPLACE TASK {task_name}\n"
            f"    WAREHOUSE = {TASK_WAREHOUSE}\n"
            f"    SCHEDULE  = '{TASK_SCHEDULE_CRON}'\n"
            f"AS\n"
            f"    CALL {proc_call};\n\n"
            f"-- Task criada pausada — ativar com: ALTER TASK {task_name} RESUME;\n"
            f"ALTER TASK {task_name} SUSPEND;"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _column_definitions(
        self, include_surrogate: bool, include_scd: bool
    ) -> str:
        """Gera as linhas de definição de colunas para CREATE TABLE.

        Args:
            include_surrogate: Se True, inclui a surrogate key como primeira coluna.
            include_scd: Se True, inclui as colunas SCD2 de controle.

        Returns:
            String com as definições separadas por vírgula e newline.
        """
        s = self.spec
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
        """Gera as listas de colunas e valores para o INSERT INTO _RAW.

        Returns:
            Tupla (cols_str, vals_str) prontas para interpolação no SQL da procedure.
        """
        s = self.spec
        cols: list[str] = []
        vals: list[str] = []

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
        """Gera as cláusulas LEFT JOIN para todos os lookups CHashedFileStage.

        Returns:
            String com os JOINs prontos para inserção no FROM da procedure.
            Vazia se não há lookups.
        """
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
        """Gera cláusula QUALIFY ROW_NUMBER se o spec indicar deduplicação.

        Returns:
            Cláusula QUALIFY como string, ou string vazia se não aplicável.
        """
        if not self.spec.has_row_number:
            return ""
        s = self.spec
        return (
            f"\n    QUALIFY ROW_NUMBER() OVER ("
            f"PARTITION BY {s.business_key} ORDER BY {s.surrogate_key} DESC) = 1"
        )

    @staticmethod
    def _clean_derivation(deriv: str) -> str:
        """Converte derivações DataStage para SQL Snowflake válido.

        Transformações aplicadas:
        - ``TRY_CAST(col AS NUMBER...)`` → ``col`` (NUMBER não precisa de cast)
        - ``set_null()`` / ``setnull()`` → ``NULL``

        Args:
            deriv: Expressão de derivação extraída do TrxGenCode DataStage.

        Returns:
            Expressão SQL pronta para Snowflake.
        """
        deriv = re.sub(
            r"TRY_CAST\s*\(\s*(\w+)\s+AS\s+NUMBER[^)]*\)",
            r"\1",
            deriv,
            flags=re.IGNORECASE,
        )
        deriv = re.sub(r"(?i)set_null\(\)|setnull\(\)", "NULL", deriv)
        return deriv.strip()

    @staticmethod
    def _infer_snowflake_type(col: ColumnSpec) -> str:
        """Infere o tipo Snowflake a partir do nome e derivação da coluna.

        Aplica heurísticas por padrão de nome:
        - ``_DAT``, ``DAT_``, ``DT_`` → ``TIMESTAMP_NTZ``
        - ``_ID``, ``IDT_``, ``_QTD``, ``_VLR``, ``_NRO`` → ``NUMBER(38)``
        - Derivação com ``SYSDATE`` ou ``CURRENT_TIMESTAMP`` → ``TIMESTAMP_NTZ``
        - Derivação com ``NEXTVAL`` → ``NUMBER(38) NOT NULL``
        - Fallback → ``VARCHAR(4000)``

        Args:
            col: Coluna a ser tipada.

        Returns:
            Tipo Snowflake como string.
        """
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
        return f"VARCHAR({VARCHAR_MAX_LENGTH})"
