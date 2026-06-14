"""
services/dimension/homologator.py
-----------------------------------
DimensionHomologator — gera 7 queries de validação para
homologação DEV vs PROD de tabelas de dimensão SCD2.
"""
from __future__ import annotations

from .schemas import DimensionSpec

_HOMO_COUNT_LIMIT = 90


class DimensionHomologator:
    """Gera 7 queries de homologação para comparar DEV vs PROD.

    Queries geradas:
      01 — Snapshot PROD via Time Travel
      02 — COUNT DEV total
      03 — COUNT PROD total
      04 — COUNT DEV por data de carga
      05 — COUNT PROD por data de carga
      06 — MINUS DEV menos PROD (com SELECT * EXCLUDE)
      07 — UNION ALL DEV vs PROD para um BSK_ID específico

    Attributes:
        spec: Especificação do job de dimensão.
        data_teste: Data de referência para Time Travel (YYYY-MM-DD).
        bsk_id: Valor da business key para query 07.
        offset_hours: Horas de offset aplicadas sobre data_teste - 1 (default 23).
    """

    def __init__(
        self,
        spec: DimensionSpec,
        data_teste: str,
        bsk_id: str,
        offset_hours: int = 23,
    ) -> None:
        self.spec = spec
        self.data_teste = data_teste
        self.bsk_id = bsk_id
        self.offset_hours = offset_hours

    def generate(self) -> dict[str, str]:
        return {
            "01_snapshot_prod":       self._sql_snapshot_prod(),
            "02_count_dev_total":     self._sql_count_dev_total(),
            "03_count_prod_total":    self._sql_count_prod_total(),
            "04_count_dev_por_data":  self._sql_count_dev_by_date(),
            "05_count_prod_por_data": self._sql_count_prod_by_date(),
            "06_minus_dev_prod":      self._sql_minus(),
            "07_union_bsk_id":        self._sql_union_bsk(),
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _dev_table(self) -> str:
        s = self.spec
        return f"{s.schema}.{s.target_table}"

    def _prod_table(self) -> str:
        s = self.spec
        return f"{s.prod_sf_schema}.{s.target_table}"

    def _tt_timestamp(self) -> str:
        return (
            f"DATEADD(HOUR, {self.offset_hours}, "
            f"TO_DATE('{self.data_teste}', 'YYYY-MM-DD') - 1)::TIMESTAMP_LTZ"
        )

    def _active_filter(self) -> str:
        s = self.spec
        if s.fl_mn == "1":
            return f"IDT_RGT_ATU = 1\n  AND NOM_SIS_ORI = '{s.nom_sis_ori}'"
        return f"RECORD_STATUS = 'A'\n  AND SRC_SYS_NAME = '{s.nom_sis_ori}'"

    def _load_date_col(self) -> str:
        s = self.spec
        return s.scd_cols["load_date"]

    def _exclude_cols(self) -> str:
        s = self.spec
        if s.fl_mn == "1":
            fixed = [
                "IDT_RGT_ATU", "DAT_PRI_VIG_RGT", "DAT_INI_VIG_RGT",
                "DAT_FIM_VIG_RGT", "DAT_CRG_RGT", "DAT_CRG_RGT_SNW", "NOM_JOB_CRG",
            ]
        else:
            fixed = [
                "RECORD_STATUS", "START_DATE", "END_DATE",
                "D_TIMESTAMP", "NOM_JOB_CRG",
            ]
        cols = [s.surrogate_key] + fixed + [s.business_key]
        return ", ".join(cols)

    # ── Queries ───────────────────────────────────────────────────────────────

    def _sql_snapshot_prod(self) -> str:
        s = self.spec
        dev = self._dev_table()
        prod = self._prod_table()
        ts = self._tt_timestamp()
        return (
            f"-- 01 — Snapshot PROD via Time Travel ({self.data_teste})\n"
            f"CREATE OR REPLACE TRANSIENT TABLE {dev} AS\n"
            f"SELECT *\n"
            f"FROM {prod}\n"
            f"    AT(TIMESTAMP => {ts});"
        )

    def _sql_count_dev_total(self) -> str:
        dev = self._dev_table()
        af = self._active_filter()
        return (
            f"-- 02 — COUNT DEV total\n"
            f"SELECT COUNT(*) AS QTD_DEV\n"
            f"FROM {dev}\n"
            f"WHERE {af};"
        )

    def _sql_count_prod_total(self) -> str:
        prod = self._prod_table()
        af = self._active_filter()
        return (
            f"-- 03 — COUNT PROD total\n"
            f"SELECT COUNT(*) AS QTD_PROD\n"
            f"FROM {prod}\n"
            f"WHERE {af};"
        )

    def _sql_count_dev_by_date(self) -> str:
        dev = self._dev_table()
        af = self._active_filter()
        dt = self._load_date_col()
        return (
            f"-- 04 — COUNT DEV por data de carga\n"
            f"SELECT CAST({dt} AS DATE) AS DAT_CARGA, COUNT(*) AS QTD\n"
            f"FROM {dev}\n"
            f"WHERE {af}\n"
            f"GROUP BY 1\n"
            f"ORDER BY 1 DESC\n"
            f"LIMIT {_HOMO_COUNT_LIMIT};"
        )

    def _sql_count_prod_by_date(self) -> str:
        prod = self._prod_table()
        af = self._active_filter()
        dt = self._load_date_col()
        return (
            f"-- 05 — COUNT PROD por data de carga\n"
            f"SELECT CAST({dt} AS DATE) AS DAT_CARGA, COUNT(*) AS QTD\n"
            f"FROM {prod}\n"
            f"WHERE {af}\n"
            f"GROUP BY 1\n"
            f"ORDER BY 1 DESC\n"
            f"LIMIT {_HOMO_COUNT_LIMIT};"
        )

    def _sql_minus(self) -> str:
        dev = self._dev_table()
        prod = self._prod_table()
        af = self._active_filter()
        excl = self._exclude_cols()
        return (
            f"-- 06 — MINUS DEV menos PROD\n"
            f"SELECT * EXCLUDE ({excl})\n"
            f"FROM {dev}\n"
            f"WHERE {af}\n"
            f"\n"
            f"MINUS\n"
            f"\n"
            f"SELECT * EXCLUDE ({excl})\n"
            f"FROM {prod}\n"
            f"WHERE {af}\n"
            f"\n"
            f"ORDER BY 1;"
        )

    def _sql_union_bsk(self) -> str:
        s = self.spec
        dev = self._dev_table()
        prod = self._prod_table()
        bsk = s.business_key
        scd = s.scd_cols
        return (
            f"-- 07 — Comparação DEV vs PROD para {bsk} = '{self.bsk_id}'\n"
            f"SELECT 'DEV' AS ORIGEM, *\n"
            f"FROM {dev}\n"
            f"WHERE {bsk} = '{self.bsk_id}'\n"
            f"\n"
            f"UNION ALL\n"
            f"\n"
            f"SELECT 'PROD' AS ORIGEM, *\n"
            f"FROM {prod}\n"
            f"WHERE {bsk} = '{self.bsk_id}'\n"
            f"\n"
            f"ORDER BY ORIGEM, {scd['start_date']} DESC;"
        )
