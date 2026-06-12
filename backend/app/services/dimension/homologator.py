"""
services/dimension/homologator.py
-----------------------------------
DimensionHomologator — gera queries de validação MINUS para
homologação DEV vs PROD de tabelas de dimensão SCD2.
"""
from __future__ import annotations

from .schemas import DimensionSpec


class DimensionHomologator:
    """
    Gera queries de homologação para comparar DEV vs PROD.

    Retorna um dicionário com queries prontas para execução manual
    no Snowflake Worksheet.
    """

    def __init__(self, spec: DimensionSpec, prod_table: str, time_travel_offset: int = 0) -> None:
        self.spec = spec
        self.prod_table = prod_table.upper()
        self.time_travel_offset = time_travel_offset  # segundos negativos, ex: -3600

    def generate(self) -> dict[str, str]:
        return {
            "minus_dev_vs_prod": self._minus_query(),
            "count_by_date": self._count_query(),
            "field_divergence": self._divergence_query(),
        }

    # ── Queries ───────────────────────────────────────────────────────────────

    def _minus_query(self) -> str:
        s = self.spec
        scd = s.scd_cols
        dev_table = f"{s.schema}.{s.target_table}"
        prod_ref = self._prod_ref()
        biz_cols = self._biz_column_list()

        return f"""-- MINUS: registros em DEV não presentes em PROD (mesmo snapshot)
-- Diferenças indicam linhas novas ou atualizadas em DEV que não existem em PROD
SELECT {biz_cols}
FROM {dev_table}
WHERE {scd['active_flag']} = 1
  AND {scd['sys_name']} = '{s.nom_sis_ori}'

MINUS

SELECT {biz_cols}
FROM {prod_ref}
WHERE {scd['active_flag']} = 1
  AND {scd['sys_name']} = '{s.nom_sis_ori}'

ORDER BY 1;"""

    def _count_query(self) -> str:
        s = self.spec
        scd = s.scd_cols
        dev_table = f"{s.schema}.{s.target_table}"
        prod_ref = self._prod_ref()
        date_col = scd["load_date"]

        return f"""-- Contagem por data de carga: DEV vs PROD
SELECT
    CAST(D.{date_col} AS DATE)    AS DAT_CARGA,
    COUNT(*)                       AS QTD_DEV,
    COALESCE(P.QTD_PROD, 0)       AS QTD_PROD,
    COUNT(*) - COALESCE(P.QTD_PROD, 0) AS DIFERENCA
FROM {dev_table} D
LEFT JOIN (
    SELECT CAST({date_col} AS DATE) AS DAT_CARGA, COUNT(*) AS QTD_PROD
    FROM {prod_ref}
    WHERE {scd['active_flag']} = 1
    GROUP BY 1
) P ON CAST(D.{date_col} AS DATE) = P.DAT_CARGA
WHERE D.{scd['active_flag']} = 1
GROUP BY 1, 3
ORDER BY 1 DESC
LIMIT 90;"""

    def _divergence_query(self) -> str:
        s = self.spec
        scd = s.scd_cols
        dev_table = f"{s.schema}.{s.target_table}"
        prod_ref = self._prod_ref()
        bsk = s.business_key

        # Pega as primeiras 5 colunas de negócio para comparação
        biz_cols = [
            c for c in s.columns
            if not c.is_scd_col and c.name.upper() != s.surrogate_key.upper()
        ][:5]

        col_comparisons = "\n    OR ".join(
            f"D.{c.name} IS DISTINCT FROM P.{c.name}" for c in biz_cols
        ) or "1=1"

        select_cols = "\n    ".join(
            f"D.{c.name} AS DEV_{c.name},\n    P.{c.name} AS PROD_{c.name},"
            for c in biz_cols
        )

        return f"""-- Divergência de campos específicos entre DEV e PROD
-- Útil para debug após MINUS revelar diferenças
SELECT
    D.{bsk}          AS BSK,
    {select_cols}
    D.{scd['load_date']} AS DEV_DAT_CARGA,
    P.{scd['load_date']} AS PROD_DAT_CARGA
FROM {dev_table} D
JOIN {prod_ref} P
  ON D.{bsk} IS NOT DISTINCT FROM P.{bsk}
 AND D.{scd['active_flag']} = 1
 AND P.{scd['active_flag']} = 1
WHERE {col_comparisons}
ORDER BY D.{bsk}
LIMIT 500;"""

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _prod_ref(self) -> str:
        if self.time_travel_offset and self.time_travel_offset < 0:
            return f"{self.prod_table} AT (OFFSET => {self.time_travel_offset})"
        return self.prod_table

    def _biz_column_list(self) -> str:
        s = self.spec
        biz_cols = [
            c.name for c in s.columns
            if not c.is_scd_col and c.name.upper() != s.surrogate_key.upper()
        ]
        return ", ".join(biz_cols) if biz_cols else "*"
