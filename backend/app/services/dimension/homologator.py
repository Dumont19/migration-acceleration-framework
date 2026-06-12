"""
services/dimension/homologator.py
-----------------------------------
DimensionHomologator — gera queries de validação MINUS para
homologação DEV vs PROD de tabelas de dimensão SCD2.
"""
from __future__ import annotations

from app.core.constants import (
    HOMO_COUNT_LIMIT,
    HOMO_DIVERGENCE_COL_SAMPLE,
    HOMO_DIVERGENCE_LIMIT,
)
from .schemas import DimensionSpec


class DimensionHomologator:
    """Gera queries de homologação para comparar DEV vs PROD.

    Produz três queries prontas para execução manual no Snowflake Worksheet:
    - MINUS DEV vs PROD para detectar linhas novas ou modificadas
    - Contagem por data de carga para comparar volumes
    - Divergência de campos específicos para debug pós-MINUS

    Suporta Snowflake Time Travel: se ``time_travel_offset < 0``, a tabela
    PROD é referenciada com ``AT (OFFSET => N)``.

    Attributes:
        spec: Especificação do job de dimensão.
        prod_table: Referência completa da tabela PROD (schema.tabela).
        time_travel_offset: Offset em segundos para Time Travel (≤ 0).
    """

    def __init__(
        self,
        spec: DimensionSpec,
        prod_table: str,
        time_travel_offset: int = 0,
    ) -> None:
        """Inicializa o homologador.

        Args:
            spec: Especificação completa do job de dimensão.
            prod_table: Tabela PROD no formato ``SCHEMA.TABELA``.
            time_travel_offset: Offset em segundos para Time Travel (ex: -3600 = 1h atrás).
                                 0 ou positivo desativa o Time Travel.
        """
        self.spec = spec
        self.prod_table = prod_table.upper()
        self.time_travel_offset = time_travel_offset

    def generate(self) -> dict[str, str]:
        """Gera as três queries de homologação.

        Returns:
            Dicionário com chaves ``minus_dev_vs_prod``, ``count_by_date``,
            ``field_divergence``, valores = SQL pronto para execução.
        """
        return {
            "minus_dev_vs_prod": self._minus_query(),
            "count_by_date":     self._count_query(),
            "field_divergence":  self._divergence_query(),
        }

    # ── Queries ───────────────────────────────────────────────────────────────

    def _minus_query(self) -> str:
        """Gera query MINUS: registros em DEV não presentes em PROD.

        Filtra apenas registros ativos (active_flag = 1) com o sistema de origem correto.

        Returns:
            SQL MINUS ordenado pela primeira coluna.
        """
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
        """Gera query de contagem por data de carga: DEV vs PROD.

        Retorna até :data:`HOMO_COUNT_LIMIT` datas mais recentes, ordenadas desc.

        Returns:
            SQL com colunas DAT_CARGA, QTD_DEV, QTD_PROD, DIFERENCA.
        """
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
LIMIT {HOMO_COUNT_LIMIT};"""

    def _divergence_query(self) -> str:
        """Gera query de divergência de campos específicos entre DEV e PROD.

        Compara as primeiras :data:`HOMO_DIVERGENCE_COL_SAMPLE` colunas de negócio
        usando ``IS DISTINCT FROM`` (correto para NULL). Retorna até
        :data:`HOMO_DIVERGENCE_LIMIT` linhas divergentes.

        Returns:
            SQL JOIN DEV vs PROD com colunas lado a lado.
        """
        s = self.spec
        scd = s.scd_cols
        dev_table = f"{s.schema}.{s.target_table}"
        prod_ref = self._prod_ref()
        bsk = s.business_key

        biz_cols = [
            c for c in s.columns
            if not c.is_scd_col and c.name.upper() != s.surrogate_key.upper()
        ][:HOMO_DIVERGENCE_COL_SAMPLE]

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
LIMIT {HOMO_DIVERGENCE_LIMIT};"""

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _prod_ref(self) -> str:
        """Retorna a referência da tabela PROD, com Time Travel se aplicável.

        Returns:
            Nome da tabela PROD com ou sem cláusula ``AT (OFFSET => N)``.
        """
        if self.time_travel_offset and self.time_travel_offset < 0:
            return f"{self.prod_table} AT (OFFSET => {self.time_travel_offset})"
        return self.prod_table

    def _biz_column_list(self) -> str:
        """Retorna lista de colunas de negócio (sem surrogate e sem SCD2) para o MINUS.

        Returns:
            Colunas separadas por vírgula, ou ``'*'`` se não houver colunas.
        """
        s = self.spec
        biz_cols = [
            c.name for c in s.columns
            if not c.is_scd_col and c.name.upper() != s.surrogate_key.upper()
        ]
        return ", ".join(biz_cols) if biz_cols else "*"
