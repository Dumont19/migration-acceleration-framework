"""
services/dimension/schemas.py
-------------------------------
Tipos de dados para o pipeline de migração de dimensões SCD2.
Sem imports de FastAPI — puro Pydantic.
"""
from __future__ import annotations

from pydantic import BaseModel, Field

from app.core.constants import (
    DEFAULT_BUSINESS_KEY,
    DEFAULT_DIM_SCHEMA,
    DEFAULT_NOM_SIS_ORI,
    DEFAULT_SRC_SCHEMA,
    DEFAULT_SURROGATE_KEY,
    SCD_FL_MN0_MAP,
    SCD_FL_MN1_MAP,
)


class ColumnSpec(BaseModel):
    """Especificação de uma coluna do transformer DataStage.

    Attributes:
        name: Nome da coluna no Snowflake.
        derivation: Expressão SQL derivada do transformer DataStage.
        is_scd_col: True se a coluna é uma coluna SCD2 de controle.
    """

    name: str
    derivation: str
    is_scd_col: bool = False


class LookupSpec(BaseModel):
    """Especificação de um lookup CHashedFileStage convertido em LEFT JOIN.

    Attributes:
        stage_name: Nome do stage no DataStage.
        hash_key: Coluna de hash usada para join.
        lookup_table: Tabela Snowflake referenciada.
        join_keys: Colunas de join (até 2).
        output_col: Coluna de saída do lookup.
        default_value: Valor padrão no COALESCE quando não há match (default -1).
    """

    stage_name: str
    hash_key: str
    lookup_table: str
    join_keys: list[str] = Field(default_factory=list)
    output_col: str
    default_value: str = "-1"


class DimensionSpec(BaseModel):
    """Especificação completa de um job de dimensão SCD2.

    Contém todos os metadados extraídos do DSX DataStage necessários para
    gerar os 6 SQLs Snowflake e as queries de homologação.

    Attributes:
        job_name: Nome do job DataStage (usado no PRO_DW_VERSIONA).
        target_table: Nome da tabela DIM no Snowflake.
        raw_table: Nome da tabela _RAW de staging.
        schema: Schema Snowflake (default DWDEV.MATHEUSDR).
        fl_mn: Padrão SCD2 — '1' (Marcia) ou '0' (legado).
        source_select: SELECT completo da ODS Oracle.
        source_schema: Schema da ODS Oracle.
        columns: Lista de colunas com derivações.
        surrogate_key: Coluna de surrogate key (via SEQUENCE).
        business_key: Coluna de business key (BSK_*).
        lookups: Lookups CHashedFileStage detectados.
        versiona_job_name: Nome passado para PRO_DW_VERSIONA (via AfterSQL).
        has_row_number: True se o transformer usa ROW_NUMBER/QUALIFY.
        nom_sis_ori: Valor de NOM_SIS_ORI / SRC_SYS_NAME.
        is_delta: True se o SELECT contém filtro de carga incremental.
    """

    job_name: str
    target_table: str
    raw_table: str
    schema: str = DEFAULT_DIM_SCHEMA

    fl_mn: str = Field("1", pattern="^[01]$")

    source_select: str
    source_schema: str = DEFAULT_SRC_SCHEMA

    columns: list[ColumnSpec] = Field(default_factory=list)
    surrogate_key: str = DEFAULT_SURROGATE_KEY
    business_key: str = DEFAULT_BUSINESS_KEY

    lookups: list[LookupSpec] = Field(default_factory=list)

    versiona_job_name: str = ""
    has_row_number: bool = False

    nom_sis_ori: str = DEFAULT_NOM_SIS_ORI
    is_delta: bool = False

    @property
    def scd_cols(self) -> dict[str, str]:
        """Retorna o mapeamento de papéis → nomes de colunas SCD2 para o fl_mn atual."""
        return SCD_FL_MN1_MAP if self.fl_mn == "1" else SCD_FL_MN0_MAP
