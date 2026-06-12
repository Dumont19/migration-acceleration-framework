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
    PROD_ORACLE_SCHEMA,
    PROD_SF_SCHEMA,
    SCD_FL_MN0_MAP,
    SCD_FL_MN1_MAP,
)


class OracleColumnInfo(BaseModel):
    """Metadados de uma coluna Oracle retornados por all_tab_columns.

    Attributes:
        name: Nome da coluna (COLUMN_NAME).
        data_type: Tipo Oracle (VARCHAR2, NUMBER, DATE, TIMESTAMP, etc.).
        data_length: Comprimento em bytes (para VARCHAR2/CHAR).
        data_precision: Precisão numérica (para NUMBER).
        data_scale: Escala numérica (para NUMBER/TIMESTAMP).
        nullable: True se a coluna aceita NULL.
        comment: Comentário da coluna (all_col_comments), ou None.
    """

    name: str
    data_type: str
    data_length: int | None = None
    data_precision: int | None = None
    data_scale: int | None = None
    nullable: bool = True
    comment: str | None = None


class OracleConstraintInfo(BaseModel):
    """Metadados de uma constraint Oracle retornados por all_constraints.

    Attributes:
        column_name: Coluna da constraint.
        constraint_type: Tipo da constraint ('P' = PK, 'U' = UNIQUE, 'C' = CHECK).
        constraint_name: Nome da constraint.
    """

    column_name: str
    constraint_type: str
    constraint_name: str


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
        output_col: Coluna de saída do lookup (surrogate key do DIM referenciado).
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
    gerar os 5 SQLs Snowflake e as queries de homologação.

    REGRA ABSOLUTA: colunas para DDL vêm do Oracle (oracle_columns),
    nunca do XML DataStage. Preencher oracle_columns antes de chamar o gerador.

    Attributes:
        job_name: Nome do job DataStage.
        target_table: Nome da tabela DIM no Snowflake.
        raw_table: Nome da tabela _RAW de staging.
        schema: Schema Snowflake destino (default DWDEV.MATHEUSDR).
        fl_mn: Padrão SCD2 — '1' (Marcia) ou '0' (legado).
        source_select: SELECT completo da ODS Oracle.
        source_schema: Schema da ODS Oracle.
        columns: Colunas com derivações DataStage (para SELECT da procedure).
        surrogate_key: Coluna de surrogate key.
        business_key: Coluna de business key primária (BSK_*).
        business_key2: Segunda business key opcional.
        business_key3: Terceira business key opcional.
        lookups: Lookups CHashedFileStage detectados.
        versiona_job_name: Nome passado para PRO_DW_VERSIONA (via AfterSQL).
        has_row_number: True se o transformer usa ROW_NUMBER/QUALIFY.
        nom_sis_ori: Valor de NOM_SIS_ORI / SRC_SYS_NAME.
        is_delta: True se o SELECT contém filtro de carga incremental.
        oracle_schema: Schema Oracle para buscar colunas (default DWADM).
        prod_sf_schema: Schema Snowflake de produção para Time Travel (default DWDEV.DWADM).
        oracle_columns: Colunas Oracle buscadas antes da geração (FONTE OBRIGATÓRIA).
        oracle_constraints: Constraints Oracle (PK/UNIQUE/CHECK) da tabela.
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
    business_key2: str | None = None
    business_key3: str | None = None

    lookups: list[LookupSpec] = Field(default_factory=list)

    versiona_job_name: str = ""
    has_row_number: bool = False

    nom_sis_ori: str = DEFAULT_NOM_SIS_ORI
    is_delta: bool = False

    oracle_schema: str = PROD_ORACLE_SCHEMA
    prod_sf_schema: str = PROD_SF_SCHEMA

    oracle_columns: list[OracleColumnInfo] = Field(default_factory=list)
    oracle_constraints: list[OracleConstraintInfo] = Field(default_factory=list)

    @property
    def scd_cols(self) -> dict[str, str]:
        """Retorna o mapeamento de papéis → nomes de colunas SCD2 para o fl_mn atual."""
        return SCD_FL_MN1_MAP if self.fl_mn == "1" else SCD_FL_MN0_MAP
