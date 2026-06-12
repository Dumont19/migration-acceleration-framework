"""
services/dimension/schemas.py
-------------------------------
Tipos de dados para o pipeline de migração de dimensões SCD2.
Sem imports de FastAPI — puro Pydantic.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ColumnSpec(BaseModel):
    name: str
    derivation: str
    is_scd_col: bool = False


class LookupSpec(BaseModel):
    stage_name: str
    hash_key: str
    lookup_table: str
    join_keys: list[str] = Field(default_factory=list)
    output_col: str
    default_value: str = "-1"


class DimensionSpec(BaseModel):
    job_name: str
    target_table: str
    raw_table: str
    schema: str = "DWDEV.MATHEUSDR"

    fl_mn: str = Field("1", pattern="^[01]$")

    source_select: str
    source_schema: str = "DWDEV"

    columns: list[ColumnSpec] = Field(default_factory=list)
    surrogate_key: str
    business_key: str

    lookups: list[LookupSpec] = Field(default_factory=list)

    versiona_job_name: str = ""
    has_row_number: bool = False

    nom_sis_ori: str = "ALGAR SOM"
    is_delta: bool = False

    @property
    def scd_cols(self) -> dict[str, str]:
        """Retorna os nomes das colunas SCD2 de acordo com fl_mn."""
        if self.fl_mn == "1":
            return {
                "active_flag": "IDT_RGT_ATU",
                "start_date": "DAT_INI_VIG_RGT",
                "sys_name": "NOM_SIS_ORI",
                "load_date": "DAT_CAR_RGT",
            }
        return {
            "active_flag": "RECORD_STATUS",
            "start_date": "START_DATE",
            "sys_name": "SRC_SYS_NAME",
            "load_date": "D_TIMESTAMP",
        }
