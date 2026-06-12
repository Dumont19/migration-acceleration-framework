"""
services/dimension/extractor.py
---------------------------------
DimensionSpecExtractor — extrai metadados de um arquivo DSX DataStage
e os converte em um DimensionSpec para geração de SQL Snowflake.
"""
from __future__ import annotations

import html
import re
from pathlib import Path

from bs4 import BeautifulSoup

from app.core.logging import get_logger
from .schemas import ColumnSpec, DimensionSpec, LookupSpec

logger = get_logger(__name__)

# Colunas SCD2 padrão fl_mn=1 (Marcia) — usadas para detectar fl_mn
_FL_MN1_MARKERS = {"IDT_RGT_ATU", "DAT_INI_VIG_RGT", "NOM_SIS_ORI", "DAT_CAR_RGT"}
# Colunas SCD2 padrão fl_mn=0 (legacy)
_FL_MN0_MARKERS = {"RECORD_STATUS", "START_DATE", "SRC_SYS_NAME", "D_TIMESTAMP"}

_IGNORED_LINK_PREFIXES = ("rowrej", "nullset", "intervar", "stagevar")
_SURROGATE_PATTERNS = re.compile(
    r"\b(SEQ_\w+\.NEXTVAL|NEXTVAL\s+FOR\s+\w+|\w+\.NEXTVAL)\b", re.IGNORECASE
)
_BSK_PATTERN = re.compile(r"\b(BSK_\w+|TABLE_SRC_KEY)\b", re.IGNORECASE)


class DimensionSpecExtractor:
    """
    Extrai um DimensionSpec a partir de um arquivo .dsx DataStage.

    Uso:
        spec = DimensionSpecExtractor.from_file(Path("job.dsx"))
        # ou
        spec = DimensionSpecExtractor.from_string(xml_content)
    """

    def __init__(self, content: str) -> None:
        self._content = content
        self._soup = BeautifulSoup(content, "xml")

    @classmethod
    def from_file(cls, path: Path) -> "DimensionSpecExtractor":
        content = path.read_text(encoding="utf-8", errors="ignore")
        return cls(content)

    @classmethod
    def from_string(cls, content: str) -> "DimensionSpecExtractor":
        return cls(content)

    def extract(self) -> DimensionSpec:
        job_name = self._find_job_name()
        logger.info("Extracting DimensionSpec", job=job_name)

        target_table, raw_table, schema = self._find_target_table()
        source_select = self._find_source_select()
        columns, fl_mn, surrogate_key, business_key = self._parse_transformer()
        lookups = self._find_lookups()
        versiona_job_name = self._find_versiona_job(job_name)
        has_row_number = self._detect_row_number()
        nom_sis_ori = self._detect_nom_sis_ori()

        # Delta: heurística — se o SELECT contiver filtro de data incremental
        is_delta = bool(
            re.search(r"LAST_LOAD|DAT_ULT|DELTA|INCREMENTAL", source_select, re.IGNORECASE)
        )

        spec = DimensionSpec(
            job_name=job_name,
            target_table=target_table,
            raw_table=raw_table,
            schema=schema,
            fl_mn=fl_mn,
            source_select=source_select,
            columns=columns,
            surrogate_key=surrogate_key,
            business_key=business_key,
            lookups=lookups,
            versiona_job_name=versiona_job_name,
            has_row_number=has_row_number,
            nom_sis_ori=nom_sis_ori,
            is_delta=is_delta,
        )
        logger.info(
            "DimensionSpec extracted",
            job=job_name,
            table=target_table,
            fl_mn=fl_mn,
            columns=len(columns),
            lookups=len(lookups),
        )
        return spec

    # ── Private helpers ──────────────────────────────────────────────────────

    def _find_job_name(self) -> str:
        for elem in self._soup.find_all(["Job", "Record"]):
            if elem.name == "Job" or elem.get("Type") == "JobDefn":
                p = elem.find("Property", attrs={"Name": "Name"})
                if p and p.text.strip():
                    return p.text.strip()
        return "UNKNOWN_JOB"

    def _find_target_table(self) -> tuple[str, str, str]:
        """Encontra tabela alvo e schema. Retorna (table, raw_table, schema)."""
        # Procura OracleConnector/SnowflakeConnector com WriteMode >= 0
        for record in self._soup.find_all("Record"):
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not p_type:
                continue
            stage_type = p_type.text.strip()
            if stage_type not in (
                "OracleConnectorPX", "OracleConnector", "SnowflakeConnectorPX",
                "PxOracleConnector",
            ):
                continue

            xml_val = self._get_xml_properties(record)
            if not xml_val:
                continue

            wm = re.search(r"<WriteMode[^>]*>\s*<!\[CDATA\[(\d+)\]\]>", xml_val, re.IGNORECASE)
            if not wm or int(wm.group(1)) < 0:
                continue

            m_table = re.search(
                r"<TableName[^>]*>\s*<!\[CDATA\[([^\]]+)\]\]>", xml_val, re.IGNORECASE
            )
            if m_table:
                full_name = m_table.group(1).strip()
                parts = full_name.upper().split(".")
                table = parts[-1]
                schema_str = ".".join(parts[:-1]) if len(parts) > 1 else "DWDEV.MATHEUSDR"
                raw_table = f"{table}_RAW"
                return table, raw_table, schema_str

        return "UNKNOWN_TABLE", "UNKNOWN_TABLE_RAW", "DWDEV.MATHEUSDR"

    def _find_source_select(self) -> str:
        """Extrai o SELECT completo da fonte ODS (primeiro OracleConnector em modo leitura)."""
        for record in self._soup.find_all("Record"):
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not p_type:
                continue
            if p_type.text.strip() not in (
                "OracleConnectorPX", "OracleConnector", "PxOracleConnector",
            ):
                continue

            xml_val = self._get_xml_properties(record)
            if not xml_val:
                continue

            wm = re.search(r"<WriteMode[^>]*>\s*<!\[CDATA\[(\d+)\]\]>", xml_val, re.IGNORECASE)
            if wm and int(wm.group(1)) >= 0:
                continue  # É write — pula

            m_sel = re.search(
                r"<SelectStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>",
                xml_val,
                re.IGNORECASE | re.DOTALL,
            )
            if m_sel:
                return re.sub(r"\s+", " ", m_sel.group(1).strip())

        return ""

    def _parse_transformer(
        self,
    ) -> tuple[list[ColumnSpec], str, str, str]:
        """
        Analisa CTransformerStage e extrai:
        - lista de colunas com derivações
        - fl_mn detectado
        - surrogate_key
        - business_key
        """
        columns: list[ColumnSpec] = []
        surrogate_key = "TABLE_KEY"
        business_key = "TABLE_SRC_KEY"
        fl_mn = "1"  # padrão

        scd_cols_found: set[str] = set()

        for record in self._soup.find_all("Record"):
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not p_type or p_type.text.strip() != "CTransformerStage":
                continue

            trx_code = self._get_trx_gen_code(record)
            if not trx_code:
                continue

            assign_pat = re.compile(
                r"^\s*(\w+)\.(\w+)\s*=\s*([^;\n]+(?:\([^)]*\))?[^;\n]*);",
                re.MULTILINE,
            )

            col_exprs: dict[str, list[str]] = {}
            col_order: list[str] = []

            for m in assign_pat.finditer(trx_code):
                link, col, expr = m.group(1), m.group(2), m.group(3).strip()
                if link.lower().startswith(_IGNORED_LINK_PREFIXES):
                    continue
                expr_clean = re.sub(r"\s+", " ", expr).strip()
                if col not in col_exprs:
                    col_exprs[col] = []
                    col_order.append(col)
                if expr_clean not in col_exprs[col]:
                    col_exprs[col].append(expr_clean)

            for col in col_order:
                exprs = col_exprs[col]
                deriv = " | ".join(exprs) if len(exprs) > 1 else (exprs[0] if exprs else col)

                # Detectar coluna surrogate (SEQUENCE)
                if _SURROGATE_PATTERNS.search(deriv):
                    surrogate_key = col
                # Detectar business key
                if _BSK_PATTERN.search(col) or _BSK_PATTERN.search(deriv):
                    business_key = col

                # Detectar colunas SCD
                col_up = col.upper()
                if col_up in _FL_MN1_MARKERS or col_up in _FL_MN0_MARKERS:
                    scd_cols_found.add(col_up)

                is_scd = col_up in _FL_MN1_MARKERS or col_up in _FL_MN0_MARKERS
                columns.append(ColumnSpec(name=col, derivation=deriv, is_scd_col=is_scd))

        # Detectar fl_mn pela presença das colunas SCD
        if scd_cols_found & _FL_MN1_MARKERS:
            fl_mn = "1"
        elif scd_cols_found & _FL_MN0_MARKERS:
            fl_mn = "0"

        return columns, fl_mn, surrogate_key, business_key

    def _find_lookups(self) -> list[LookupSpec]:
        """Converte CHashedFileStage em LookupSpec (LEFT JOIN com COALESCE)."""
        lookups: list[LookupSpec] = []
        for record in self._soup.find_all("Record"):
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not p_type or p_type.text.strip() != "CHashedFileStage":
                continue

            p_name = record.find("Property", attrs={"Name": "StageName"})
            stage_name = p_name.text.strip() if p_name else "LOOKUP"

            # Tentar extrair tabela do hash key
            p_table = record.find("Property", attrs={"Name": "TableName"})
            lookup_table = p_table.text.strip() if p_table else stage_name

            p_key = record.find("Property", attrs={"Name": "HashKey"})
            hash_key = p_key.text.strip() if p_key else ""

            # Colunas de junção e saída
            join_keys: list[str] = []
            output_col = stage_name

            for sub in record.find_all("SubRecord"):
                cn = sub.find("Property", attrs={"Name": "Name"})
                if cn and cn.text.strip():
                    join_keys.append(cn.text.strip())

            lookups.append(
                LookupSpec(
                    stage_name=stage_name,
                    hash_key=hash_key,
                    lookup_table=lookup_table,
                    join_keys=join_keys[:2],
                    output_col=output_col,
                    default_value="-1",
                )
            )
        return lookups

    def _find_versiona_job(self, job_name: str) -> str:
        """Extrai o nome do job passado para PRO_DW_VERSIONA no AfterSQL."""
        for record in self._soup.find_all("Record"):
            xml_val = self._get_xml_properties(record)
            if not xml_val:
                continue
            m = re.search(
                r"<AfterSQL[^>]*>\s*<!\[CDATA\[(.+?)\]\]>",
                xml_val,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                after = m.group(1)
                # Procura CALL PRO_DW_VERSIONA(..., 'NOME_JOB', ...)
                mj = re.search(
                    r"PRO_DW_VERSIONA\s*\([^)]*'([^']+)'", after, re.IGNORECASE
                )
                if mj:
                    return mj.group(1).strip()
        return job_name

    def _detect_row_number(self) -> bool:
        """Detecta se há lógica de deduplicação via ROW_NUMBER/QUALIFY no XML."""
        content_upper = self._content.upper()
        return "ROW_NUMBER" in content_upper or "QUALIFY" in content_upper

    def _detect_nom_sis_ori(self) -> str:
        """Detecta o valor de NOM_SIS_ORI/SRC_SYS_NAME no transformer."""
        m = re.search(
            r"(?:NOM_SIS_ORI|SRC_SYS_NAME)\s*=\s*['\"]([^'\"]+)['\"]",
            self._content,
            re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        # Heurística para jobs SOM
        if re.search(r"\bSOM\b", self._content, re.IGNORECASE):
            return "ALGAR SOM"
        return "ALGAR SOM"

    def _get_xml_properties(self, record) -> str | None:
        for sub in record.find_all("SubRecord"):
            name_prop = sub.find("Property", attrs={"Name": "Name"})
            val_prop = sub.find("Property", attrs={"Name": "Value"})
            if name_prop and val_prop and name_prop.text == "XMLProperties":
                return html.unescape(val_prop.text or "")
        for prop in record.find_all("Property"):
            if prop.get("Name") == "Value" and prop.text and "SelectStatement" in prop.text:
                return html.unescape(prop.text)
        return None

    def _get_trx_gen_code(self, record) -> str | None:
        for sub in record.find_all("SubRecord"):
            name_prop = sub.find("Property", attrs={"Name": "Name"})
            val_prop = sub.find("Property", attrs={"Name": "Value"})
            if name_prop and val_prop and name_prop.text.strip() == "TrxGenCode":
                return val_prop.text or ""
        return None
