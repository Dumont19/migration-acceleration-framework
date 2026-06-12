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

from bs4 import BeautifulSoup, Tag

from app.core.constants import (
    DEFAULT_BUSINESS_KEY,
    DEFAULT_DIM_SCHEMA,
    DEFAULT_NOM_SIS_ORI,
    DEFAULT_SURROGATE_KEY,
    IGNORED_LINK_PREFIXES,
    SCD_FL_MN0_COLS,
    SCD_FL_MN1_COLS,
)
from app.core.logging import get_logger
from .schemas import ColumnSpec, DimensionSpec, LookupSpec

logger = get_logger(__name__)

_SURROGATE_PATTERNS = re.compile(
    r"\b(SEQ_\w+\.NEXTVAL|NEXTVAL\s+FOR\s+\w+|\w+\.NEXTVAL)\b", re.IGNORECASE
)
_BSK_PATTERN = re.compile(r"\b(BSK_\w+|TABLE_SRC_KEY)\b", re.IGNORECASE)
_DELTA_PATTERN = re.compile(r"LAST_LOAD|DAT_ULT|DELTA|INCREMENTAL", re.IGNORECASE)


class DimensionSpecExtractor:
    """Extrai um :class:`DimensionSpec` a partir de um arquivo .dsx DataStage.

    Analisa o XML/DSX gerado pelo DataStage 11.5 e infere:
    - Nome do job e tabela alvo
    - fl_mn pela presença das colunas SCD2
    - SELECT completo da ODS Oracle
    - Derivações do transformer (CTransformerStage)
    - Surrogate key via padrão SEQUENCE.NEXTVAL
    - Business key via prefixo BSK_
    - Lookups CHashedFileStage → LookupSpec
    - Nome do job passado ao PRO_DW_VERSIONA no AfterSQL

    Exemplo::

        spec = DimensionSpecExtractor.from_file(Path("job.dsx")).extract()
        spec = DimensionSpecExtractor.from_string(xml_content).extract()
    """

    def __init__(self, content: str) -> None:
        """Inicializa o extrator com o conteúdo XML/DSX.

        Args:
            content: Conteúdo textual do arquivo DSX ou XML exportado do DataStage.
        """
        self._content = content
        self._soup = BeautifulSoup(content, "xml")

    @classmethod
    def from_file(cls, path: Path) -> "DimensionSpecExtractor":
        """Cria um extrator a partir de um arquivo no disco.

        Args:
            path: Caminho para o arquivo .dsx ou .xml.

        Returns:
            Instância pronta para chamar :meth:`extract`.
        """
        content = path.read_text(encoding="utf-8", errors="ignore")
        return cls(content)

    @classmethod
    def from_string(cls, content: str) -> "DimensionSpecExtractor":
        """Cria um extrator a partir de uma string XML.

        Args:
            content: Conteúdo XML/DSX como string.

        Returns:
            Instância pronta para chamar :meth:`extract`.
        """
        return cls(content)

    def extract(self) -> DimensionSpec:
        """Executa a extração completa e retorna um :class:`DimensionSpec`.

        Returns:
            Spec preenchido com todos os metadados extraídos do DSX.
        """
        job_name = self._find_job_name()
        logger.info("Extracting DimensionSpec", job=job_name)

        target_table, raw_table, schema = self._find_target_table()
        source_select = self._find_source_select()
        columns, fl_mn, surrogate_key, business_key = self._parse_transformer()
        lookups = self._find_lookups()
        versiona_job_name = self._find_versiona_job(job_name)
        has_row_number = self._detect_row_number()
        nom_sis_ori = self._detect_nom_sis_ori()

        is_delta = bool(_DELTA_PATTERN.search(source_select))

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
        """Extrai o nome do job DataStage do XML.

        Returns:
            Nome do job ou ``'UNKNOWN_JOB'`` se não encontrado.
        """
        for elem in self._soup.find_all(["Job", "Record"]):
            if not isinstance(elem, Tag):
                continue
            if elem.name == "Job" or elem.get("Type") == "JobDefn":
                p = elem.find("Property", attrs={"Name": "Name"})
                if isinstance(p, Tag) and p.text.strip():
                    return p.text.strip()
        return "UNKNOWN_JOB"

    def _find_target_table(self) -> tuple[str, str, str]:
        """Localiza a tabela alvo buscando conectores com modo de escrita.

        Inspeciona OracleConnectorPX e SnowflakeConnectorPX com WriteMode >= 0.

        Returns:
            Tupla (table, raw_table, schema). Valores padrão se não encontrado.
        """
        for record in self._soup.find_all("Record"):
            if not isinstance(record, Tag):
                continue
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not isinstance(p_type, Tag):
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
                schema_str = ".".join(parts[:-1]) if len(parts) > 1 else DEFAULT_DIM_SCHEMA
                return table, f"{table}_RAW", schema_str

        return "UNKNOWN_TABLE", "UNKNOWN_TABLE_RAW", DEFAULT_DIM_SCHEMA

    def _find_source_select(self) -> str:
        """Extrai o SELECT completo da fonte ODS (primeiro conector Oracle em modo leitura).

        Returns:
            SELECT como string única (espaços normalizados), ou ``''`` se não encontrado.
        """
        for record in self._soup.find_all("Record"):
            if not isinstance(record, Tag):
                continue
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not isinstance(p_type, Tag):
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
                continue  # conector de escrita — ignorar

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
        """Analisa CTransformerStage e extrai colunas, fl_mn, surrogate e business key.

        Varre o código gerado (TrxGenCode) linha a linha usando regex de atribuição.
        Links com prefixos em :data:`IGNORED_LINK_PREFIXES` são descartados.

        Returns:
            Tupla (columns, fl_mn, surrogate_key, business_key).
        """
        columns: list[ColumnSpec] = []
        surrogate_key = DEFAULT_SURROGATE_KEY
        business_key = DEFAULT_BUSINESS_KEY
        fl_mn = "1"

        scd_cols_found: set[str] = set()

        for record in self._soup.find_all("Record"):
            if not isinstance(record, Tag):
                continue
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not isinstance(p_type, Tag) or p_type.text.strip() != "CTransformerStage":
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
                if link.lower().startswith(IGNORED_LINK_PREFIXES):
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

                if _SURROGATE_PATTERNS.search(deriv):
                    surrogate_key = col
                if _BSK_PATTERN.search(col) or _BSK_PATTERN.search(deriv):
                    business_key = col

                col_up = col.upper()
                if col_up in SCD_FL_MN1_COLS or col_up in SCD_FL_MN0_COLS:
                    scd_cols_found.add(col_up)

                is_scd = col_up in SCD_FL_MN1_COLS or col_up in SCD_FL_MN0_COLS
                columns.append(ColumnSpec(name=col, derivation=deriv, is_scd_col=is_scd))

        if scd_cols_found & SCD_FL_MN1_COLS:
            fl_mn = "1"
        elif scd_cols_found & SCD_FL_MN0_COLS:
            fl_mn = "0"

        return columns, fl_mn, surrogate_key, business_key

    def _find_lookups(self) -> list[LookupSpec]:
        """Converte stages CHashedFileStage em :class:`LookupSpec` (LEFT JOIN com COALESCE).

        Returns:
            Lista de lookups detectados no DSX.
        """
        lookups: list[LookupSpec] = []
        for record in self._soup.find_all("Record"):
            if not isinstance(record, Tag):
                continue
            p_type = record.find("Property", attrs={"Name": "StageType"})
            if not isinstance(p_type, Tag) or p_type.text.strip() != "CHashedFileStage":
                continue

            p_name = record.find("Property", attrs={"Name": "StageName"})
            stage_name = p_name.text.strip() if isinstance(p_name, Tag) else "LOOKUP"

            p_table = record.find("Property", attrs={"Name": "TableName"})
            lookup_table = p_table.text.strip() if isinstance(p_table, Tag) else stage_name

            p_key = record.find("Property", attrs={"Name": "HashKey"})
            hash_key = p_key.text.strip() if isinstance(p_key, Tag) else ""

            join_keys: list[str] = []
            for sub in record.find_all("SubRecord"):
                if not isinstance(sub, Tag):
                    continue
                cn = sub.find("Property", attrs={"Name": "Name"})
                if isinstance(cn, Tag) and cn.text.strip():
                    join_keys.append(cn.text.strip())

            lookups.append(
                LookupSpec(
                    stage_name=stage_name,
                    hash_key=hash_key,
                    lookup_table=lookup_table,
                    join_keys=join_keys[:2],
                    output_col=stage_name,
                    default_value="-1",
                )
            )
        return lookups

    def _find_versiona_job(self, job_name: str) -> str:
        """Extrai o nome do job passado para PRO_DW_VERSIONA no AfterSQL.

        Args:
            job_name: Nome do job DataStage usado como fallback.

        Returns:
            Nome encontrado no AfterSQL, ou ``job_name`` se ausente.
        """
        for record in self._soup.find_all("Record"):
            if not isinstance(record, Tag):
                continue
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
                mj = re.search(
                    r"PRO_DW_VERSIONA\s*\([^)]*'([^']+)'", after, re.IGNORECASE
                )
                if mj:
                    return mj.group(1).strip()
        return job_name

    def _detect_row_number(self) -> bool:
        """Detecta lógica de deduplicação via ROW_NUMBER/QUALIFY no conteúdo do XML.

        Returns:
            True se ROW_NUMBER ou QUALIFY estiver presente no XML.
        """
        content_upper = self._content.upper()
        return "ROW_NUMBER" in content_upper or "QUALIFY" in content_upper

    def _detect_nom_sis_ori(self) -> str:
        """Detecta o valor de NOM_SIS_ORI ou SRC_SYS_NAME no transformer.

        Primeiro tenta encontrar a atribuição explícita no XML. Caso não encontre,
        aplica heurística: presença da palavra SOM indica 'ALGAR SOM'.

        Returns:
            Valor detectado, ou :data:`DEFAULT_NOM_SIS_ORI` como fallback.
        """
        m = re.search(
            r"(?:NOM_SIS_ORI|SRC_SYS_NAME)\s*=\s*['\"]([^'\"]+)['\"]",
            self._content,
            re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        return DEFAULT_NOM_SIS_ORI

    def _get_xml_properties(self, record: Tag) -> str | None:
        """Extrai o bloco XMLProperties de um Record DataStage.

        Args:
            record: Tag BeautifulSoup representando um Record DataStage.

        Returns:
            Conteúdo XML do bloco de propriedades, ou ``None`` se ausente.
        """
        for sub in record.find_all("SubRecord"):
            if not isinstance(sub, Tag):
                continue
            name_prop = sub.find("Property", attrs={"Name": "Name"})
            val_prop = sub.find("Property", attrs={"Name": "Value"})
            if (
                isinstance(name_prop, Tag)
                and isinstance(val_prop, Tag)
                and name_prop.text == "XMLProperties"
            ):
                return html.unescape(val_prop.text or "")
        for prop in record.find_all("Property"):
            if not isinstance(prop, Tag):
                continue
            if prop.get("Name") == "Value" and prop.text and "SelectStatement" in prop.text:
                return html.unescape(prop.text)
        return None

    def _get_trx_gen_code(self, record: Tag) -> str | None:
        """Extrai o bloco TrxGenCode (código do transformer) de um Record.

        Args:
            record: Tag BeautifulSoup representando um Record CTransformerStage.

        Returns:
            Código gerado como string, ou ``None`` se ausente.
        """
        for sub in record.find_all("SubRecord"):
            if not isinstance(sub, Tag):
                continue
            name_prop = sub.find("Property", attrs={"Name": "Name"})
            val_prop = sub.find("Property", attrs={"Name": "Value"})
            if (
                isinstance(name_prop, Tag)
                and isinstance(val_prop, Tag)
                and name_prop.text.strip() == "TrxGenCode"
            ):
                return val_prop.text or ""
        return None
