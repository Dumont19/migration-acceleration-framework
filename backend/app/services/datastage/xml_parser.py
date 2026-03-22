import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from datetime import datetime

from app.core.logging import get_logger
from app.models.schemas import LineageEdge, LineageGraphResponse, LineageNode

logger = get_logger(__name__)

# ── Stage type mappings ───────────────────────────────────────────────────────

SOURCE_STAGES = {
    "OracleConnectorPX",
    "DB2ConnectorPX",
    "ODBCConnectorPX",
    "JDBCConnectorPX",
    "DatabaseInputStage",
}

TARGET_STAGES = {
    "SnowflakeConnectorPX",
    "DB2ConnectorPX",
    "OracleConnectorPX",
    "DatabaseOutputStage",
    "CopyStage",
}

TRANSFORM_STAGES = {
    "TransformerStage",
    "FilterStage",
    "AggregatorStage",
    "JoinStage",
    "LookupStage",
    "SortStage",
    "RemoveDuplicatesStage",
}

# Default schema label shown in the lineage graph for known node types
_DEFAULT_SOURCE_SCHEMA = "DWADM"
_DEFAULT_TARGET_SCHEMA = "DWADM"


class DataStageXMLParser:

    # ── Public parse interface ────────────────────────────────────────────────

    def parse_file(self, xml_path: str | Path) -> dict[str, Any]:
        path = Path(xml_path)
        if not path.exists():
            raise FileNotFoundError(f"DataStage XML file not found: {path}")

        log = logger.bind(file=path.name, operation="datastage_parse")
        log.info("Parsing DataStage XML", file=str(path))

        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except ET.ParseError as exc:
            raise ValueError(f"Invalid XML in {path}: {exc}") from exc

        jobs = [self._parse_job(j) for j in root.iter("Job")]
        log.info("Parsed DataStage XML", job_count=len(jobs))
        return {"file": path.name, "jobs": jobs}

    def parse_content(self, xml_content: str) -> dict[str, Any]:
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as exc:
            raise ValueError(f"Invalid XML content: {exc}") from exc
        jobs = [self._parse_job(j) for j in root.iter("Job")]
        return {"file": "uploaded", "jobs": jobs}

    # ── Job parsing ───────────────────────────────────────────────────────────

    def _parse_job(self, job_elem: ET.Element) -> dict[str, Any]:
        job_name  = job_elem.get("Identifier", "UNKNOWN")
        job_type  = job_elem.get("JobType", "Parallel")
        description = ""

        for prop in job_elem.iter("Property"):
            if prop.get("Name") == "Comment":
                description = prop.get("Value", "")
                break

        stages: list[dict] = []
        links:  list[dict] = []

        for stage in job_elem.iter("Stage"):
            meta = self._parse_stage(stage)
            if meta:
                stages.append(meta)

        for link in job_elem.iter("Link"):
            src = link.get("From", "")
            tgt = link.get("To", "")
            if src and tgt:
                links.append({"from": src, "to": tgt})

        sources    = [s for s in stages if s["stage_type"] in SOURCE_STAGES]
        targets    = [s for s in stages if s["stage_type"] in TARGET_STAGES]
        transforms = [s for s in stages if s["stage_type"] in TRANSFORM_STAGES]

        return {
            "job_name":      job_name,
            "job_type":      job_type,
            "description":   description,
            "stages":        stages,
            "links":         links,
            "sources":       sources,
            "targets":       targets,
            "transforms":    transforms,
            "source_tables": [s.get("table_name") for s in sources if s.get("table_name")],
            "target_tables": [t.get("table_name") for t in targets if t.get("table_name")],
            "sql_queries":   [s.get("sql") for s in stages if s.get("sql")],
        }

    # ── Stage parsing ─────────────────────────────────────────────────────────

    def _parse_stage(self, stage: ET.Element) -> dict[str, Any] | None:
        stage_name = stage.get("Identifier", "")
        stage_type = stage.get("StageType", "")

        if not stage_type:
            return None

        result: dict[str, Any] = {
            "stage_name":  stage_name,
            "stage_type":  stage_type,
            "table_name":  None,
            "schema_name": None,
            "sql":         None,
            "columns":     [],
        }

        for xml_props in stage.iter("XMLProperties"):
            content = xml_props.text or ""
            result.update(self._parse_xml_properties(content, stage_type))

        for output in stage.iter("OutputPin"):
            for col in output.iter("Column"):
                result["columns"].append({
                    "name":     col.get("Identifier", ""),
                    "type":     col.get("DataType", ""),
                    "nullable": col.get("Nullable", "true") == "true",
                })

        return result

    def _parse_xml_properties(self, content: str, stage_type: str) -> dict[str, Any]:
        """Extract table name and SQL from XMLProperties text blob."""
        if not content.strip():
            return {}

        try:
            inner = ET.fromstring(f"<root>{content}</root>")
        except ET.ParseError:
            return self._regex_extract(content, stage_type)

        result: dict[str, Any] = {}
        for prop in inner.iter("Property"):
            name  = prop.get("Name", "").lower()
            value = prop.get("Value", "") or (prop.text or "")

            if name in ("tablename", "table_name", "targetname"):
                result["table_name"] = value.strip().upper()
            elif name in ("schemaname", "schema_name"):
                result["schema_name"] = value.strip().upper()
            elif name in ("selectstatement", "sqlstatement", "userdefinedsql", "sql"):
                result["sql"] = value.strip()
            elif name == "tabletype" and not result.get("table_name"):
                result["table_name"] = value.strip().upper()

        return result

    def _regex_extract(self, content: str, stage_type: str) -> dict[str, Any]:
        result: dict[str, Any] = {}

        table_match = re.search(
            r'(?:TableName|TargetName|tableName)\s*[=:]\s*["\']?([A-Z_][A-Z0-9_]*)["\']?',
            content, re.IGNORECASE,
        )
        if table_match:
            result["table_name"] = table_match.group(1).upper()

        sql_match = re.search(
            r'(?:SelectStatement|SQLStatement|UserDefinedSQL)\s*[=:]\s*["\'](.+?)["\']',
            content, re.IGNORECASE | re.DOTALL,
        )
        if sql_match:
            result["sql"] = sql_match.group(1).strip()

        return result

    # ── Lineage graph builder ─────────────────────────────────────────────────

    def build_lineage_graph(self, job_metadata: dict[str, Any]) -> LineageGraphResponse:
        """
        Constrói o grafo SOURCE → JOB → TARGET a partir dos metadados
        retornados por parse_file() ou parse_content().

        LineageNode usa alias `schema` no JSON mas `db_schema` internamente.
        Com populate_by_name=True no model podemos passar schema= como kwarg.
        Nós do tipo `job` não têm schema — campo fica None (opcional).
        """
        nodes:    list[LineageNode] = []
        edges:    list[LineageEdge] = []
        seen_ids: set[str]          = set()

        for job in job_metadata.get("jobs", []):
            job_name = job["job_name"]
            job_id   = f"job_{job_name}"

            # ── Job node (sem schema) ─────────────────────────────────────
            if job_id not in seen_ids:
                nodes.append(LineageNode(
                    id=job_id,
                    label=job_name,
                    type="job",
                    db_schema=None,
                    extra={
                        "job_type":    job.get("job_type", ""),
                        "description": job.get("description", ""),
                    },
                ))
                seen_ids.add(job_id)

            # ── Source nodes ──────────────────────────────────────────────
            for src_table in job.get("source_tables", []):
                src_id = f"src_{src_table}"
                if src_id not in seen_ids:
                    nodes.append(LineageNode(
                        id=src_id,
                        label=src_table,
                        type="source",
                        db_schema=_DEFAULT_SOURCE_SCHEMA,
                    ))
                    seen_ids.add(src_id)
                edges.append(LineageEdge(source=src_id, target=job_id))

            # ── Target nodes ──────────────────────────────────────────────
            for tgt_table in job.get("target_tables", []):
                tgt_id = f"tgt_{tgt_table}"
                if tgt_id not in seen_ids:
                    nodes.append(LineageNode(
                        id=tgt_id,
                        label=tgt_table,
                        type="target",
                        db_schema=_DEFAULT_TARGET_SCHEMA,
                    ))
                    seen_ids.add(tgt_id)
                edges.append(LineageEdge(source=job_id, target=tgt_id))

        job_label = job_metadata.get("file", "unknown")
        if job_metadata.get("jobs"):
            job_label = job_metadata["jobs"][0]["job_name"]

        return LineageGraphResponse(
            nodes=nodes,
            edges=edges,
            job_name=job_label,
            generated_at=datetime.utcnow(),
        )