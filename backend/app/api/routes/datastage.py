import hashlib
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, Response

from app.core.logging import get_logger
from app.models.schemas import LineageGraphResponse
from app.services.datastage.xml_analyzer import DataStagePrecisionMapper

router = APIRouter(prefix="/api/datastage", tags=["datastage"])
logger = get_logger(__name__)

_report_cache: dict[str, str] = {}
_MAX_CACHE = 20


def _cache_key(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _get_mapper_from_content(xml_content: str) -> DataStagePrecisionMapper:
    with tempfile.NamedTemporaryFile(
        suffix=".xml", mode="w", encoding="utf-8", delete=False
    ) as f:
        f.write(xml_content)
        tmp_path = Path(f.name)
    try:
        mapper = DataStagePrecisionMapper(tmp_path)
        mapper.run()
        return mapper
    finally:
        tmp_path.unlink(missing_ok=True)


async def _resolve_content(file: UploadFile | None, xml_content: str | None) -> str:
    """Arquivo tem prioridade (sem limite). Texto colado como fallback."""
    if file and file.filename:
        data = await file.read()
        return data.decode("utf-8", errors="replace")
    if xml_content:
        return xml_content
    raise HTTPException(
        status_code=422,
        detail="Envie um arquivo (.dsx/.xml) no campo 'file' ou o XML no campo 'xml_content'.",
    )


def _safe_set(val) -> list:
    if isinstance(val, (set, list)):
        return sorted(str(x) for x in val)
    return []


def _safe_list(val) -> list:
    return val if isinstance(val, list) else []


@router.post("/analyze", response_model=dict)
async def analyze_xml(
    file: UploadFile | None = File(None, description="Arquivo .dsx ou .xml (sem limite de tamanho)"),
    xml_content: str | None = Form(None, description="XML como texto (fallback, max ~1MB)"),
):
    content = await _resolve_content(file, xml_content)
    try:
        mapper = _get_mapper_from_content(content)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"XML parse error: {exc}") from exc

    jobs_out = []
    for job_name, comps in mapper.jobs.items():
        stages = []
        for cid, data in comps.items():
            if not isinstance(data, dict):
                continue
            src_list = _safe_set(data.get("tables_src"))
            tgt_list = _safe_set(data.get("tables_tgt"))
            sqls     = _safe_list(data.get("sqls"))
            logic    = _safe_list(data.get("logic"))
            stages.append({
                "stage_name":    data.get("name", cid),
                "stage_type":    data.get("type", ""),
                "table_name":    tgt_list[0] if tgt_list else (src_list[0] if src_list else None),
                "is_write":      bool(data.get("is_write", False)),
                "source_tables": src_list,
                "target_tables": tgt_list,
                "sql_count":     len(sqls),
                "sqls":          sqls[:3],
                "logic_count":   len(logic),
                "columns": [
                    {
                        "name":       str(l).split(" \u2190 ")[0],
                        "derivation": str(l).split(" \u2190 ")[1] if " \u2190 " in str(l) else "",
                    }
                    for l in logic[:20]
                ],
            })

        all_src: set = set()
        all_tgt: set = set()
        all_sqls: list = []
        for d in comps.values():
            if not isinstance(d, dict):
                continue
            s = d.get("tables_src")
            t = d.get("tables_tgt")
            if isinstance(s, (set, list)):
                all_src.update(s)
            if isinstance(t, (set, list)):
                all_tgt.update(t)
            all_sqls.extend(_safe_list(d.get("sqls"))[:2])

        jobs_out.append({
            "job_name":      job_name,
            "job_type":      "Parallel",
            "description":   "",
            "stages":        stages,
            "source_tables": sorted(str(x) for x in all_src),
            "target_tables": sorted(str(x) for x in all_tgt),
            "sql_queries":   all_sqls,
        })

    return {"jobs": jobs_out}


@router.post("/report", response_class=HTMLResponse)
async def generate_report_from_post(
    file: UploadFile | None = File(None, description="Arquivo .dsx ou .xml"),
    xml_content: str | None = Form(None, description="XML como texto (fallback)"),
    inline: bool = Form(True),
):
    content = await _resolve_content(file, xml_content)
    key = _cache_key(content)

    if key in _report_cache:
        html_str = _report_cache[key]
    else:
        try:
            mapper = _get_mapper_from_content(content)
            html_str = mapper.generate_html_report()
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"XML parse error: {exc}") from exc
        if len(_report_cache) >= _MAX_CACHE:
            del _report_cache[next(iter(_report_cache))]
        _report_cache[key] = html_str

    disposition = "inline" if inline else 'attachment; filename="datastage_report.html"'
    return Response(
        content=html_str,
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": disposition},
    )


@router.post("/lineage", response_model=LineageGraphResponse)
async def get_lineage(
    file: UploadFile | None = File(None),
    xml_content: str | None = Form(None),
):
    content = await _resolve_content(file, xml_content)
    try:
        mapper = _get_mapper_from_content(content)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    metadata: dict = {"file": "uploaded", "jobs": []}
    for job_name, comps in mapper.jobs.items():
        all_src: set = set()
        all_tgt: set = set()
        for d in comps.values():
            if not isinstance(d, dict):
                continue
            s = d.get("tables_src")
            t = d.get("tables_tgt")
            if isinstance(s, (set, list)):
                all_src.update(s)
            if isinstance(t, (set, list)):
                all_tgt.update(t)
        metadata["jobs"].append({
            "job_name":      job_name,
            "job_type":      "Parallel",
            "description":   "",
            "source_tables": sorted(str(x) for x in all_src),
            "target_tables": sorted(str(x) for x in all_tgt),
        })

    from app.services.datastage.xml_parser import DataStageXMLParser
    return DataStageXMLParser().build_lineage_graph(metadata)