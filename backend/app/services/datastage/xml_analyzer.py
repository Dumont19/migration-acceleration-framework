"""
services/datastage/xml_analyzer.py
------------------------------------
Módulo 3 do MAF — reimplementado com o design corporativo v4.

Porta o DataStagePrecisionMapper original com todas as correções acumuladas:
  - Parser de XMLProperties (OracleConnectorPX)
  - Captura de PL/SQL, INSERT, UPDATE, MERGE, BEGIN…END
  - Distinção source vs target via WriteMode
  - TrxOutput / TrxInput column parsing (jobs LDW)
  - InterVar resolution → CASE WHEN Snowflake
  - Jobs Hashed (lookup enrichment)

Saída:
  1. dict estruturado para a API (LineageGraph, etc.)
  2. Relatório HTML standalone — abre no browser com:
       - SQL syntax highlighting (Prism.js via CDN)
       - SQL indentado automaticamente (sqlparse)
       - Design dark terminal (vars do globals.css)
       - Cards por stage com botão copy SQL
       - Mapa de dependências colapsável
"""

import html
import re
import textwrap
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup, Tag

from app.core.logging import get_logger

logger = get_logger(__name__)

# ── Stage type classification ────────────────────────────────────────────────

TYPE_MAP: dict[str, dict] = {
    # Oracle connectors
    "ORACLECONNECTOR":    {"label": "Oracle Connector",    "color": "#e07b39", "icon": "🔶"},
    "ORACLECONNECTORPX":  {"label": "Oracle Connector PX", "color": "#e07b39", "icon": "🔶"},
    "PXORACLECONNECTOR":  {"label": "Oracle Connector",    "color": "#e07b39", "icon": "🔶"},
    # Snowflake
    "SNOWFLAKECONNECTORPX": {"label": "Snowflake Connector", "color": "#29b5e8", "icon": "❄️"},
    # Transformers
    "CTRANSFORMERSTAGE":  {"label": "Transformer",         "color": "#00ff88", "icon": "⚡"},
    "TRANSFORMERSTAGE":   {"label": "Transformer",         "color": "#00ff88", "icon": "⚡"},
    # Lookups
    "PXLOOKUP":           {"label": "Lookup",              "color": "#a855f7", "icon": "🔍"},
    "LOOKUPSTAGE":        {"label": "Lookup",              "color": "#a855f7", "icon": "🔍"},
    # Filters / Sort / Agg
    "FILTERSTAGE":        {"label": "Filter",              "color": "#f59e0b", "icon": "🔽"},
    "AGGREGATORSTAGE":    {"label": "Aggregator",          "color": "#f59e0b", "icon": "∑"},
    "SORTSTAGE":          {"label": "Sort",                "color": "#6b7280", "icon": "↕"},
    "REMOVEDUPLICATES":   {"label": "Remove Duplicates",   "color": "#6b7280", "icon": "✂"},
    # Join / Merge
    "PXJOIN":             {"label": "Join",                "color": "#3b82f6", "icon": "⋈"},
    "JOINSTAGE":          {"label": "Join",                "color": "#3b82f6", "icon": "⋈"},
    "PXFUNNEL":           {"label": "Funnel",              "color": "#3b82f6", "icon": "V"},
    # Sequence
    "SEQUENCER":          {"label": "Sequencer",           "color": "#9ca3af", "icon": "▶"},
    # Copy/Head
    "COPYSTAGE":          {"label": "Copy",                "color": "#9ca3af", "icon": "©"},
    # Generic
    "DATASTAGE":          {"label": "DataStage Stage",     "color": "#9ca3af", "icon": "◈"},
}

WRITE_STAGE_TYPES = {
    "ORACLECONNECTOR", "ORACLECONNECTORPX", "PXORACLECONNECTOR",
    "SNOWFLAKECONNECTORPX",
}

WRITE_NAME_PREFIXES = ("F_", "FATO", "DEL_", "UPD_", "TGT_", "DEST_", "OUTPUT")


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _indent_sql(sql: str) -> str:
    """
    Simple SQL indenter — no external deps required.
    Breaks on keywords and indents sub-clauses.
    """
    if not sql or len(sql) < 20:
        return sql.strip()

    keywords = [
        "SELECT", "FROM", "WHERE", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
        "FULL OUTER JOIN", "JOIN", "ON", "AND", "OR", "GROUP BY", "ORDER BY",
        "HAVING", "UNION ALL", "UNION", "INSERT INTO", "VALUES", "UPDATE",
        "SET", "DELETE FROM", "MERGE INTO", "USING", "WHEN MATCHED",
        "WHEN NOT MATCHED", "BEGIN", "END", "DECLARE",
    ]
    result = sql.strip()
    # Normalize whitespace
    result = re.sub(r'\s+', ' ', result)
    # Break before major keywords
    for kw in keywords:
        result = re.sub(
            r'(?<!\w)(' + re.escape(kw) + r')(?!\w)',
            r'\n\1',
            result,
            flags=re.IGNORECASE,
        )
    # Indent sub-clauses
    lines = result.split('\n')
    indented = []
    indent = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith(("SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY",
                              "HAVING", "UNION", "INSERT", "UPDATE", "MERGE",
                              "BEGIN", "DECLARE")):
            indented.append(line)
            indent = 4
        elif upper.startswith(("LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
                                "FULL OUTER JOIN", "JOIN")):
            indented.append("  " + line)
        elif upper.startswith(("AND ", "OR ")):
            indented.append(" " * indent + line)
        elif upper.startswith(("WHEN", "THEN", "ELSE", "END")):
            indented.append("    " + line)
        else:
            indented.append(" " * indent + line)
    return '\n'.join(indented)


def _decode_entities(text: str) -> str:
    """Decode HTML entities from XMLProperties blobs."""
    text = html.unescape(text)
    text = text.replace("&apos;", "'").replace("&quot;", '"')
    return text


# ── Core analyzer ────────────────────────────────────────────────────────────

class DataStagePrecisionMapper:
    """
    Parses a DataStage .dsx / .xml export and extracts structured metadata.

    Supports:
      - Parallel jobs (LDW, ODS)
      - Hashed lookup jobs
      - Sequence jobs
      - OracleConnectorPX with XMLProperties
      - Transformer stages with InterVar resolution
      - TrxOutput/TrxInput (LDW column derivations)
    """

    def __init__(self, xml_path: str | Path) -> None:
        self.xml_path = Path(xml_path)
        self.jobs: dict[str, dict] = {}
        self.stats = {"jobs": 0, "sqls": 0}
        self._patterns = {
            "sql_select": re.compile(
                r"(SELECT\s+.+?FROM\s+.+?)(?=\s*(?:WHERE|GROUP|ORDER|HAVING|UNION|;|$))",
                re.IGNORECASE | re.DOTALL,
            ),
            "sql_dml": re.compile(
                r"((?:INSERT\s+INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+.+?)(?=\s*(?:;|BEGIN|END|$))",
                re.IGNORECASE | re.DOTALL,
            ),
            "sql_plsql": re.compile(
                r"(BEGIN\s+.+?END\s*;?)",
                re.IGNORECASE | re.DOTALL,
            ),
            "tbl_src": re.compile(
                r"(?:FROM|JOIN)\s+([A-Za-z0-9_.$#]+)",
                re.IGNORECASE,
            ),
            "tbl_tgt": re.compile(
                r"(?:INSERT\s+INTO|UPDATE|MERGE\s+INTO)\s+([A-Za-z0-9_.$#]+)",
                re.IGNORECASE,
            ),
            "intervar_assign": re.compile(
                r"(InterVar\d+_\d+)\s*=\s*(.+?)(?=\s*(?:InterVar|\Z))",
                re.IGNORECASE | re.DOTALL,
            ),
            "intervar_ref": re.compile(r"\bInterVar\d+_\d+\b", re.IGNORECASE),
            "cond_block": re.compile(
                r"if\s*\((.+?)\)\s*\{?\s*InterVar\d+_\d+\s*=\s*(.+?)\s*\}?(?:\s*else\s*\{?\s*InterVar\d+_\d+\s*=\s*(.+?)\s*\}?)?",
                re.IGNORECASE | re.DOTALL,
            ),
        }

    # ── Public ────────────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """Parse the XML and return structured metadata dict."""
        log = logger.bind(file=self.xml_path.name, operation="datastage_parse")
        log.info("Parsing DataStage XML")

        content = self.xml_path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(content, "xml")

        for record in soup.find_all("Record"):
            rtype = record.get("Type", "")
            if rtype == "JobDefn":
                self._init_job(record)
            elif rtype == "TrxOutput":
                self._analyze_trx_output(record)
            elif rtype not in ("", "JobDefn", "TrxOutput", "TrxInput"):
                self._analyze_component(record)

        self.stats["jobs"] = len(self.jobs)
        log.info("Parse complete", jobs=self.stats["jobs"], sqls=self.stats["sqls"])
        return self.jobs

    def generate_html_report(self, output_path: str | Path | None = None) -> str:
        """
        Generate standalone HTML report with SQL highlighting.
        Returns the HTML string. If output_path given, also writes to file.
        """
        if not self.jobs:
            self.run()

        html_str = self._render_html()

        if output_path:
            Path(output_path).write_text(html_str, encoding="utf-8")
            logger.info("HTML report written", path=str(output_path))

        return html_str

    def get_dependency_map(self) -> dict[str, dict]:
        """Return SOURCE → TARGETS dependency map for lineage graph."""
        deps: dict[str, dict] = {}
        for job_name, comps in self.jobs.items():
            sources: set[str] = set()
            targets: set[str] = set()
            for data in comps.values():
                sources.update(data.get("tables_src", set()))
                targets.update(data.get("tables_tgt", set()))
            deps[job_name] = {"sources": sorted(sources), "targets": sorted(targets)}
        return deps

    # ── Parsing internals ────────────────────────────────────────────────

    def _init_job(self, record: Tag) -> None:
        job_id = record.get("Identifier", f"JOB_{uuid.uuid4().hex[:6]}")
        self.jobs[job_id] = {}

    def _analyze_component(self, record: Tag) -> None:
        job = self._find_parent_job(record)
        if not job:
            return

        comp_id = record.get("Identifier", "")
        comp_type = record.get("Type", "").upper()
        comp_name = ""

        # Get name from subrecord
        name_rec = record.find("SubRecord", {"Type": "GeneralInfo"})
        if name_rec:
            comp_name = name_rec.get("Identifier", comp_id)

        if comp_id not in self.jobs[job]:
            self.jobs[job][comp_id] = {
                "name": comp_name or comp_id,
                "type": comp_type,
                "tables_src": set(),
                "tables_tgt": set(),
                "sqls": [],
                "files": [],
                "logic": [],
                "is_write": False,
            }

        data = self.jobs[job][comp_id]

        # Process all properties
        for prop in record.find_all("Property"):
            self._process_property(prop, data, comp_type)

        # Process XMLProperties (OracleConnectorPX)
        for xml_props in record.find_all("XMLProperties"):
            self._parse_xml_properties_blob(xml_props.get_text(), data, comp_type)

        # Extract SQL from logic
        for sql in data["sqls"]:
            self.stats["sqls"] += 1
            for t in self._patterns["tbl_src"].findall(sql):
                if "." in t or len(t) > 3:
                    data["tables_src"].add(t.upper())
            for t in self._patterns["tbl_tgt"].findall(sql):
                if "." in t or len(t) > 3:
                    data["tables_tgt"].add(t.upper())

        # Determine write stage
        name_up = data["name"].upper()
        if (
            comp_type in WRITE_STAGE_TYPES and data.get("_write_mode_detected")
        ) or any(name_up.startswith(p) for p in WRITE_NAME_PREFIXES):
            data["is_write"] = True

    def _process_property(self, prop: Tag, data: dict, comp_type: str) -> None:
        name = prop.get("Name", "").strip()
        value = prop.get("Value", "") or prop.get_text(strip=True)
        value = _decode_entities(value).strip()

        # Table references
        if name in ("TableName", "SourceTableName"):
            if comp_type in WRITE_STAGE_TYPES and data.get("is_write"):
                data["tables_tgt"].add(value.upper())
            else:
                data["tables_src"].add(value.upper())
        elif name in ("DestTableName", "WriteTableName"):
            data["tables_tgt"].add(value.upper())
            data["is_write"] = True
        elif name == "WriteMode":
            data["_write_mode_detected"] = True
            data["is_write"] = True

        # SQL properties
        elif name in (
            "SelectStatement", "UserDefinedSQL", "SQLStatement",
            "WriteSQL", "UpdateSQL", "BeforeSQL", "AfterSQL",
            "PreSQL", "PostSQL", "StoredProcedure",
        ):
            if value and len(value) > 5:
                data["sqls"].append(_decode_entities(value))

        # File references
        elif name in ("Filename", "File", "FileName"):
            if value.startswith(("/", "\\")):
                data["files"].append(value)

        # TrxGenCode — transformer logic
        elif name == "TrxGenCode":
            self._extract_transformer_logic(value, data)

    def _parse_xml_properties_blob(self, blob: str, data: dict, comp_type: str) -> None:
        """Parse the XMLProperties text blob (encoded XML-within-XML)."""
        decoded = _decode_entities(blob)
        try:
            inner = BeautifulSoup(f"<root>{decoded}</root>", "xml")
        except Exception:
            # Fallback: regex
            self._xml_props_regex_fallback(decoded, data)
            return

        for prop in inner.find_all("Property"):
            self._process_property(prop, data, comp_type)

        # Also check CDATA sections for SQL
        cdata_re = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
        for match in cdata_re.finditer(decoded):
            cdata = match.group(1).strip()
            if re.search(r"\b(SELECT|INSERT|UPDATE|MERGE|BEGIN)\b", cdata, re.IGNORECASE):
                data["sqls"].append(cdata)

    def _xml_props_regex_fallback(self, text: str, data: dict) -> None:
        """Regex fallback when XMLProperties can't be parsed as XML."""
        # Table name
        tm = re.search(r'TableName["\s:=]+([A-Z0-9_.]+)', text, re.IGNORECASE)
        if tm:
            data["tables_src"].add(tm.group(1).upper())
        # Write mode
        wm = re.search(r'WriteMode["\s:=]+([01])', text)
        if wm and wm.group(1) == "1":
            data["is_write"] = True
        # SQL
        for pattern in [self._patterns["sql_select"], self._patterns["sql_dml"]]:
            for m in pattern.finditer(text):
                data["sqls"].append(m.group(1).strip())

    def _analyze_trx_output(self, record: Tag) -> None:
        """Handle TrxOutput records (LDW column derivations)."""
        identifier = record.get("Identifier", "")
        # Parent stage ID: strip suffix P\d+
        parent_id = re.sub(r"P\d+$", "", identifier)
        job = self._find_parent_job(record)
        if not job or parent_id not in self.jobs.get(job, {}):
            return

        data = self.jobs[job][parent_id]
        for col in record.find_all("OutputColumn"):
            col_name = col.get("Identifier", "")
            derivation = ""
            deriv_tag = col.find("ParsedDerivation") or col.find("Derivation")
            if deriv_tag:
                derivation = deriv_tag.get_text(strip=True)
            if col_name:
                entry = f"{col_name} ← {derivation}" if derivation else col_name
                data["logic"].append(entry)

    def _extract_transformer_logic(self, trx_code: str, data: dict) -> None:
        """
        Extract column derivations and CASE WHEN from TrxGenCode.
        Resolves InterVar assignments to their values.
        """
        if not trx_code:
            return

        decoded = _decode_entities(trx_code)
        intervar_map = self._parse_intervar_map(decoded)

        # Line-by-line: output column assignments
        for line in decoded.splitlines():
            line = line.strip()
            if not line or line.startswith("//"):
                continue

            # Column derivation: COLNAME = expression
            col_match = re.match(r"(\w+)\s*=\s*(.+)", line)
            if col_match:
                col = col_match.group(1)
                expr = col_match.group(2).strip()

                # Skip InterVar assignments — those are internal
                if re.match(r"InterVar\d+_\d+", col, re.IGNORECASE):
                    continue

                # Resolve conditional (if/else → CASE WHEN)
                if "InterVar" in expr and intervar_map:
                    case_expr = self._build_case_when(col, decoded, intervar_map)
                    if case_expr:
                        data["logic"].append(case_expr)
                        continue

                # Substitute any remaining InterVar refs
                expr_resolved = self._substitute_intervars(expr, intervar_map)
                data["logic"].append(f"{col} ← {expr_resolved}")

    def _parse_intervar_map(self, code: str) -> dict[str, str]:
        """Extract InterVar0_N = "value" assignments from initialize block."""
        imap: dict[str, str] = {}
        for m in re.finditer(
            r"(InterVar\d+_\d+)\s*=\s*\"([^\"]*)\"|"
            r"(InterVar\d+_\d+)\s*=\s*'([^']*)'",
            code,
            re.IGNORECASE,
        ):
            if m.group(1):
                imap[m.group(1).upper()] = m.group(2)
            elif m.group(3):
                imap[m.group(3).upper()] = m.group(4)
        return imap

    def _substitute_intervars(self, expr: str, imap: dict[str, str]) -> str:
        def replacer(m: re.Match) -> str:
            key = m.group(0).upper()
            return f'"{imap[key]}"' if key in imap else m.group(0)
        return self._patterns["intervar_ref"].sub(replacer, expr)

    def _build_case_when(self, col: str, code: str, imap: dict[str, str]) -> str | None:
        """
        Reconstruct CASE WHEN from DataStage if/else InterVar block.
        Returns formatted SQL string or None.
        """
        # Find the block for this column
        pattern = re.compile(
            r"if\s*\((.+?)\)\s*" + re.escape(col) + r"\s*=\s*(InterVar\d+_\d+)"
            r"(?:\s*else\s*" + re.escape(col) + r"\s*=\s*(InterVar\d+_\d+))?",
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(code)
        if not match:
            return None

        cond_raw = match.group(1).strip()
        then_var = match.group(2).strip().upper()
        else_var = match.group(3).strip().upper() if match.group(3) else None

        # Convert condition to SQL
        cond_sql = cond_raw
        cond_sql = re.sub(r"\s*==\s*", " = ", cond_sql)
        cond_sql = re.sub(r"\s*\|\|\s*", " OR ", cond_sql)
        cond_sql = re.sub(r"\s*&&\s*", " AND ", cond_sql)
        cond_sql = re.sub(r"(?i)\bnull\(([^)]+)\)", r"\1 IS NULL", cond_sql)
        cond_sql = re.sub(r"(?i)\bisnull\(([^)]+)\)", r"\1 IS NULL", cond_sql)
        # Remove link prefixes (In_TRANSF2.COL → COL)
        cond_sql = re.sub(r"\b\w+\.(\w+)", r"\1", cond_sql)
        cond_sql = re.sub(r"^\(+|\)+$", "", cond_sql).strip()

        then_val = f'"{imap[then_var]}"' if then_var in imap else then_var
        else_val = ""
        if else_var:
            else_val = f'"{imap[else_var]}"' if else_var in imap else else_var

        lines = [
            f"{col} [CONDICIONAL]",
            f"  DataStage:  if ({cond_raw.strip()}) → {then_var}"
            + (f"  else → {else_var}" if else_var else ""),
            f"  Snowflake:  CASE WHEN {cond_sql} THEN {then_val}"
            + (f" ELSE {else_val}" if else_val else "")
            + f" END AS {col}",
        ]
        return "\n".join(lines)

    def _find_parent_job(self, record: Tag) -> str | None:
        """Walk up the XML tree to find the parent JobDefn identifier."""
        node = record.parent
        while node:
            if node.name == "Record" and node.get("Type") == "JobDefn":
                return node.get("Identifier")
            node = node.parent
        # Fallback: use first job
        if self.jobs:
            return next(iter(self.jobs))
        return None

    # ── HTML Report ──────────────────────────────────────────────────────

    def _render_html(self) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filename = self.xml_path.name
        job_sections = ""

        for job_name, comps in self.jobs.items():
            # Filter to meaningful stages only
            valid = {
                cid: d for cid, d in comps.items()
                if d.get("type", "").upper() in TYPE_MAP
                and (d["sqls"] or d["tables_src"] or d["tables_tgt"] or d["logic"])
            }
            if not valid:
                continue

            cards = ""
            for cid, data in valid.items():
                cards += self._render_card(data)

            job_sections += f"""
            <section class="job-section">
              <div class="job-header">
                <span class="job-prefix">// JOB</span>
                <h2 class="job-title">{html.escape(job_name)}</h2>
                <span class="job-meta">{len(valid)} stages</span>
              </div>
              <div class="cards-grid">{cards}</div>
            </section>
            """

        # Dependency map
        deps = self.get_dependency_map()
        dep_rows = ""
        for job, info in deps.items():
            srcs = ", ".join(info["sources"]) or "—"
            tgts = ", ".join(info["targets"]) or "—"
            dep_rows += f"""
            <tr>
              <td class="dep-job">{html.escape(job)}</td>
              <td class="dep-src">{html.escape(srcs)}</td>
              <td class="dep-tgt">{html.escape(tgts)}</td>
            </tr>
            """

        return HTML_TEMPLATE.format(
            filename=html.escape(filename),
            timestamp=timestamp,
            total_jobs=self.stats["jobs"],
            total_sqls=self.stats["sqls"],
            job_sections=job_sections,
            dep_rows=dep_rows,
        )

    def _render_card(self, data: dict) -> str:
        t = data.get("type", "").upper()
        meta = TYPE_MAP.get(t, {"label": t, "color": "#9ca3af", "icon": "◈"})
        color = meta["color"]
        icon = meta["icon"]
        label = meta["label"]
        name = html.escape(data["name"])
        badge_cls = "badge-write" if data.get("is_write") else "badge-read"
        badge_text = "WRITE" if data.get("is_write") else "READ"

        # Tables
        src_tags = "".join(
            f'<span class="tag tag-src">{html.escape(t)}</span>'
            for t in sorted(data["tables_src"])
        )
        tgt_tags = "".join(
            f'<span class="tag tag-tgt">{html.escape(t)}</span>'
            for t in sorted(data["tables_tgt"])
        )
        file_tags = "".join(
            f'<span class="tag tag-file">{html.escape(f)}</span>'
            for f in data["files"][:5]
        )

        # SQL blocks
        sql_blocks = ""
        for i, sql in enumerate(data["sqls"][:5]):
            clean = _indent_sql(sql)
            escaped = html.escape(clean)
            uid = f"sql_{id(data)}_{i}"
            sql_blocks += f"""
            <div class="sql-block">
              <div class="sql-header">
                <span class="sql-label">SQL_{i+1}</span>
                <button class="copy-btn" onclick="copySql('{uid}')">copy</button>
              </div>
              <pre id="{uid}" class="language-sql"><code class="language-sql">{escaped}</code></pre>
            </div>
            """

        # Logic / derivations
        logic_html = ""
        if data["logic"]:
            items = "".join(
                f'<div class="logic-item">{html.escape(l)}</div>'
                for l in data["logic"][:20]
            )
            logic_html = f'<div class="logic-section"><div class="section-label">derivations</div>{items}</div>'

        return f"""
        <div class="card" style="--card-color:{color}">
          <div class="card-header">
            <span class="stage-icon">{icon}</span>
            <div class="card-title-group">
              <div class="card-title">{name}</div>
              <div class="card-type">{html.escape(label)}</div>
            </div>
            <span class="badge {badge_cls}">{badge_text}</span>
          </div>
          <div class="card-body">
            {('<div class="section-label">source_tables</div>' + src_tags) if src_tags else ''}
            {('<div class="section-label">target_tables</div>' + tgt_tags) if tgt_tags else ''}
            {('<div class="section-label">files</div>' + file_tags) if file_tags else ''}
            {sql_blocks}
            {logic_html}
          </div>
        </div>
        """


# ── HTML Template ─────────────────────────────────────────────────────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MAF // {filename}</title>

<!-- Prism.js — SQL syntax highlighting -->
<link rel="stylesheet"
  href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&display=swap" rel="stylesheet">

<style>
/* ── Design system tokens (from MAF v4 globals.css) ─────────────────── */
:root {{
  --bg-primary:   #0d0d0d;
  --bg-secondary: #111111;
  --bg-tertiary:  #161616;
  --bg-border:    #1a1a1a;
  --bg-hover:     #1f1f1f;
  --accent:       #00ff88;
  --accent-dim:   #00cc66;
  --accent-bg:    rgba(0,255,136,0.06);
  --text-primary: #f0f0f0;
  --text-secondary:#888888;
  --text-muted:   #555555;
  --status-error: #ff4444;
  --status-warn:  #ffcc00;
  --status-info:  #4488ff;
  --font-mono:    'JetBrains Mono', 'Courier New', monospace;
  --radius:       2px;
}}

* {{ box-sizing: border-box; margin: 0; padding: 0; }}

html, body {{
  font-family: var(--font-mono);
  background: var(--bg-primary);
  color: var(--text-primary);
  font-size: 13px;
  -webkit-font-smoothing: antialiased;
}}

::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: var(--bg-secondary); }}
::-webkit-scrollbar-thumb {{ background: #333; border-radius: 2px; }}

/* ── Header ──────────────────────────────────────────────────────────── */
.site-header {{
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--bg-border);
  padding: 14px 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 100;
}}
.header-brand {{
  display: flex;
  align-items: baseline;
  gap: 10px;
}}
.header-maf {{
  font-size: 15px;
  font-weight: 700;
  color: var(--text-primary);
  letter-spacing: 0.08em;
}}
.header-sep {{ color: var(--text-muted); }}
.header-file {{ font-size: 13px; color: var(--accent); }}
.header-meta {{
  font-size: 11px;
  color: var(--text-muted);
  display: flex;
  gap: 20px;
}}
.header-meta span {{ color: var(--text-secondary); }}
.header-meta b {{ color: var(--accent); }}

/* ── Stats bar ───────────────────────────────────────────────────────── */
.stats-bar {{
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--bg-border);
  padding: 10px 32px;
  display: flex;
  gap: 32px;
}}
.stat {{
  display: flex;
  flex-direction: column;
  gap: 2px;
}}
.stat-label {{
  font-size: 9px;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.1em;
}}
.stat-value {{
  font-size: 18px;
  font-weight: 700;
  color: var(--accent);
  line-height: 1;
}}

/* ── Main ────────────────────────────────────────────────────────────── */
.main {{ padding: 32px; max-width: 1400px; margin: 0 auto; }}

/* ── Section label (portfolio style) ────────────────────────────────── */
.section-label {{
  font-size: 10px;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  margin: 12px 0 6px;
  display: flex;
  align-items: center;
  gap: 6px;
}}
.section-label::before {{ content: '//'; color: var(--accent); }}
.section-label::after {{
  content: '';
  flex: 1;
  height: 1px;
  background: var(--bg-border);
  margin-left: 4px;
}}

/* ── Job section ─────────────────────────────────────────────────────── */
.job-section {{
  margin-bottom: 48px;
}}
.job-header {{
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-bottom: 16px;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--bg-border);
}}
.job-prefix {{ font-size: 11px; color: var(--accent); opacity: 0.6; }}
.job-title {{ font-size: 16px; font-weight: 600; color: var(--text-primary); }}
.job-meta {{ font-size: 11px; color: var(--text-muted); margin-left: auto; }}

/* ── Cards grid ──────────────────────────────────────────────────────── */
.cards-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 16px;
}}

/* ── Card ────────────────────────────────────────────────────────────── */
.card {{
  background: var(--bg-secondary);
  border: 1px solid var(--bg-border);
  border-top: 2px solid var(--card-color, var(--accent));
  border-radius: var(--radius);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  max-height: 560px;
}}
.card-header {{
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 14px 10px;
  border-bottom: 1px solid var(--bg-border);
  background: rgba(255,255,255,0.02);
  flex-shrink: 0;
}}
.stage-icon {{ font-size: 16px; flex-shrink: 0; }}
.card-title-group {{ flex: 1; min-width: 0; }}
.card-title {{
  font-size: 13px;
  font-weight: 600;
  color: var(--text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.card-type {{ font-size: 10px; color: var(--text-muted); margin-top: 1px; }}
.badge {{
  font-size: 9px;
  padding: 2px 7px;
  border: 1px solid;
  border-radius: 1px;
  flex-shrink: 0;
  font-weight: 600;
  letter-spacing: 0.05em;
}}
.badge-write {{ color: var(--accent); border-color: rgba(0,255,136,0.3); }}
.badge-read  {{ color: var(--text-muted); border-color: var(--bg-border); }}

.card-body {{
  padding: 12px 14px;
  overflow-y: auto;
  flex: 1;
  min-height: 0;
}}

/* ── Tags ────────────────────────────────────────────────────────────── */
.tag {{
  display: inline-block;
  font-size: 11px;
  padding: 3px 8px;
  border-radius: 1px;
  margin: 2px 3px 2px 0;
  border-left: 2px solid;
  font-family: var(--font-mono);
  word-break: break-all;
}}
.tag-src  {{ background: rgba(244,162,97,0.08); border-color: #f4a261; color: #f4a261; }}
.tag-tgt  {{ background: rgba(0,255,136,0.07);  border-color: var(--accent); color: var(--accent); }}
.tag-file {{ background: rgba(100,100,100,0.1); border-color: #666; color: #aaa; }}

/* ── SQL blocks ──────────────────────────────────────────────────────── */
.sql-block {{
  margin-top: 12px;
  border: 1px solid var(--bg-border);
  border-radius: var(--radius);
  overflow: hidden;
}}
.sql-header {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 5px 10px;
  background: var(--bg-tertiary);
  border-bottom: 1px solid var(--bg-border);
}}
.sql-label {{
  font-size: 9px;
  color: var(--accent);
  text-transform: uppercase;
  letter-spacing: 0.1em;
}}
.copy-btn {{
  background: rgba(255,255,255,0.06);
  border: 1px solid rgba(255,255,255,0.12);
  color: var(--text-secondary);
  padding: 2px 8px;
  border-radius: 1px;
  cursor: pointer;
  font-size: 10px;
  font-family: var(--font-mono);
  transition: all 0.15s;
}}
.copy-btn:hover {{
  background: rgba(0,255,136,0.1);
  color: var(--accent);
  border-color: rgba(0,255,136,0.3);
}}
.copy-btn.copied {{
  color: var(--accent);
}}

/* Override Prism tomorrow theme for our container */
pre[class*="language-"] {{
  background: #0a0a0a !important;
  margin: 0 !important;
  border-radius: 0 !important;
  max-height: 280px;
  overflow-y: auto;
  font-size: 11.5px !important;
  line-height: 1.7 !important;
  padding: 12px 14px !important;
  white-space: pre-wrap !important;
  word-break: break-word !important;
}}
code[class*="language-"] {{
  font-family: var(--font-mono) !important;
  font-size: inherit !important;
}}

/* ── Logic/derivations ───────────────────────────────────────────────── */
.logic-section {{ margin-top: 12px; }}
.logic-item {{
  font-size: 11px;
  color: var(--text-secondary);
  padding: 3px 0;
  border-bottom: 1px solid var(--bg-border);
  white-space: pre-wrap;
  word-break: break-all;
  line-height: 1.6;
}}
.logic-item:last-child {{ border-bottom: none; }}

/* ── Dependency table ────────────────────────────────────────────────── */
.dep-section {{
  margin-top: 48px;
  padding-top: 24px;
  border-top: 1px solid var(--bg-border);
}}
.dep-title {{
  font-size: 14px;
  font-weight: 600;
  color: var(--accent);
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.dep-title::before {{ content: '//'; opacity: 0.6; font-size: 11px; }}
table.dep-table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}}
.dep-table th {{
  text-align: left;
  padding: 8px 12px;
  font-size: 10px;
  color: var(--accent);
  border-bottom: 1px solid var(--bg-border);
  text-transform: uppercase;
  letter-spacing: 0.06em;
}}
.dep-table td {{
  padding: 7px 12px;
  border-bottom: 1px solid var(--bg-border);
  vertical-align: top;
  word-break: break-word;
}}
.dep-table tbody tr:hover td {{ background: var(--bg-hover); }}
.dep-job {{ color: var(--accent); font-weight: 500; }}
.dep-src {{ color: #f4a261; }}
.dep-tgt {{ color: var(--text-primary); }}

/* ── Footer ──────────────────────────────────────────────────────────── */
.site-footer {{
  margin-top: 64px;
  padding: 20px 32px;
  border-top: 1px solid var(--bg-border);
  font-size: 11px;
  color: var(--text-muted);
  display: flex;
  justify-content: space-between;
}}
</style>
</head>
<body>

<!-- Header -->
<header class="site-header">
  <div class="header-brand">
    <span class="header-maf">MAF</span>
    <span class="header-sep">//</span>
    <span class="header-file">{filename}</span>
  </div>
  <div class="header-meta">
    <span>generated <b>{timestamp}</b></span>
    <span>jobs <b>{total_jobs}</b></span>
    <span>sql_blocks <b>{total_sqls}</b></span>
  </div>
</header>

<!-- Stats bar -->
<div class="stats-bar">
  <div class="stat">
    <div class="stat-label">jobs_parsed</div>
    <div class="stat-value">{total_jobs}</div>
  </div>
  <div class="stat">
    <div class="stat-label">sql_blocks</div>
    <div class="stat-value">{total_sqls}</div>
  </div>
</div>

<!-- Main -->
<main class="main">
  {job_sections}

  <!-- Dependency map -->
  <div class="dep-section">
    <div class="dep-title">dependency_map</div>
    <table class="dep-table">
      <thead>
        <tr>
          <th>job</th>
          <th>source_tables</th>
          <th>target_tables</th>
        </tr>
      </thead>
      <tbody>
        {dep_rows}
      </tbody>
    </table>
  </div>
</main>

<!-- Footer -->
<footer class="site-footer">
  <span>MAF // Migration Acceleration Framework v4.0</span>
  <span>Oracle → Snowflake // Algar Telecom</span>
</footer>

<!-- Prism.js -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>

<script>
function copySql(id) {{
  const el = document.getElementById(id);
  if (!el) return;
  const text = el.textContent || el.innerText;
  navigator.clipboard.writeText(text).then(() => {{
    const btn = el.closest('.sql-block').querySelector('.copy-btn');
    if (btn) {{
      btn.textContent = 'copied!';
      btn.classList.add('copied');
      setTimeout(() => {{
        btn.textContent = 'copy';
        btn.classList.remove('copied');
      }}, 1800);
    }}
  }});
}}
// Re-highlight after load (in case of dynamic content)
window.addEventListener('load', () => {{
  if (window.Prism) Prism.highlightAll();
}});
</script>
</body>
</html>
"""
