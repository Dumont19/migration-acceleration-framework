"""
services/datastage/xml_analyzer.py
------------------------------------
Parser DataStage XML portado do datastage_job_analyzer_2.py.
Lógica de parse 100% original — só adaptado para o MAF (sem os.path, sem logger externo,
sem webbrowser). HTML report com tema claro/escuro sincronizado com a aplicação.
"""
import html
import re
import datetime
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

from app.core.logging import get_logger

logger = get_logger(__name__)


def _indent_sql(sql: str) -> str:
    if not sql or len(sql) < 20:
        return sql.strip()
    keywords = [
        "SELECT","FROM","WHERE","LEFT JOIN","RIGHT JOIN","INNER JOIN",
        "FULL OUTER JOIN","JOIN","ON","GROUP BY","ORDER BY","HAVING",
        "UNION ALL","UNION","INSERT INTO","VALUES","UPDATE","SET",
        "DELETE FROM","MERGE INTO","USING","WHEN MATCHED","WHEN NOT MATCHED",
        "BEGIN","END","DECLARE",
    ]
    result = re.sub(r'\s+', ' ', sql.strip())
    for kw in keywords:
        result = re.sub(r'(?<!\w)(' + re.escape(kw) + r')(?!\w)',
                        r'\n\1', result, flags=re.IGNORECASE)
    lines, indented, indent = result.split('\n'), [], 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith(("SELECT","FROM","WHERE","GROUP BY","ORDER BY","HAVING",
                              "UNION","INSERT","UPDATE","MERGE","BEGIN","DECLARE")):
            indented.append(line); indent = 4
        elif upper.startswith(("LEFT JOIN","RIGHT JOIN","INNER JOIN","FULL OUTER JOIN","JOIN")):
            indented.append("  " + line)
        elif upper.startswith(("AND ","OR ")):
            indented.append(" " * indent + line)
        elif upper.startswith(("WHEN","THEN","ELSE","END")):
            indented.append("    " + line)
        else:
            indented.append(" " * indent + line)
    return '\n'.join(indented)


class DataStagePrecisionMapper:
    """Parser DataStage XML — lógica original do datastage_job_analyzer_2.py."""

    def __init__(self, filepath) -> None:
        self.filepath = Path(filepath)
        self.jobs = defaultdict(lambda: defaultdict(lambda: {
            'name': 'Unknown', 'type': 'General',
            'sqls': set(), 'tables_src': set(), 'tables_tgt': set(),
            'files': set(), 'logic': set(), 'properties': set(),
        }))
        self.stats = {'jobs': 0, 'sqls': 0}

        self.patterns = {
            'sql_select':    re.compile(r'(SELECT\s+.+?FROM\s+.+?)(?=\s*(?:WHERE|GROUP|ORDER|HAVING|UNION|MINUS|INTERSECT|$|<|&))', re.IGNORECASE | re.DOTALL),
            'sql_insert':    re.compile(r'(INSERT\s+(?:/\*[^*]*\*/\s*)?INTO\s+.+?)(?=\s*(?:INSERT|UPDATE|DELETE|MERGE|BEGIN|END|$|<|&))', re.IGNORECASE | re.DOTALL),
            'sql_update':    re.compile(r'(UPDATE\s+.+?SET\s+.+?)(?=\s*(?:WHERE|INSERT|UPDATE|DELETE|MERGE|BEGIN|END|$|<|&))', re.IGNORECASE | re.DOTALL),
            'sql_merge':     re.compile(r'(MERGE\s+INTO\s+.+?)(?=\s*(?:INSERT|UPDATE|DELETE|BEGIN|END|$|<|&))', re.IGNORECASE | re.DOTALL),
            'sql_delete':    re.compile(r'(DELETE\s+FROM\s+.+?)(?=\s*(?:WHERE|INSERT|UPDATE|MERGE|BEGIN|END|$|<|&))', re.IGNORECASE | re.DOTALL),
            'sql_plsql':     re.compile(r'(BEGIN\s+.+?END\s*;)', re.IGNORECASE | re.DOTALL),
            'sql_call':      re.compile(r'((?:CALL|EXECUTE)\s+[a-zA-Z0-9_.$#]+\s*\([^)]*\))', re.IGNORECASE | re.DOTALL),
            'tbl_src':       re.compile(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_.$#]+)', re.IGNORECASE),
            'tbl_tgt':       re.compile(r'(?:INSERT\s+(?:/\*[^*]*\*/\s*)?INTO|UPDATE|MERGE\s+INTO|DELETE\s+FROM)\s+([a-zA-Z0-9_.$#]+)', re.IGNORECASE),
            'file_unix':     re.compile(r'(/[a-zA-Z0-9_./\-]+)'),
            'file_win':      re.compile(r'([a-zA-Z]:\\[a-zA-Z0-9_.\-\\]+)'),
            'xml_tablename': re.compile(r'<TableName[^>]*>\s*<!\[CDATA\[([^\]]+)\]\]>', re.IGNORECASE),
            'xml_writemode': re.compile(r'<WriteMode[^>]*>\s*<!\[CDATA\[(\d+)\]\]>', re.IGNORECASE),
            'xml_plsql':     re.compile(r'<PlSqlStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_before_sql':re.compile(r'<BeforeSQL[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_after_sql': re.compile(r'<AfterSQL[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_select':    re.compile(r'<SelectStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_insert':    re.compile(r'<InsertStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_update':    re.compile(r'<UpdateStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
            'xml_delete_st': re.compile(r'<DeleteStatement[^>]*>\s*<!\[CDATA\[(.+?)\]\]>', re.IGNORECASE | re.DOTALL),
        }

        self.sql_output_props = {
            'WriteSQL','UpdateSQL','DeleteSQL','UpsertSQL',
            'BeforeSQL','AfterSQL','UserDefinedSQL','SQLStatement',
            'SelectStatement','PreSQL','PostSQL','BeforeAfterSQL',
            'StoredProcedure','SPName','PlSqlStatement',
            'InsertStatement','UpdateStatement','DeleteStatement',
        }

        self.xmlprop_stage_types = {
            'OracleConnectorPX','OracleConnector','PxOracleConnector',
            'DB2ConnectorPX','SybaseConnectorPX',
            'ODBCConnectorPX','SnowflakeConnectorPX','JDBCConnectorPX',
        }

        # type_map: todos os tipos conhecidos com cores dark e light
        self.type_map = {
            'PxOracleConnector':   {'label': 'Oracle Database',    'cd': '#e07b39', 'cl': '#b85c20'},
            'OracleConnectorPX':   {'label': 'Oracle Connector PX','cd': '#e07b39', 'cl': '#b85c20'},
            'OracleConnector':     {'label': 'Oracle Connector',   'cd': '#e07b39', 'cl': '#b85c20'},
            'SnowflakeConnectorPX':{'label': 'Snowflake',          'cd': '#29b5e8', 'cl': '#0090c0'},
            'CTransformerStage':   {'label': 'Transformer',        'cd': '#00ff88', 'cl': '#007a3d'},
            'CHashedFileStage':    {'label': 'Hashed Lookup',      'cd': '#a855f7', 'cl': '#7c3aed'},
            'PxLookup':            {'label': 'Lookup',             'cd': '#a855f7', 'cl': '#7c3aed'},
            'PxSequentialFile':    {'label': 'Flat File',          'cd': '#6b7280', 'cl': '#5a6470'},
            'CSeqFileStage':       {'label': 'Sequential File',    'cd': '#6b7280', 'cl': '#5a6470'},
            'PxFunnel':            {'label': 'Funnel',             'cd': '#2A9D8F', 'cl': '#1a7a6e'},
            'PxSort':              {'label': 'Sort',               'cd': '#64748b', 'cl': '#475569'},
            'PxRemoveDuplicates':  {'label': 'Remove Duplicates',  'cd': '#457B9D', 'cl': '#2d5f80'},
            'PxDataSet':           {'label': 'Data Set',           'cd': '#1D3557', 'cl': '#1D3557'},
            'CCustomStage':        {'label': 'Custom Stage',       'cd': '#9ca3af', 'cl': '#6b7280'},
        }

        self.ignored_tables = {
            "TYPE","DATE","SELECT","FROM","WHERE","AND","OR","DUAL",
            "VARCHAR","INTEGER","CHAR","TIMESTAMP","TABLE","SYSDATE","TRUNC",
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self):
        log = logger.bind(file=self.filepath.name, operation="datastage_parse")
        log.info("Parsing DataStage XML")

        content = self._load_file()
        if not content:
            return self.jobs

        soup = BeautifulSoup(content, 'xml')
        elements = soup.find_all(['Record', 'Job'])
        current_job = "Global_Context"

        for elem in elements:
            if elem.name == 'Job' or (elem.name == 'Record' and elem.get('Type') == 'JobDefn'):
                p_name = elem.find('Property', attrs={'Name': 'Name'})
                current_job = p_name.text.strip() if p_name else elem.get('Identifier', 'Unknown')
                self.stats['jobs'] += 1
                continue

            comp_id = elem.get('Identifier')
            if not comp_id:
                continue

            rec_type = elem.get('Type', '')
            if rec_type in ('TrxOutput', 'TrxInput'):
                parent_id = re.sub(r'P\d+$', '', comp_id)
                if rec_type == 'TrxOutput':
                    self._analyze_trx_output(elem, current_job, parent_id)
                continue

            p_stage_name = elem.find('Property', attrs={'Name': 'StageName'})
            p_name       = elem.find('Property', attrs={'Name': 'Name'})
            friendly_name = (p_stage_name.text.strip() if p_stage_name
                             else (p_name.text.strip() if p_name else f"Stage_{comp_id}"))
            p_type     = elem.find('Property', attrs={'Name': 'StageType'})
            stage_type = p_type.text.strip() if p_type else "Processing"

            self.jobs[current_job][comp_id]['name'] = friendly_name
            self.jobs[current_job][comp_id]['type'] = stage_type
            self._analyze_component(elem, current_job, comp_id)

        log.info("Parse complete", jobs=self.stats['jobs'], sqls=self.stats['sqls'])
        return self.jobs

    def generate_html_report(self, output_path=None):
        if not self.jobs:
            self.run()
        html_str = self._render_html()
        if output_path:
            Path(output_path).write_text(html_str, encoding='utf-8')
        return html_str

    def get_dependency_map(self):
        deps = {}
        for job_name, comps in self.jobs.items():
            sources, targets = set(), set()
            for data in comps.values():
                sources.update(data.get('tables_src', set()))
                targets.update(data.get('tables_tgt', set()))
            deps[job_name] = {'sources': sorted(sources), 'targets': sorted(targets)}
        return deps

    # ── Parsing (lógica 100% original do datastage_job_analyzer_2.py) ─────────

    def _load_file(self):
        if not self.filepath.exists():
            logger.error("File not found", path=str(self.filepath))
            return None
        try:
            return self.filepath.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            return self.filepath.read_text(encoding='latin-1')

    def _analyze_component(self, record, job, comp_id):
        stage_type = self.jobs[job][comp_id].get('type', '')

        if stage_type in self.xmlprop_stage_types:
            self._analyze_xmlproperties(record, job, comp_id)
            return

        if stage_type == 'OracleConnectorPX':
            for prop in record.find_all('Property'):
                if prop.get('Name') == 'Value' and prop.text and 'SelectStatement' in prop.text:
                    self._analyze_xmlproperties(record, job, comp_id)
                    return

        if stage_type == 'CTransformerStage':
            self._analyze_transformer(record, job, comp_id)
            return

        for item in record.find_all(['Property', 'SubProperty']):
            val  = item.text
            name = item.get('Name', '')
            if not val or len(val) < 2:
                continue
            val = str(val).strip()
            if '&lt;' in val:
                val = html.unescape(val)
            if name == 'TableName':
                self.jobs[job][comp_id]['tables_src'].add(val)
            if name in ['DestTableName', 'WriteTableName', 'TargetTableName']:
                self.jobs[job][comp_id]['tables_tgt'].add(val)
            if name == 'RefTableName':
                self.jobs[job][comp_id]['tables_src'].add(val)
            if name in ['FilePath', 'FileName', 'Directory']:
                self.jobs[job][comp_id]['files'].add(val)
            self._extract_sql(val, name, job, comp_id)
            if '/' in val or '\\' in val:
                if len(val) < 200:
                    if val.startswith('/'):
                        self.jobs[job][comp_id]['files'].add(val)
                    elif ':' in val and '\\' in val:
                        self.jobs[job][comp_id]['files'].add(val)
            if 'Derivation' in str(name) and len(val) > 1:
                self.jobs[job][comp_id]['logic'].add(f"{name}: {val}")

    def _parse_intervar_map(self, trx_code):
        imap = {}
        pat = re.compile(r'^\s*((?:InterVar|StageVar|NullSetVar)\w+)\s*=\s*([^;\n]+);', re.MULTILINE)
        for m in pat.finditer(trx_code):
            var, val = m.group(1).strip(), m.group(2).strip()
            if re.match(r'^".*"$|^\d+(\.\d+)?$|^\'.*\'$', val):
                imap[var] = val
        return imap

    def _substitute_intervars(self, expr, imap):
        def replacer(m):
            return imap.get(m.group(0), m.group(0))
        return re.sub(r'(?:InterVar|StageVar|NullSetVar)\w+', replacer, expr)

    def _build_case_when(self, col, trx_code, imap):
        lines    = trx_code.split('\n')
        branches = []
        i = 0
        while i < len(lines):
            line  = lines[i].strip()
            if_m  = re.match(r'^if\s*\((.+)\)\s*\{', line)
            if if_m:
                cond_raw    = if_m.group(1).strip()
                j, depth, block_lines = i + 1, 1, []
                while j < len(lines) and depth > 0:
                    l = lines[j].strip()
                    if l.endswith('{'):
                        depth += 1
                    if l == '}' or l == '} else {' or l.startswith('} else if'):
                        depth -= 1
                        if depth == 0:
                            break
                    block_lines.append(l)
                    j += 1
                for bl in block_lines:
                    am = re.match(r'^\w+\.' + re.escape(col) + r'\s*=\s*([^;\n]+);', bl)
                    if am:
                        branches.append((cond_raw, am.group(1).strip()))
                        break
            i += 1

        if not branches:
            return None

        ds_lines  = []
        sql_lines = ['CASE']
        for cond_raw, val_raw in branches:
            cond_resolved = self._substitute_intervars(cond_raw, imap)
            val_resolved  = self._substitute_intervars(val_raw,  imap)
            cond_sql = cond_resolved
            cond_sql = re.sub(r'\s*==\s*', ' = ', cond_sql)
            cond_sql = re.sub(r'\s*\|\|\s*', ' OR ', cond_sql)
            cond_sql = re.sub(r'\s*&&\s*', ' AND ', cond_sql)
            cond_sql = re.sub(r'(?i)\bnull\(([^)]+)\)', r'\1 IS NULL', cond_sql)
            cond_sql = re.sub(r'(?i)\bisnull\(([^)]+)\)', r'\1 IS NULL', cond_sql)
            cond_sql = re.sub(r'\b\w+\.(\w+)', r'\1', cond_sql)
            cond_sql = re.sub(r'^\(+|\)+$', '', cond_sql).strip()
            val_sql  = re.sub(r'(?i)\bset_null\(\)', 'NULL', val_resolved)
            val_sql  = re.sub(r'(?i)\bsetnull\(\)', 'NULL', val_sql)
            ternary  = re.match(r'^\(\s*(.+?)\s*\?\s*(.+?)\s*:\s*(.+?)\s*\)$', val_sql)
            if ternary:
                tc, tv, fv = ternary.group(1), ternary.group(2), ternary.group(3)
                tc = re.sub(r'\s*==\s*', ' = ', tc)
                val_sql = f"CASE WHEN {tc} THEN {tv} ELSE {fv} END"
            val_sql = re.sub(r'\b\w+\.(\w+)', r'\1', val_sql)
            ds_lines.append(f"  if ({cond_resolved}) \u2192 {val_raw}")
            sql_lines.append(f"  WHEN {cond_sql} THEN {val_sql}")

        else_pat    = re.compile(r'^\s*\w+\.' + re.escape(col) + r'\s*=\s*([^;\n]+);', re.MULTILINE)
        all_assigns = [m.group(1).strip() for m in else_pat.finditer(trx_code)]
        if all_assigns:
            else_raw      = all_assigns[-1]
            else_resolved = self._substitute_intervars(else_raw, imap)
            else_sql      = re.sub(r'(?i)\bset_null\(\)|setnull\(\)', 'NULL', else_resolved)
            else_sql      = re.sub(r'\b\w+\.(\w+)', r'\1', else_sql)
            sql_lines.append(f"  ELSE {else_sql}")
            ds_lines.append(f"  else \u2192 {else_raw}")

        sql_lines.append(f"END AS {col}")
        return '\n'.join(ds_lines), '\n'.join(sql_lines)

    def _analyze_transformer(self, record, job, comp_id):
        output_cols = []
        trx_code    = None
        for sub in record.find_all('SubRecord'):
            name_prop = sub.find('Property', attrs={'Name': 'Name'})
            val_prop  = sub.find('Property', attrs={'Name': 'Value'})
            if name_prop and val_prop and name_prop.text.strip() == 'TrxGenCode':
                trx_code = val_prop.text or ''
                break

        if trx_code:
            imap       = self._parse_intervar_map(trx_code)
            assign_pat = re.compile(r'^\s*(\w+)\.(\w+)\s*=\s*([^;\n]+(?:\([^)]*\))?[^;\n]*);', re.MULTILINE)
            col_exprs, col_order = {}, []
            for m in assign_pat.finditer(trx_code):
                link, col, expr = m.group(1), m.group(2), m.group(3).strip()
                if link.lower().startswith(('rowrej', 'nullset', 'intervar', 'stagevar')):
                    continue
                expr_clean = re.sub(r'(?i)setnull\(\)', 'set_null()', re.sub(r'\s+', ' ', expr).strip())
                if col not in col_exprs:
                    col_exprs[col] = []; col_order.append(col)
                if expr_clean not in col_exprs[col]:
                    col_exprs[col].append(expr_clean)

            for col in col_order:
                exprs          = col_exprs[col]
                exprs_filtered = [e for e in exprs if not re.match(r'^(InterVar\w+|StageVar\w+|NullSetVar\w+)$', e)]
                if not exprs_filtered:
                    exprs_filtered = exprs
                if len(exprs_filtered) == 1:
                    e = re.sub(r'(?i)setnull\(\)', 'set_null()', self._substitute_intervars(exprs_filtered[0], imap))
                    output_cols.append(col if e.lower() in (col.lower(), '0', '1') else f"{col} \u2190 {e}")
                else:
                    case_result = self._build_case_when(col, trx_code, imap)
                    if case_result:
                        ds_logic, sql_case = case_result
                        output_cols.append(f"{col} [CONDICIONAL]\n\u2500\u2500 DataStage \u2500\u2500\n{ds_logic}\n\u2500\u2500 Snowflake \u2500\u2500\n{sql_case}")
                    else:
                        exprs_resolved = [self._substitute_intervars(e, imap) for e in exprs_filtered]
                        output_cols.append(f"{col} \u2190 {' | '.join(exprs_resolved)}")

        if not output_cols:
            for col_rec in record.find_all('Collection', attrs={'Type': 'OutputColumn'}):
                for sub in col_rec.find_all('SubRecord'):
                    cn = sub.find('Property', attrs={'Name': 'Name'})
                    cd = sub.find('Property', attrs={'Name': 'ParsedDerivation'}) or \
                         sub.find('Property', attrs={'Name': 'Derivation'})
                    col_name  = cn.text.strip() if cn else None
                    col_deriv = cd.text.strip() if cd else None
                    if col_name and col_deriv and col_deriv.lower() not in ('sysdate', '0', '1', 'null', "''"):
                        output_cols.append(f"{col_name} \u2190 {col_deriv}")
                    elif col_name:
                        output_cols.append(col_name)

        if output_cols:
            self.jobs[job][comp_id]['logic'].update(output_cols)

    def _analyze_trx_output(self, record, job, parent_id):
        output_cols = []
        for col_rec in record.find_all('Collection', attrs={'Type': 'OutputColumn'}):
            for sub in col_rec.find_all('SubRecord'):
                cn = sub.find('Property', attrs={'Name': 'Name'})
                cd = sub.find('Property', attrs={'Name': 'ParsedDerivation'}) or \
                     sub.find('Property', attrs={'Name': 'Derivation'})
                col_name  = cn.text.strip() if cn else None
                col_deriv = cd.text.strip() if cd else None
                if not col_name or col_name.lower().startswith(('rowrej', 'nullset')):
                    continue
                if col_deriv and col_deriv.lower() != col_name.lower():
                    output_cols.append(f"{col_name} \u2190 {col_deriv}")
                else:
                    output_cols.append(col_name)
        if output_cols:
            if not self.jobs[job][parent_id].get('type'):
                self.jobs[job][parent_id]['type'] = 'CTransformerStage'
            if self.jobs[job][parent_id].get('name', 'Unknown') == 'Unknown':
                self.jobs[job][parent_id]['name'] = f"Transformer_{parent_id}"
            self.jobs[job][parent_id]['logic'].update(output_cols)

    def _analyze_xmlproperties(self, record, job, comp_id):
        xml_val = None
        for sub in record.find_all('SubRecord'):
            name_prop = sub.find('Property', attrs={'Name': 'Name'})
            val_prop  = sub.find('Property', attrs={'Name': 'Value'})
            if name_prop and val_prop and name_prop.text == 'XMLProperties':
                xml_val = html.unescape(val_prop.text or ''); break
        if not xml_val:
            for prop in record.find_all('Property'):
                if prop.get('Name') == 'Value' and prop.text and 'SelectStatement' in prop.text:
                    xml_val = html.unescape(prop.text); break
        if not xml_val:
            return

        wm         = self.patterns['xml_writemode'].search(xml_val)
        write_mode = int(wm.group(1)) if wm else -1
        is_target  = write_mode >= 0

        m_table = self.patterns['xml_tablename'].search(xml_val)
        if m_table:
            table = m_table.group(1).strip()
            (self.jobs[job][comp_id]['tables_tgt'] if is_target else self.jobs[job][comp_id]['tables_src']).add(table)
        else:
            for t in re.findall(r'<Table[^>]*>\s*<!\[CDATA\[([^\]]+)\]\]>\s*</Table>', xml_val, re.IGNORECASE):
                t = t.strip()
                if t and t.upper() not in self.ignored_tables:
                    (self.jobs[job][comp_id]['tables_tgt'] if is_target else self.jobs[job][comp_id]['tables_src']).add(t)

        for pat_key in ['xml_plsql','xml_before_sql','xml_after_sql','xml_select','xml_insert','xml_update','xml_delete_st']:
            m = self.patterns[pat_key].search(xml_val)
            if m and m.group(1).strip():
                self._register_sql(m.group(1).strip(), job, comp_id)

        for pat in [self.patterns['file_unix'], self.patterns['file_win']]:
            for path in pat.findall(xml_val):
                if 3 < len(path) < 200:
                    self.jobs[job][comp_id]['files'].add(path)

    def _extract_sql(self, val, prop_name, job, comp_id):
        vu        = val.upper()
        is_sp     = prop_name in self.sql_output_props
        has_sel   = 'SELECT' in vu and 'FROM' in vu
        has_dml   = any(k in vu for k in ['INSERT INTO', 'UPDATE ', 'DELETE FROM', 'MERGE INTO'])
        has_plsql = 'BEGIN' in vu and 'END' in vu
        has_call  = vu.startswith('CALL ') or vu.startswith('EXECUTE ')
        if not (is_sp or has_sel or has_dml or has_plsql or has_call):
            return
        if len(val) > 50000:
            return
        found = []
        if has_sel:
            m = self.patterns['sql_select'].search(val)
            if m: found.append(m.group(1).strip())
        if has_dml or is_sp:
            for pk in ('sql_insert','sql_update','sql_merge','sql_delete'):
                m = self.patterns[pk].search(val)
                if m: found.append(m.group(1).strip())
        if has_plsql:
            m = self.patterns['sql_plsql'].search(val)
            if m: found.append(m.group(1).strip())
        if has_call:
            m = self.patterns['sql_call'].search(val)
            if m: found.append(m.group(1).strip())
        if is_sp and not found and len(val) > 5:
            found.append(val)
        for sql_raw in found:
            self._register_sql(sql_raw, job, comp_id)

    def _register_sql(self, sql_raw, job, comp_id):
        sql = re.sub(r'\s+', ' ', sql_raw.replace(']]>', '').strip())
        if not sql.endswith(';'):
            sql += ';'
        self.jobs[job][comp_id]['sqls'].add(sql)
        self.stats['sqls'] += 1
        for t in self.patterns['tbl_src'].findall(sql):
            if len(t) > 1 and t.upper() not in self.ignored_tables:
                self.jobs[job][comp_id]['tables_src'].add(t)
        for t in self.patterns['tbl_tgt'].findall(sql):
            if len(t) > 1 and t.upper() not in self.ignored_tables:
                self.jobs[job][comp_id]['tables_tgt'].add(t)

    # ── HTML Report — dark/light sincronizado com a app ───────────────────────

    def _render_html(self) -> str:
        timestamp    = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filename     = self.filepath.name
        job_sections = ""

        for job_name, components in sorted(self.jobs.items()):
            valid_comps = {
                cid: data for cid, data in components.items()
                if (data['sqls'] or data['tables_src'] or data['tables_tgt']
                    or data['files'] or data['logic'])
                and (data['type'] in self.type_map
                     or data['sqls'] or data['tables_src'] or data['tables_tgt'])
            }
            if not valid_comps:
                continue
            cards = "".join(
                self._render_card(data)
                for _, data in sorted(valid_comps.items(), key=lambda x: x[1]['type'])
            )
            job_sections += f"""
            <section class="job-section">
              <div class="job-header">
                <span class="job-prefix">// JOB</span>
                <h2 class="job-title">{html.escape(job_name)}</h2>
                <span class="job-meta">{len(valid_comps)} stages</span>
              </div>
              <div class="cards-grid">{cards}</div>
            </section>"""

        deps     = self.get_dependency_map()
        dep_rows = "".join(
            f'<tr><td class="dep-job">{html.escape(j)}</td>'
            f'<td class="dep-src">{html.escape(", ".join(i["sources"]) or "\u2014")}</td>'
            f'<td class="dep-tgt">{html.escape(", ".join(i["targets"]) or "\u2014")}</td></tr>'
            for j, i in deps.items()
        )

        return HTML_TEMPLATE.format(
            filename=html.escape(filename),
            timestamp=timestamp,
            total_jobs=self.stats['jobs'],
            total_sqls=self.stats['sqls'],
            job_sections=job_sections,
            dep_rows=dep_rows,
        )

    def _render_card(self, data: dict) -> str:
        t    = data.get('type', '')
        meta = self.type_map.get(t, {'label': t or 'Stage', 'cd': '#9ca3af', 'cl': '#6b7280'})
        is_db = any(k in t for k in ('Connector','Oracle','DB2','Sybase','JDBC','ODBC','Snowflake'))

        src_html = ""
        if data['tables_src']:
            src_html = '<div class="section-label">source_tables</div>'
            for tbl in sorted(data['tables_src']):
                src_html += f'<span class="tag tag-src">{html.escape(tbl)}</span>'

        tgt_html = ""
        if data['tables_tgt']:
            tgt_html = '<div class="section-label">target_tables</div>'
            for tbl in sorted(data['tables_tgt']):
                tgt_html += f'<span class="tag tag-tgt">{html.escape(tbl)}</span>'

        file_html = ""
        if data['files'] and not is_db:
            file_html = '<div class="section-label">files</div>'
            for f in sorted(data['files']):
                file_html += f'<span class="tag tag-file">{html.escape(f)}</span>'

        logic_html = ""
        if data['logic']:
            lbl = "colunas \u2192 fato_final" if t == 'CTransformerStage' else "regras / derivations"
            logic_html = f'<div class="section-label">{lbl}</div><div class="logic-box">'
            for logic in sorted(data['logic']):
                if '[CONDICIONAL]' in logic:
                    parts = logic.split('\n')
                    col_header = html.escape(parts[0])
                    ds_block, sql_block, mode = [], [], None
                    for p in parts[1:]:
                        if '\u2500\u2500 DataStage' in p: mode = 'ds'
                        elif '\u2500\u2500 Snowflake' in p: mode = 'sql'
                        elif mode == 'ds':  ds_block.append(html.escape(p))
                        elif mode == 'sql': sql_block.append(html.escape(p))
                    logic_html += (
                        f'<div class="cond-block">'
                        f'<div class="cond-header">\u26a1 {col_header}</div>'
                        f'<div class="cond-grid">'
                        f'<div class="cond-ds"><span class="cond-lbl">\u25b6 DataStage</span>'
                        + '\n'.join(ds_block) +
                        f'</div><div class="cond-sql"><span class="cond-lbl">\u25b6 Snowflake</span>'
                        + '\n'.join(sql_block) +
                        f'</div></div></div>'
                    )
                else:
                    logic_html += f'<div class="logic-item">\u2022 {html.escape(logic)}</div>'
            logic_html += '</div>'

        sql_html = ""
        if data['sqls']:
            sql_html = '<div class="section-label">sql_blocks</div>'
            for i, sql in enumerate(data['sqls']):
                uid = f"sql_{abs(hash(sql)) % 0xFFFFFF}_{i}"
                sql_html += (
                    f'<div class="sql-block">'
                    f'<div class="sql-header">'
                    f'<span class="sql-label">SQL_{i+1}</span>'
                    f'<button class="copy-btn" onclick="copySql(\'{uid}\')">copy</button>'
                    f'</div>'
                    f'<pre id="{uid}" class="language-sql">'
                    f'<code class="language-sql">{html.escape(_indent_sql(sql))}</code>'
                    f'</pre></div>'
                )

        return (
            f'<div class="card" data-dark="{meta["cd"]}" data-light="{meta["cl"]}" '
            f'style="--card-color:{meta["cl"]}">'
            f'<div class="card-header">'
            f'<div class="card-title">{html.escape(data["name"])}</div>'
            f'<div class="card-type">{html.escape(meta["label"])}</div>'
            f'</div>'
            f'<div class="card-body">{src_html}{tgt_html}{file_html}{logic_html}{sql_html}</div>'
            f'</div>'
        )


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MAF // {filename}</title>
<script>
  try {{
    var t = localStorage.getItem('maf-theme');
    if (t) document.documentElement.setAttribute('data-theme', t);
  }} catch(e) {{}}
</script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:#f4f3ef;--bg2:#fff;--bg3:#eeede8;--bdr:#dddcd4;--bhv:#f0efe9;
  --acc:#007a3d;--adm:#005c2a;--amut:#004d22;--abg:rgba(0,122,61,.06);--abrd:rgba(0,122,61,.25);
  --tp:#1a1a18;--ts:#444440;--tm:#88887e;
  --err:#cc2200;--wrn:#b35c00;--inf:#1a5fa8;
  --tag-src-bg:#fff3e0;--tag-src-c:#8c3a10;--tag-src-b:#c05818;
  --tag-tgt-bg:#e6f4ec;--tag-tgt-c:#004d26;--tag-tgt-b:#007a3d;
  --dep-src:#8c3a10;
  --fm:'JetBrains Mono','Courier New',monospace;--r:2px;
}}
[data-theme="dark"] {{
  --bg:#0d0d0d;--bg2:#111;--bg3:#161616;--bdr:#1a1a1a;--bhv:#1f1f1f;
  --acc:#00ff88;--adm:#00cc66;--amut:#007a3d;--abg:rgba(0,255,136,.06);--abrd:rgba(0,255,136,.2);
  --tp:#f0f0f0;--ts:#888;--tm:#555;
  --err:#ff4444;--wrn:#ffcc00;--inf:#4488ff;
  --tag-src-bg:rgba(244,162,97,.08);--tag-src-c:#f4a261;--tag-src-b:#f4a261;
  --tag-tgt-bg:rgba(0,255,136,.07);--tag-tgt-c:#00ff88;--tag-tgt-b:#00ff88;
  --dep-src:#f4a261;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html,body{{font-family:var(--fm);background:var(--bg);color:var(--tp);font-size:13px;-webkit-font-smoothing:antialiased;transition:background .2s,color .2s}}
::-webkit-scrollbar{{width:5px;height:5px}}
::-webkit-scrollbar-track{{background:var(--bg2)}}
::-webkit-scrollbar-thumb{{background:var(--bdr);border-radius:2px}}
::selection{{background:var(--abg);color:var(--tp)}}

.site-header{{background:var(--bg2);border-bottom:1px solid var(--bdr);padding:12px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;transition:background .2s,border-color .2s}}
.header-brand{{display:flex;align-items:baseline;gap:10px}}
.header-maf{{font-size:15px;font-weight:700;letter-spacing:.08em;color:var(--tp)}}
.header-sep{{color:var(--tm)}}
.header-file{{font-size:13px;color:var(--acc)}}
.header-right{{display:flex;align-items:center;gap:20px}}
.header-meta{{font-size:11px;color:var(--tm);display:flex;gap:18px}}
.header-meta b{{color:var(--acc)}}

.theme-toggle{{display:flex;align-items:center;gap:7px;background:var(--bg3);border:1px solid var(--bdr);border-radius:20px;padding:4px 8px 4px 6px;cursor:pointer;transition:border-color .15s,background .15s}}
.theme-toggle:hover{{border-color:var(--acc);background:var(--abg)}}
.theme-toggle:hover .toggle-icon{{color:var(--acc)}}
.toggle-icon{{color:var(--tm);display:flex;align-items:center;transition:color .15s}}
.toggle-track{{width:28px;height:16px;background:var(--bdr);border-radius:8px;position:relative;transition:background .2s}}
.theme-toggle:hover .toggle-track{{background:var(--amut)}}
.toggle-thumb{{width:10px;height:10px;background:var(--tm);border-radius:50%;position:absolute;top:3px;left:3px;transition:transform .2s,background .2s}}
.thumb-on{{transform:translateX(12px);background:var(--acc)}}

.stats-bar{{background:var(--bg3);border-bottom:1px solid var(--bdr);padding:10px 28px;display:flex;gap:28px}}
.stat-label{{font-size:9px;color:var(--tm);text-transform:uppercase;letter-spacing:.1em}}
.stat-value{{font-size:20px;font-weight:700;color:var(--acc);line-height:1}}

.main{{padding:28px;max-width:1400px;margin:0 auto}}

.section-label{{font-size:10px;color:var(--tm);text-transform:uppercase;letter-spacing:.08em;margin:10px 0 6px;display:flex;align-items:center;gap:6px}}
.section-label::before{{content:'//';color:var(--acc)}}
.section-label::after{{content:'';flex:1;height:1px;background:var(--bdr);margin-left:4px}}

.job-section{{margin-bottom:44px}}
.job-header{{display:flex;align-items:baseline;gap:12px;margin-bottom:14px;padding-bottom:10px;border-bottom:1px solid var(--bdr)}}
.job-prefix{{font-size:11px;color:var(--acc);opacity:.6}}
.job-title{{font-size:16px;font-weight:600;color:var(--tp)}}
.job-meta{{font-size:11px;color:var(--tm);margin-left:auto}}

.cards-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}}
.card{{background:var(--bg2);border:1px solid var(--bdr);border-top:2px solid var(--card-color,var(--acc));border-radius:var(--r);display:flex;flex-direction:column;max-height:560px;overflow:hidden;transition:background .2s,border-color .2s}}
.card-header{{display:flex;flex-direction:column;padding:11px 14px 9px;border-bottom:1px solid var(--bdr);background:var(--bg3);flex-shrink:0}}
.card-title{{font-size:13px;font-weight:600;color:var(--tp);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.card-type{{font-size:10px;color:var(--tm);margin-top:2px}}
.card-body{{padding:12px 14px;overflow-y:auto;flex:1;min-height:0}}

.tag{{display:inline-block;font-size:11px;padding:3px 8px;border-radius:1px;margin:2px 3px 2px 0;border-left:2px solid;font-family:var(--fm);word-break:break-all}}
.tag-src{{background:var(--tag-src-bg);border-color:var(--tag-src-b);color:var(--tag-src-c)}}
.tag-tgt{{background:var(--tag-tgt-bg);border-color:var(--tag-tgt-b);color:var(--tag-tgt-c)}}
.tag-file{{background:rgba(100,100,100,.08);border-color:#999;color:var(--ts)}}

.logic-box{{font-size:11px;color:var(--ts);max-height:180px;overflow-y:auto;background:var(--bg3);border:1px solid var(--bdr);padding:7px;border-radius:var(--r);margin-top:4px}}
.logic-item{{padding:2px 0;border-bottom:1px solid var(--bdr);line-height:1.6;white-space:pre-wrap;word-break:break-all}}
.logic-item:last-child{{border-bottom:none}}

.cond-block{{margin-bottom:8px;border:1px solid var(--bdr);border-radius:2px;overflow:hidden}}
.cond-header{{background:#2d2d2d;color:#e6db74;padding:4px 8px;font-weight:600;font-size:11px}}
[data-theme="light"] .cond-header{{background:#2a2a2a;color:#d4b800}}
.cond-grid{{display:grid;grid-template-columns:1fr 1fr}}
.cond-ds{{padding:6px;border-right:1px solid var(--bdr);background:#1e1e1e;color:#9cdcfe;font-size:10px;white-space:pre-wrap;font-family:var(--fm)}}
.cond-sql{{padding:6px;background:#1a1a2e;color:#4ec9b0;font-size:10px;white-space:pre-wrap;font-family:var(--fm)}}
.cond-lbl{{display:block;color:#555;font-size:9px;margin-bottom:4px}}

.sql-block{{margin-top:10px;border:1px solid var(--bdr);border-radius:var(--r);overflow:hidden}}
.sql-header{{display:flex;align-items:center;justify-content:space-between;padding:5px 10px;background:var(--bg3);border-bottom:1px solid var(--bdr)}}
.sql-label{{font-size:9px;color:var(--acc);text-transform:uppercase;letter-spacing:.1em}}
.copy-btn{{background:rgba(128,128,128,.1);border:1px solid var(--bdr);color:var(--tm);padding:2px 8px;border-radius:1px;cursor:pointer;font-size:10px;font-family:var(--fm);transition:all .15s}}
.copy-btn:hover{{background:var(--abg);color:var(--acc);border-color:var(--abrd)}}
.copy-btn.copied{{color:var(--acc)}}
pre[class*="language-"]{{background:#0a0a0a!important;margin:0!important;border-radius:0!important;max-height:280px;overflow-y:auto;font-size:11.5px!important;line-height:1.7!important;padding:12px 14px!important;white-space:pre-wrap!important;word-break:break-word!important}}
code[class*="language-"]{{font-family:var(--fm)!important}}

.dep-section{{margin-top:44px;padding-top:22px;border-top:1px solid var(--bdr)}}
.dep-title{{font-size:14px;font-weight:600;color:var(--acc);margin-bottom:14px}}
.dep-title::before{{content:'// ';opacity:.6;font-size:11px}}
table.dep-table{{width:100%;border-collapse:collapse;font-size:12px}}
.dep-table th{{text-align:left;padding:8px 12px;font-size:10px;color:var(--acc);border-bottom:1px solid var(--bdr);text-transform:uppercase;letter-spacing:.06em}}
.dep-table td{{padding:7px 12px;border-bottom:1px solid var(--bdr);vertical-align:top;word-break:break-word}}
.dep-table tbody tr:hover td{{background:var(--bhv)}}
.dep-job{{color:var(--acc);font-weight:500}}
.dep-src{{color:var(--dep-src)}}

.site-footer{{margin-top:48px;padding:16px 28px;border-top:1px solid var(--bdr);font-size:11px;color:var(--tm);display:flex;justify-content:space-between}}
</style>
</head>
<body>
<header class="site-header">
  <div class="header-brand">
    <span class="header-maf">MAF</span>
    <span class="header-sep">//</span>
    <span class="header-file">{filename}</span>
  </div>
  <div class="header-right">
    <div class="header-meta">
      <span>generated <b>{timestamp}</b></span>
      <span>jobs <b>{total_jobs}</b></span>
      <span>sql_blocks <b>{total_sqls}</b></span>
    </div>
    <button class="theme-toggle" onclick="toggleTheme()" id="theme-btn" aria-label="Toggle theme">
      <span class="toggle-icon" id="toggle-icon">
        <svg id="icon-moon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
        <svg id="icon-sun"  width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display:none"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>
      </span>
      <div class="toggle-track"><div class="toggle-thumb" id="toggle-thumb"></div></div>
    </button>
  </div>
</header>
<div class="stats-bar">
  <div class="stat"><div class="stat-label">jobs_parsed</div><div class="stat-value">{total_jobs}</div></div>
  <div class="stat"><div class="stat-label">sql_blocks</div><div class="stat-value">{total_sqls}</div></div>
</div>
<main class="main">
  {job_sections}
  <div class="dep-section">
    <div class="dep-title">dependency_map</div>
    <table class="dep-table">
      <thead><tr><th>job</th><th>source_tables</th><th>target_tables</th></tr></thead>
      <tbody>{dep_rows}</tbody>
    </table>
  </div>
</main>
<footer class="site-footer">
  <span>MAF // Migration Acceleration Framework v4.0</span>
  <span>Oracle \u2192 Snowflake // Algar Telecom</span>
</footer>
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
<script>
function applyTheme(t) {{
  document.documentElement.setAttribute('data-theme', t);
  var d = t === 'dark';
  document.getElementById('icon-moon').style.display = d ? 'none'  : 'block';
  document.getElementById('icon-sun').style.display  = d ? 'block' : 'none';
  var th = document.getElementById('toggle-thumb');
  d ? th.classList.add('thumb-on') : th.classList.remove('thumb-on');
  document.querySelectorAll('.card[data-dark]').forEach(function(c) {{
    c.style.setProperty('--card-color', d ? c.dataset.dark : c.dataset.light);
  }});
}}
function toggleTheme() {{
  var next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  applyTheme(next);
  try {{ localStorage.setItem('maf-theme', next); }} catch(e) {{}}
}}
(function() {{
  applyTheme(document.documentElement.getAttribute('data-theme') || 'light');
}})();
function copySql(id) {{
  var el = document.getElementById(id);
  if (!el) return;
  navigator.clipboard.writeText(el.textContent).then(function() {{
    var btn = el.closest('.sql-block').querySelector('.copy-btn');
    if (btn) {{ btn.textContent = 'copied!'; btn.classList.add('copied'); setTimeout(function() {{ btn.textContent = 'copy'; btn.classList.remove('copied'); }}, 1800); }}
  }});
}}
window.addEventListener('load', function() {{ if (window.Prism) Prism.highlightAll(); }});
</script>
</body>
</html>
"""