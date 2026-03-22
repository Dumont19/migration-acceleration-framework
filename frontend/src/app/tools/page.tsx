'use client'

import { useState } from 'react'
import { SectionLabel, StatCard } from '@/components/ui'

// ── Types ──────────────────────────────────────────────────────────────────

interface MetadataResult {
  table_name: string
  columns: { name: string; type: string; nullable: boolean; comment: string | null }[]
  total_columns: number
  source_schema: string
}

interface CreateTableResult {
  ddl: string
  table: string
  schema: string
  created: boolean
  message: string
}

interface CopyResult {
  rows_loaded: number
  table: string
  s3_key: string
  status: string
}

interface MergeResult {
  rows_inserted: number
  rows_updated: number
  table: string
  status: string
}

type ToolStatus = 'idle' | 'running' | 'done' | 'error'

// ── Page ───────────────────────────────────────────────────────────────────

export default function ToolsPage() {
  const [tableName, setTableName] = useState('F_CEL_NETWORK_EVENT')
  const [schemaSource, setSchemaSource] = useState('DWADM')
  const [schemaTarget, setSchemaTarget] = useState('DWADM')
  const [s3Key, setS3Key] = useState('')
  const [partitionDate, setPartitionDate] = useState('')

  const [metaStatus, setMetaStatus] = useState<ToolStatus>('idle')
  const [metaResult, setMetaResult] = useState<MetadataResult | null>(null)
  const [metaError, setMetaError] = useState('')

  const [createStatus, setCreateStatus] = useState<ToolStatus>('idle')
  const [createResult, setCreateResult] = useState<CreateTableResult | null>(null)
  const [createError, setCreateError] = useState('')

  const [copyStatus, setCopyStatus] = useState<ToolStatus>('idle')
  const [copyResult, setCopyResult] = useState<CopyResult | null>(null)
  const [copyError, setCopyError] = useState('')

  const [mergeStatus, setMergeStatus] = useState<ToolStatus>('idle')
  const [mergeResult, setMergeResult] = useState<MergeResult | null>(null)
  const [mergeError, setMergeError] = useState('')

  // ── Handlers ──────────────────────────────────────────────────────────────

  async function runExtractMetadata() {
    setMetaStatus('running')
    setMetaResult(null)
    setMetaError('')
    try {
      const res = await fetch(
        `/api/tools/metadata?table=${encodeURIComponent(tableName)}&schema=${encodeURIComponent(schemaSource)}`
      )
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Error extracting metadata')
      setMetaResult(data)
      setMetaStatus('done')
    } catch (e: unknown) {
      setMetaError(e instanceof Error ? e.message : String(e))
      setMetaStatus('error')
    }
  }

  async function runCreateTable() {
    setCreateStatus('running')
    setCreateResult(null)
    setCreateError('')
    try {
      const res = await fetch('/api/tools/create-table', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: tableName,
          schema_source: schemaSource,
          schema_target: schemaTarget,
        }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Error creating table')
      setCreateResult(data)
      setCreateStatus('done')
    } catch (e: unknown) {
      setCreateError(e instanceof Error ? e.message : String(e))
      setCreateStatus('error')
    }
  }

  async function runCopyInto() {
    setCopyStatus('running')
    setCopyResult(null)
    setCopyError('')
    try {
      const res = await fetch('/api/tools/copy-into', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: tableName,
          schema: schemaTarget,
          s3_key: s3Key,
        }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Error running COPY INTO')
      setCopyResult(data)
      setCopyStatus('done')
    } catch (e: unknown) {
      setCopyError(e instanceof Error ? e.message : String(e))
      setCopyStatus('error')
    }
  }

  async function runMerge() {
    setMergeStatus('running')
    setMergeResult(null)
    setMergeError('')
    try {
      const res = await fetch('/api/tools/merge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: tableName,
          schema: schemaTarget,
          partition_date: partitionDate || undefined,
        }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || 'Error running MERGE')
      setMergeResult(data)
      setMergeStatus('done')
    } catch (e: unknown) {
      setMergeError(e instanceof Error ? e.message : String(e))
      setMergeStatus('error')
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  function statusDot(s: ToolStatus) {
    if (s === 'running') return <span className="status-dot status-dot--warn" style={{ animation: 'pulse 1s infinite' }} />
    if (s === 'done')    return <span className="status-dot status-dot--ok" />
    if (s === 'error')   return <span className="status-dot status-dot--error" />
    return null
  }

  function copyDDL(ddl: string) {
    navigator.clipboard.writeText(ddl).catch(() => {})
  }

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div>
      <p style={{ fontSize: 'var(--font-size-sm)', color: 'var(--text-secondary)', marginBottom: 24 }}>
        Standalone operations — extract Oracle metadata, create Snowflake tables, run COPY INTO from S3, and execute MERGE.
      </p>

      {/* Config */}
      <SectionLabel>tool_config</SectionLabel>
      <div className="card" style={{ marginBottom: 20 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr 1fr', gap: 12 }}>
          <div>
            <label style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>
              table_name *
            </label>
            <input
              className="form-input"
              value={tableName}
              onChange={e => setTableName(e.target.value.toUpperCase())}
              placeholder="F_CEL_NETWORK_EVENT"
            />
          </div>
          <div>
            <label style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>
              schema_source
            </label>
            <input
              className="form-input"
              value={schemaSource}
              onChange={e => setSchemaSource(e.target.value.toUpperCase())}
              placeholder="DWADM"
            />
          </div>
          <div>
            <label style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>
              schema_target
            </label>
            <input
              className="form-input"
              value={schemaTarget}
              onChange={e => setSchemaTarget(e.target.value.toUpperCase())}
              placeholder="DWADM"
            />
          </div>
        </div>
      </div>

      {/* Tool grid */}
      <div className="grid-2" style={{ gap: 16 }}>

        {/* 01 — Extract Metadata */}
        <div className="card card--active">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--accent-primary)', opacity: 0.6 }}>/01</span>
            <span style={{ fontWeight: 600 }}>extract_metadata</span>
            {statusDot(metaStatus)}
          </div>
          <p style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 14, lineHeight: 1.6 }}>
            Extract column structure and comments from Oracle. Source: <code style={{ color: 'var(--accent-primary)' }}>{schemaSource}.{tableName}</code>
          </p>
          <button
            className="btn btn--primary"
            style={{ width: '100%' }}
            onClick={runExtractMetadata}
            disabled={metaStatus === 'running' || !tableName}
          >
            {metaStatus === 'running' ? '> extracting...' : '> extract_metadata'}
          </button>
          {metaError && (
            <div style={{ marginTop: 10, fontSize: 'var(--font-size-xs)', color: 'var(--status-error)' }}>
              ✗ {metaError}
            </div>
          )}
          {metaResult && (
            <div style={{ marginTop: 12 }}>
              <div style={{ display: 'flex', gap: 16, marginBottom: 10 }}>
                <StatCard label="columns" value={metaResult.total_columns} />
                <StatCard label="source" value={metaResult.source_schema} />
              </div>
              <div className="log-stream" style={{ maxHeight: 200, overflowY: 'auto' }}>
                {metaResult.columns.map((col, i) => (
                  <div key={i} style={{ borderBottom: '1px solid var(--bg-border)', padding: '3px 0', display: 'flex', gap: 12 }}>
                    <span style={{ color: 'var(--accent-primary)', minWidth: 160, fontSize: 'var(--font-size-xs)' }}>{col.name}</span>
                    <span style={{ color: 'var(--text-secondary)', minWidth: 120, fontSize: 'var(--font-size-xs)' }}>{col.type}</span>
                    <span style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>{col.comment || ''}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* 02 — Create Table */}
        <div className="card card--active">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--accent-primary)', opacity: 0.6 }}>/02</span>
            <span style={{ fontWeight: 600 }}>create_snowflake_table</span>
            {statusDot(createStatus)}
          </div>
          <p style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 14, lineHeight: 1.6 }}>
            Generate DDL from Oracle structure and create <code style={{ color: 'var(--accent-primary)' }}>{schemaTarget}.{tableName}</code> + <code style={{ color: 'var(--accent-primary)' }}>{tableName}_RAW</code> in Snowflake.
          </p>
          <button
            className="btn btn--primary"
            style={{ width: '100%' }}
            onClick={runCreateTable}
            disabled={createStatus === 'running' || !tableName}
          >
            {createStatus === 'running' ? '> creating...' : '> create_table'}
          </button>
          {createError && (
            <div style={{ marginTop: 10, fontSize: 'var(--font-size-xs)', color: 'var(--status-error)' }}>
              ✗ {createError}
            </div>
          )}
          {createResult && (
            <div style={{ marginTop: 12 }}>
              <div style={{
                fontSize: 'var(--font-size-xs)',
                color: createResult.created ? 'var(--accent-primary)' : 'var(--status-warn)',
                marginBottom: 8,
              }}>
                {createResult.created ? '✓' : '○'} {createResult.message}
              </div>
              {createResult.ddl && (
                <div style={{ position: 'relative' }}>
                  <button
                    onClick={() => copyDDL(createResult.ddl)}
                    style={{
                      position: 'absolute', top: 6, right: 6, zIndex: 1,
                      background: 'rgba(0,0,0,0.4)', border: '1px solid var(--bg-border)',
                      color: 'var(--text-muted)', padding: '2px 8px',
                      borderRadius: 1, cursor: 'pointer', fontSize: 'var(--font-size-xs)',
                      fontFamily: 'var(--font-mono)',
                    }}
                  >
                    copy
                  </button>
                  <pre className="log-stream" style={{ maxHeight: 160, overflowY: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                    {createResult.ddl}
                  </pre>
                </div>
              )}
            </div>
          )}
        </div>

        {/* 03 — COPY INTO */}
        <div className="card card--active">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--accent-primary)', opacity: 0.6 }}>/03</span>
            <span style={{ fontWeight: 600 }}>copy_into_snowflake</span>
            {statusDot(copyStatus)}
          </div>
          <p style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 14, lineHeight: 1.6 }}>
            Load a CSV.GZ file from S3 into <code style={{ color: 'var(--accent-primary)' }}>{tableName}_RAW</code> via COPY INTO.
          </p>
          <div style={{ marginBottom: 10 }}>
            <label style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>
              s3_key
            </label>
            <input
              className="form-input"
              value={s3Key}
              onChange={e => setS3Key(e.target.value)}
              placeholder="F_CEL_NETWORK_EVENT/2024-01-01/file.csv.gz"
            />
          </div>
          <button
            className="btn btn--primary"
            style={{ width: '100%' }}
            onClick={runCopyInto}
            disabled={copyStatus === 'running' || !tableName || !s3Key}
          >
            {copyStatus === 'running' ? '> loading...' : '> run_copy_into'}
          </button>
          {copyError && (
            <div style={{ marginTop: 10, fontSize: 'var(--font-size-xs)', color: 'var(--status-error)' }}>
              ✗ {copyError}
            </div>
          )}
          {copyResult && (
            <div style={{ marginTop: 12, display: 'flex', gap: 16 }}>
              <StatCard label="rows_loaded" value={copyResult.rows_loaded.toLocaleString()} />
              <StatCard label="status" value={copyResult.status} />
            </div>
          )}
        </div>

        {/* 04 — MERGE */}
        <div className="card card--active">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14 }}>
            <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--accent-primary)', opacity: 0.6 }}>/04</span>
            <span style={{ fontWeight: 600 }}>run_merge</span>
            {statusDot(mergeStatus)}
          </div>
          <p style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 14, lineHeight: 1.6 }}>
            Execute MERGE from <code style={{ color: 'var(--accent-primary)' }}>{tableName}_RAW</code> into <code style={{ color: 'var(--accent-primary)' }}>{tableName}</code>. Optionally filter by partition date.
          </p>
          <div style={{ marginBottom: 10 }}>
            <label style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', display: 'block', marginBottom: 4 }}>
              partition_date (optional)
            </label>
            <input
              className="form-input"
              type="date"
              value={partitionDate}
              onChange={e => setPartitionDate(e.target.value)}
            />
          </div>
          <button
            className="btn btn--primary"
            style={{ width: '100%' }}
            onClick={runMerge}
            disabled={mergeStatus === 'running' || !tableName}
          >
            {mergeStatus === 'running' ? '> merging...' : '> run_merge'}
          </button>
          {mergeError && (
            <div style={{ marginTop: 10, fontSize: 'var(--font-size-xs)', color: 'var(--status-error)' }}>
              ✗ {mergeError}
            </div>
          )}
          {mergeResult && (
            <div style={{ marginTop: 12, display: 'flex', gap: 16 }}>
              <StatCard label="inserted" value={mergeResult.rows_inserted.toLocaleString()} />
              <StatCard label="updated" value={mergeResult.rows_updated.toLocaleString()} />
              <StatCard label="status" value={mergeResult.status} />
            </div>
          )}
        </div>

      </div>

      <div style={{ marginTop: 32 }}>
        <SectionLabel>// load a table config above and run each operation independently</SectionLabel>
      </div>
    </div>
  )
}
