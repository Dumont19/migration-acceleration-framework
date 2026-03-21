'use client'

import { useState, useRef, useCallback } from 'react'
import { SectionLabel, EmptyState, CodeBlock, StatCard } from '@/components/ui'

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

interface JobSummary {
  job_name: string
  job_type: string
  source_tables: string[]
  target_tables: string[]
  sql_queries: string[]
  stages: {
    stage_name: string
    stage_type: string
    table_name: string | null
    is_write: boolean
    source_tables: string[]
    target_tables: string[]
    sql_count: number
    sqls: string[]
    logic_count: number
  }[]
}

export default function DocsPage() {
  const [xmlContent, setXmlContent] = useState('')
  const [fileName, setFileName] = useState('')
  const [jobs, setJobs] = useState<JobSummary[]>([])
  const [selectedJob, setSelectedJob] = useState<JobSummary | null>(null)
  const [loading, setLoading] = useState(false)
  const [reportLoading, setReportLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setFileName(file.name)
    const reader = new FileReader()
    reader.onload = (ev) => {
      setXmlContent(ev.target?.result as string ?? '')
      setJobs([])
      setSelectedJob(null)
      setError(null)
    }
    reader.readAsText(file, 'utf-8')
  }

  /**
   * Sempre envia como File — evita o limite de 1MB do Form field.
   * Se o usuario colou texto, cria um File em memoria.
   * Se carregou arquivo fisico, usa ele diretamente.
   */
  const buildFormWithFile = useCallback((): FormData => {
    const form = new FormData()
    const physicalFile = fileInputRef.current?.files?.[0]
    if (physicalFile) {
      form.append('file', physicalFile)
    } else {
      const inMemoryFile = new File(
        [xmlContent],
        fileName || 'upload.xml',
        { type: 'application/xml' }
      )
      form.append('file', inMemoryFile)
    }
    return form
  }, [xmlContent, fileName])

  const parseXml = useCallback(async () => {
    if (!xmlContent.trim()) { setError('Load a .xml file first'); return }
    setError(null)
    setLoading(true)
    try {
      const form = buildFormWithFile()
      const res = await fetch(`${API_BASE}/api/datastage/analyze`, {
        method: 'POST',
        body: form,
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }))
        throw new Error((err as { detail?: string }).detail ?? 'Parse failed')
      }
      const data: { jobs: JobSummary[] } = await res.json()
      setJobs(data.jobs)
      setSelectedJob(data.jobs[0] ?? null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to parse XML')
    } finally {
      setLoading(false)
    }
  }, [xmlContent, buildFormWithFile])

  const openHtmlReport = useCallback(async () => {
    if (!xmlContent.trim()) { setError('Load a .dsx / .xml file first'); return }
    setError(null)
    setReportLoading(true)
    try {
      const form = buildFormWithFile()
      form.append('inline', 'true')
      const res = await fetch(`${API_BASE}/api/datastage/report`, {
        method: 'POST',
        body: form,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const htmlBlob = await res.blob()
      const url = URL.createObjectURL(htmlBlob)
      window.open(url, '_blank', 'noopener')
      setTimeout(() => URL.revokeObjectURL(url), 60_000)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to generate report')
    } finally {
      setReportLoading(false)
    }
  }, [xmlContent, buildFormWithFile])

  const downloadReport = useCallback(async () => {
    if (!xmlContent.trim()) return
    setReportLoading(true)
    try {
      const form = buildFormWithFile()
      form.append('inline', 'false')
      const res = await fetch(`${API_BASE}/api/datastage/report`, {
        method: 'POST',
        body: form,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const blob = await res.blob()
      const a = document.createElement('a')
      a.href = URL.createObjectURL(blob)
      a.download = fileName.replace(/\.(dsx|xml)$/i, '') + '_report.html'
      a.click()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Download failed')
    } finally {
      setReportLoading(false)
    }
  }, [xmlContent, fileName, buildFormWithFile])

  const totalStages = jobs.reduce((s, j) => s + j.stages.length, 0)
  const totalSql = jobs.reduce((s, j) => s + j.stages.reduce((a, st) => a + st.sql_count, 0), 0)

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Auto-generate documentation from DataStage .xml export files.
        SQL blocks are highlighted and indented in the HTML report.
      </p>

      <SectionLabel>load_datastage_xml</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

          <div
            onClick={() => fileInputRef.current?.click()}
            style={{
              border: `1px dashed ${xmlContent ? 'var(--accent-muted)' : 'var(--bg-border)'}`,
              borderRadius: 'var(--radius)',
              padding: '24px',
              textAlign: 'center',
              cursor: 'pointer',
              background: xmlContent ? 'var(--accent-bg)' : 'var(--bg-primary)',
              transition: 'all 0.15s',
            }}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".dsx,.xml"
              onChange={handleFileChange}
              style={{ display: 'none' }}
            />
            {xmlContent ? (
              <div>
                <div style={{ color: 'var(--accent)', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>
                  ✓ {fileName}
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                  {(xmlContent.length / 1024).toFixed(1)} KB loaded · click to change
                </div>
              </div>
            ) : (
              <div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>
                  click to upload .dsx / .xml
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                  or paste XML content below
                </div>
              </div>
            )}
          </div>

          <textarea
            className="form-input"
            rows={3}
            placeholder="Or paste DataStage XML content directly here..."
            value={xmlContent}
            onChange={e => { setXmlContent(e.target.value); setFileName('pasted.xml') }}
            style={{ resize: 'vertical', fontSize: 'var(--font-size-xs)', lineHeight: 1.6 }}
          />

          {error && (
            <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>
              ✗ {error}
            </div>
          )}

          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <button className="btn btn--outline" onClick={parseXml} disabled={loading || !xmlContent}>
              {loading ? '// parsing...' : '> parse_xml'}
            </button>
            <button className="btn btn--primary" onClick={openHtmlReport} disabled={reportLoading || !xmlContent}>
              {reportLoading ? '// generating...' : '> open_report_in_browser ↗'}
            </button>
            {xmlContent && (
              <button className="btn btn--ghost" onClick={downloadReport} disabled={reportLoading}>
                ↓ download_html
              </button>
            )}
          </div>

          <div style={{
            fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)',
            padding: '8px 12px', background: 'var(--bg-primary)',
            border: '1px solid var(--bg-border)', borderLeft: '2px solid var(--accent-muted)',
            borderRadius: 'var(--radius)',
          }}>
            ℹ <b style={{ color: 'var(--text-secondary)' }}>open_report_in_browser</b> — opens full HTML
            report with SQL syntax highlighting (Prism.js), auto-indented queries and cards per stage.{' '}
            <b style={{ color: 'var(--text-secondary)' }}>parse_xml</b> shows a structured preview here.
          </div>
        </div>
      </div>

      {jobs.length > 0 && (
        <>
          <SectionLabel>parse_summary</SectionLabel>
          <div className="grid-4" style={{ marginBottom: 24 }}>
            <StatCard label="jobs_found"    value={jobs.length} />
            <StatCard label="total_stages"  value={totalStages} />
            <StatCard label="sql_blocks"    value={totalSql} accent={totalSql > 0} />
            <StatCard label="source_tables" value={jobs.reduce((s, j) => s + j.source_tables.length, 0)} />
          </div>
        </>
      )}

      {jobs.length === 0 ? (
        <EmptyState message="load a .xml file and click parse_xml or open_report_in_browser" />
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '200px 1fr', gap: 20, alignItems: 'start' }}>

          <div className="card" style={{ padding: 0 }}>
            <div style={{
              padding: '9px 14px',
              borderBottom: '1px solid var(--bg-border)',
              fontSize: 'var(--font-size-xs)',
              color: 'var(--text-muted)',
            }}>
              {jobs.length} job{jobs.length !== 1 ? 's' : ''}
            </div>
            {jobs.map(job => (
              <button
                key={job.job_name}
                onClick={() => setSelectedJob(job)}
                style={{
                  display: 'block', width: '100%', textAlign: 'left', padding: '9px 14px',
                  background: selectedJob?.job_name === job.job_name ? 'var(--accent-bg)' : 'transparent',
                  borderLeft: `2px solid ${selectedJob?.job_name === job.job_name ? 'var(--accent-primary)' : 'transparent'}`,
                  border: 'none', borderBottom: '1px solid var(--bg-border)',
                  color: selectedJob?.job_name === job.job_name ? 'var(--accent-primary)' : 'var(--text-secondary)',
                  fontSize: 'var(--font-size-xs)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                }}
              >
                <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {job.job_name}
                </div>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', marginTop: 2 }}>
                  {job.stages.length} stages
                </div>
              </button>
            ))}
          </div>

          {selectedJob && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              <div className="card card--active">
                <div style={{ fontSize: 'var(--font-size-xl)', fontWeight: 700, color: 'var(--accent)', marginBottom: 6 }}>
                  {selectedJob.job_name}
                </div>
                <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                  {selectedJob.stages.length} stages · {selectedJob.source_tables.length} sources · {selectedJob.target_tables.length} targets
                </div>
              </div>

              <div className="grid-2">
                <div className="card">
                  <SectionLabel>source_tables</SectionLabel>
                  {selectedJob.source_tables.length === 0
                    ? <span style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>none detected</span>
                    : selectedJob.source_tables.map(t => (
                      <div key={t} style={{ color: '#f4a261', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>● {t}</div>
                    ))
                  }
                </div>
                <div className="card">
                  <SectionLabel>target_tables</SectionLabel>
                  {selectedJob.target_tables.length === 0
                    ? <span style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>none detected</span>
                    : selectedJob.target_tables.map(t => (
                      <div key={t} style={{ color: 'var(--status-warn)', fontSize: 'var(--font-size-sm)', marginBottom: 4 }}>● {t}</div>
                    ))
                  }
                </div>
              </div>

              <div className="card">
                <SectionLabel>stages ({selectedJob.stages.length})</SectionLabel>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>stage_name</th>
                      <th>type</th>
                      <th>table</th>
                      <th>mode</th>
                      <th>sql</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedJob.stages.map(s => (
                      <tr key={s.stage_name}>
                        <td style={{ color: 'var(--accent-primary)' }}>{s.stage_name}</td>
                        <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>{s.stage_type}</td>
                        <td style={{ fontSize: 'var(--font-size-xs)' }}>{s.table_name ?? '—'}</td>
                        <td>
                          {s.is_write
                            ? <span className="badge badge--running">WRITE</span>
                            : <span className="badge badge--pending">READ</span>
                          }
                        </td>
                        <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                          {s.sql_count > 0 ? `${s.sql_count}` : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {selectedJob.sql_queries.filter(Boolean).length > 0 && (
                <div className="card">
                  <SectionLabel>sql_preview (full syntax highlighting in browser report)</SectionLabel>
                  <CodeBlock language="sql">
                    {selectedJob.sql_queries.filter(Boolean)[0]}
                  </CodeBlock>
                  {selectedJob.sql_queries.filter(Boolean).length > 1 && (
                    <div style={{ marginTop: 8, fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                      + {selectedJob.sql_queries.filter(Boolean).length - 1} more — open report for full view
                    </div>
                  )}
                </div>
              )}

              <div style={{
                padding: '14px 18px', background: 'var(--accent-bg)',
                border: '1px solid var(--accent-border)', borderRadius: 'var(--radius)',
                display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16,
              }}>
                <div>
                  <div style={{ fontSize: 'var(--font-size-sm)', color: 'var(--accent)', fontWeight: 600 }}>
                    Full report available
                  </div>
                  <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginTop: 2 }}>
                    SQL highlighting · indented queries · all stages · dependency map
                  </div>
                </div>
                <button className="btn btn--primary" onClick={openHtmlReport} disabled={reportLoading}>
                  {reportLoading ? '// generating...' : 'open_report ↗'}
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
