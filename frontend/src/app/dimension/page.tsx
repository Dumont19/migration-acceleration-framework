'use client'

import { useState, useCallback, useRef } from 'react'
import { dimensionApi } from '@/lib/api'
import type {
  DimensionSpec,
  DimensionGenerateResult,
  DimensionHomologateResult,
  DimensionJobSummary,
} from '@/lib/api'
import { SectionLabel, StatusBadge } from '@/components/ui'

// ── Types ────────────────────────────────────────────────────────────────────

type Tab = 'upload' | 'generate' | 'homologate' | 'history'

// ── Helpers ──────────────────────────────────────────────────────────────────

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true)
          setTimeout(() => setCopied(false), 1800)
        })
      }}
      style={{
        background: 'var(--bg-hover)',
        border: '1px solid var(--bg-border)',
        color: copied ? 'var(--accent)' : 'var(--text-muted)',
        padding: '2px 10px',
        borderRadius: 2,
        cursor: 'pointer',
        fontSize: 'var(--font-size-xs)',
        fontFamily: 'var(--font-mono)',
      }}
    >
      {copied ? 'copied!' : 'copy'}
    </button>
  )
}

function SqlBlock({ label, sql }: { label: string; sql: string }) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
        <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--accent)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
          // {label}
        </span>
        <CopyButton text={sql} />
      </div>
      <pre style={{
        background: 'var(--bg-code, #0a0a0a)',
        border: '1px solid var(--bg-border)',
        borderRadius: 2,
        padding: '12px 14px',
        fontSize: 12,
        lineHeight: 1.7,
        overflowX: 'auto',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
        color: 'var(--text-secondary)',
        maxHeight: 320,
        overflowY: 'auto',
      }}>
        <code>{sql}</code>
      </pre>
    </div>
  )
}

// ── Main Page ─────────────────────────────────────────────────────────────────

export default function DimensionPage() {
  const [activeTab, setActiveTab] = useState<Tab>('upload')

  // Upload/analyze state
  const fileRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [spec, setSpec] = useState<DimensionSpec | null>(null)

  // Generate state
  const [generating, setGenerating] = useState(false)
  const [generateError, setGenerateError] = useState<string | null>(null)
  const [result, setResult] = useState<DimensionGenerateResult | null>(null)

  // Homologate state
  const [prodTable, setProdTable] = useState('')
  const [timeTravelOffset, setTimeTravelOffset] = useState(0)
  const [homologating, setHomologating] = useState(false)
  const [homoError, setHomoError] = useState<string | null>(null)
  const [homoResult, setHomoResult] = useState<DimensionHomologateResult | null>(null)

  // History state
  const [jobs, setJobs] = useState<DimensionJobSummary[]>([])
  const [loadingJobs, setLoadingJobs] = useState(false)

  // ── Handlers ──────────────────────────────────────────────────────────────

  const handleUpload = useCallback(async () => {
    const file = fileRef.current?.files?.[0]
    if (!file) { setUploadError('Selecione um arquivo .dsx ou .xml'); return }
    setUploadError(null)
    setUploading(true)
    try {
      const form = new FormData()
      form.append('file', file)
      const extracted = await dimensionApi.analyze(form)
      setSpec(extracted)
      setActiveTab('generate')
    } catch (err: unknown) {
      setUploadError(err instanceof Error ? err.message : 'Erro ao analisar XML')
    } finally {
      setUploading(false)
    }
  }, [])

  const handleGenerate = useCallback(async () => {
    if (!spec) return
    setGenerateError(null)
    setGenerating(true)
    try {
      const res = await dimensionApi.generate(spec)
      setResult(res)
    } catch (err: unknown) {
      setGenerateError(err instanceof Error ? err.message : 'Erro ao gerar SQL')
    } finally {
      setGenerating(false)
    }
  }, [spec])

  const handleHomologate = useCallback(async () => {
    if (!spec || !prodTable.trim()) { setHomoError('Informe a tabela PROD'); return }
    setHomoError(null)
    setHomologating(true)
    try {
      const form = new FormData()
      Object.entries(spec).forEach(([k, v]) => form.append(k, JSON.stringify(v)))
      form.set('prod_table', prodTable.trim())
      form.set('time_travel_offset', String(timeTravelOffset))
      const res = await dimensionApi.homologate(form)
      setHomoResult(res)
    } catch (err: unknown) {
      setHomoError(err instanceof Error ? err.message : 'Erro ao gerar queries')
    } finally {
      setHomologating(false)
    }
  }, [spec, prodTable, timeTravelOffset])

  const handleLoadHistory = useCallback(async () => {
    setLoadingJobs(true)
    try {
      const list = await dimensionApi.listJobs()
      setJobs(list)
    } catch { /* noop */ } finally {
      setLoadingJobs(false)
    }
  }, [])

  // ── Tab navigation ────────────────────────────────────────────────────────

  const tabs: { id: Tab; label: string }[] = [
    { id: 'upload', label: '01_upload' },
    { id: 'generate', label: '02_generate' },
    { id: 'homologate', label: '03_homologate' },
    { id: 'history', label: '04_history' },
  ]

  const tabStyle = (id: Tab) => ({
    padding: '6px 16px',
    fontSize: 'var(--font-size-xs)',
    fontFamily: 'var(--font-mono)',
    background: activeTab === id ? 'var(--accent)' : 'transparent',
    color: activeTab === id ? 'var(--bg)' : 'var(--text-muted)',
    border: '1px solid var(--bg-border)',
    borderRadius: 2,
    cursor: 'pointer',
  } as const)

  return (
    <div>
      <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 24 }}>
        Geração automática de SQL para migração de dimensões SCD2 (DataStage → Snowflake).
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 24 }}>
        {tabs.map(t => (
          <button
            key={t.id}
            style={tabStyle(t.id)}
            onClick={() => {
              setActiveTab(t.id)
              if (t.id === 'history') handleLoadHistory()
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Tab: Upload ─────────────────────────────────────────────────── */}
      {activeTab === 'upload' && (
        <div className="card" style={{ maxWidth: 540 }}>
          <SectionLabel>upload_dsx</SectionLabel>
          <p style={{ fontSize: 'var(--font-size-sm)', color: 'var(--text-muted)', marginBottom: 20 }}>
            Faça upload de um arquivo <code>.dsx</code> ou <code>.xml</code> exportado do DataStage.
            O sistema detecta automaticamente o padrão fl_mn, colunas, lookups e surrogate key.
          </p>

          <div className="form-group" style={{ marginBottom: 16 }}>
            <label className="form-label">arquivo .dsx / .xml</label>
            <input
              ref={fileRef}
              type="file"
              accept=".dsx,.xml"
              className="form-input"
              style={{ cursor: 'pointer' }}
            />
          </div>

          {uploadError && (
            <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 12 }}>
              ✗ {uploadError}
            </div>
          )}

          <button className="btn btn--primary" onClick={handleUpload} disabled={uploading}>
            {uploading ? '// analisando...' : '> analisar_xml'}
          </button>
        </div>
      )}

      {/* ── Tab: Generate ───────────────────────────────────────────────── */}
      {activeTab === 'generate' && (
        <div>
          {!spec ? (
            <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
              <div style={{ color: 'var(--accent-muted)' }}>// nenhum spec carregado</div>
              <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)', marginTop: 8 }}>
                Faça upload de um arquivo XML primeiro.
              </div>
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 20, alignItems: 'start' }}>
              {/* Spec preview */}
              <div className="card">
                <SectionLabel>spec_preview</SectionLabel>
                <table style={{ width: '100%', fontSize: 'var(--font-size-xs)', borderCollapse: 'collapse' }}>
                  <tbody>
                    {[
                      ['job_name', spec.job_name],
                      ['target_table', spec.target_table],
                      ['raw_table', spec.raw_table],
                      ['schema', spec.schema],
                      ['fl_mn', spec.fl_mn],
                      ['surrogate_key', spec.surrogate_key],
                      ['business_key', spec.business_key],
                      ['nom_sis_ori', spec.nom_sis_ori],
                      ['has_row_number', String(spec.has_row_number)],
                      ['is_delta', String(spec.is_delta)],
                      ['colunas', String(spec.columns.length)],
                      ['lookups', String(spec.lookups.length)],
                    ].map(([k, v]) => (
                      <tr key={k} style={{ borderBottom: '1px solid var(--bg-border)' }}>
                        <td style={{ padding: '4px 8px 4px 0', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{k}</td>
                        <td style={{ padding: '4px 0', color: 'var(--accent)', wordBreak: 'break-all' }}>{v}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                {spec.lookups.length > 0 && (
                  <>
                    <div style={{ marginTop: 16 }}><SectionLabel>lookups_detectados</SectionLabel></div>
                    {spec.lookups.map((lkp, i) => (
                      <div key={i} style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-secondary)', marginBottom: 6 }}>
                        <span style={{ color: 'var(--accent)' }}>{lkp.stage_name}</span>
                        {' → '}{lkp.lookup_table} (COALESCE default: {lkp.default_value})
                      </div>
                    ))}
                  </>
                )}

                <div style={{ marginTop: 16 }}>
                  {generateError && (
                    <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 8 }}>
                      ✗ {generateError}
                    </div>
                  )}
                  <button className="btn btn--primary" onClick={handleGenerate} disabled={generating} style={{ width: '100%' }}>
                    {generating ? '// gerando...' : '> gerar_sql'}
                  </button>
                </div>
              </div>

              {/* SQL output */}
              <div>
                {!result ? (
                  <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
                    <div style={{ color: 'var(--accent-muted)' }}>// aguardando geração</div>
                  </div>
                ) : (
                  <div className="card">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
                      <SectionLabel>sqls_gerados — {result.target_table} · fl_mn={result.fl_mn}</SectionLabel>
                      {result.record_id && (
                        <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                          id #{result.record_id}
                        </span>
                      )}
                    </div>
                    {Object.entries(result.sqls).map(([key, sql]) => (
                      <SqlBlock key={key} label={key.replace(/_/g, ' ')} sql={sql} />
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Tab: Homologate ─────────────────────────────────────────────── */}
      {activeTab === 'homologate' && (
        <div>
          {!spec ? (
            <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
              <div style={{ color: 'var(--accent-muted)' }}>// nenhum spec carregado</div>
            </div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 20, alignItems: 'start' }}>
              <div className="card">
                <SectionLabel>homologation_config</SectionLabel>

                <div className="form-group" style={{ marginBottom: 16 }}>
                  <label className="form-label">tabela PROD (schema.tabela)</label>
                  <input
                    className="form-input"
                    placeholder="DWADM.D_ORDEM_SRV_TECNICA"
                    value={prodTable}
                    onChange={e => setProdTable(e.target.value)}
                  />
                </div>

                <div className="form-group" style={{ marginBottom: 20 }}>
                  <label className="form-label">Time Travel offset (segundos, ≤ 0)</label>
                  <input
                    className="form-input"
                    type="number"
                    max={0}
                    value={timeTravelOffset}
                    onChange={e => setTimeTravelOffset(Number(e.target.value))}
                  />
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
                    Ex: -3600 = PROD de 1h atrás. 0 = sem Time Travel.
                  </div>
                </div>

                {homoError && (
                  <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 8 }}>
                    ✗ {homoError}
                  </div>
                )}
                <button className="btn btn--primary" onClick={handleHomologate} disabled={homologating} style={{ width: '100%' }}>
                  {homologating ? '// gerando...' : '> gerar_queries'}
                </button>
              </div>

              <div>
                {!homoResult ? (
                  <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
                    <div style={{ color: 'var(--accent-muted)' }}>// aguardando parâmetros</div>
                  </div>
                ) : (
                  <div className="card">
                    <div style={{ marginBottom: 16, fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                      DEV: <span style={{ color: 'var(--accent)' }}>{homoResult.dev_table}</span>
                      {' vs '}
                      PROD: <span style={{ color: 'var(--accent)' }}>{homoResult.prod_table}</span>
                      {homoResult.time_travel_offset < 0 && (
                        <span> · AT OFFSET {homoResult.time_travel_offset}s</span>
                      )}
                    </div>
                    {Object.entries(homoResult.queries).map(([key, sql]) => (
                      <SqlBlock key={key} label={key.replace(/_/g, ' ')} sql={sql} />
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Tab: History ────────────────────────────────────────────────── */}
      {activeTab === 'history' && (
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <SectionLabel>dimension_jobs</SectionLabel>
            <button className="btn btn--ghost" onClick={handleLoadHistory} disabled={loadingJobs}>
              {loadingJobs ? '// carregando...' : '↺ refresh'}
            </button>
          </div>

          {jobs.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '32px 0', color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)' }}>
              Nenhum job de dimensão registrado.
            </div>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--font-size-xs)' }}>
              <thead>
                <tr>
                  {['#', 'job_name', 'target_table', 'schema', 'fl_mn', 'nom_sis_ori', 'created_at'].map(h => (
                    <th key={h} style={{
                      textAlign: 'left',
                      padding: '6px 10px',
                      borderBottom: '1px solid var(--bg-border)',
                      color: 'var(--accent)',
                      textTransform: 'uppercase',
                      letterSpacing: '0.06em',
                    }}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {jobs.map(j => (
                  <tr key={j.id} style={{ borderBottom: '1px solid var(--bg-border)' }}>
                    <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>{j.id}</td>
                    <td style={{ padding: '6px 10px', color: 'var(--accent)' }}>{j.job_name}</td>
                    <td style={{ padding: '6px 10px' }}>{j.target_table}</td>
                    <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>{j.schema}</td>
                    <td style={{ padding: '6px 10px' }}>
                      <span style={{
                        background: j.fl_mn === '1' ? 'rgba(0,255,136,.1)' : 'rgba(244,162,97,.1)',
                        color: j.fl_mn === '1' ? 'var(--accent)' : '#f4a261',
                        padding: '2px 8px',
                        borderRadius: 2,
                        fontSize: 10,
                      }}>
                        fl_mn={j.fl_mn}
                      </span>
                    </td>
                    <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>{j.nom_sis_ori}</td>
                    <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>
                      {j.created_at ? new Date(j.created_at).toLocaleString('pt-BR') : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  )
}
