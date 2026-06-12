'use client'

import { useState, useCallback } from 'react'
import type { DimensionSpec } from '@/lib/api'
import { SectionLabel } from '@/components/ui'
import { SqlBlock } from '@/components/dimension/SqlBlock'
import {
  useDimensionUpload,
  useDimensionGenerate,
  useDimensionHomologate,
  useDimensionHistory,
  type DimensionTab,
} from '@/hooks/useDimension'

// ── Tab bar ───────────────────────────────────────────────────────────────────

const TABS: { id: DimensionTab; label: string }[] = [
  { id: 'upload',     label: '01_upload' },
  { id: 'generate',   label: '02_generate' },
  { id: 'homologate', label: '03_homologate' },
  { id: 'history',    label: '04_history' },
]

function tabStyle(active: boolean) {
  return {
    padding: '6px 16px',
    fontSize: 'var(--font-size-xs)',
    fontFamily: 'var(--font-mono)',
    background: active ? 'var(--accent)' : 'transparent',
    color: active ? 'var(--bg)' : 'var(--text-muted)',
    border: '1px solid var(--bg-border)',
    borderRadius: 2,
    cursor: 'pointer',
  } as const
}

function flMnBadgeStyle(flMn: string) {
  return flMn === '1'
    ? { background: 'rgba(0,255,136,.1)', color: 'var(--accent)' }
    : { background: 'rgba(244,162,97,.1)', color: '#f4a261' }
}

// ── Page ──────────────────────────────────────────────────────────────────────

/** Página de migração de dimensões SCD2 (DataStage → Snowflake). */
export default function DimensionPage() {
  const [activeTab, setActiveTab] = useState<DimensionTab>('upload')
  const [spec, setSpec] = useState<DimensionSpec | null>(null)

  const switchTab = useCallback((tab: DimensionTab) => setActiveTab(tab), [])

  const upload    = useDimensionUpload((s, tab) => { setSpec(s); setActiveTab(tab) })
  const generate  = useDimensionGenerate()
  const homo      = useDimensionHomologate()
  const history   = useDimensionHistory()

  return (
    <div>
      <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 24 }}>
        Geração automática de SQL para migração de dimensões SCD2 (DataStage → Snowflake).
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 24 }}>
        {TABS.map(t => (
          <button
            key={t.id}
            style={tabStyle(activeTab === t.id)}
            onClick={() => {
              switchTab(t.id)
              if (t.id === 'history') history.handleLoad()
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === 'upload'     && <UploadTab upload={upload} />}
      {activeTab === 'generate'   && <GenerateTab spec={spec} generate={generate} />}
      {activeTab === 'homologate' && <HomologateTab spec={spec} homo={homo} />}
      {activeTab === 'history'    && <HistoryTab history={history} />}
    </div>
  )
}

// ── Tab: Upload ───────────────────────────────────────────────────────────────

function UploadTab({ upload }: { upload: ReturnType<typeof useDimensionUpload> }) {
  return (
    <div className="card" style={{ maxWidth: 540 }}>
      <SectionLabel>upload_dsx</SectionLabel>
      <p style={{ fontSize: 'var(--font-size-sm)', color: 'var(--text-muted)', marginBottom: 20 }}>
        Faça upload de um arquivo <code>.dsx</code> ou <code>.xml</code> exportado do DataStage.
        O sistema detecta automaticamente o padrão fl_mn, colunas, lookups e surrogate key.
      </p>
      <div className="form-group" style={{ marginBottom: 16 }}>
        <label className="form-label">arquivo .dsx / .xml</label>
        <input ref={upload.fileRef} type="file" accept=".dsx,.xml" className="form-input" style={{ cursor: 'pointer' }} />
      </div>
      {upload.error && (
        <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 12 }}>
          ✗ {upload.error}
        </div>
      )}
      <button className="btn btn--primary" onClick={upload.handleUpload} disabled={upload.uploading}>
        {upload.uploading ? '// analisando...' : '> analisar_xml'}
      </button>
    </div>
  )
}

// ── Tab: Generate ─────────────────────────────────────────────────────────────

function GenerateTab({
  spec,
  generate,
}: {
  spec: DimensionSpec | null
  generate: ReturnType<typeof useDimensionGenerate>
}) {
  if (!spec) {
    return (
      <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
        <div style={{ color: 'var(--accent-muted)' }}>// nenhum spec carregado</div>
        <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)', marginTop: 8 }}>
          Faça upload de um arquivo XML primeiro.
        </div>
      </div>
    )
  }

  const specRows: [string, string][] = [
    ['job_name', spec.job_name], ['target_table', spec.target_table],
    ['raw_table', spec.raw_table], ['schema', spec.schema],
    ['fl_mn', spec.fl_mn], ['surrogate_key', spec.surrogate_key],
    ['business_key', spec.business_key], ['nom_sis_ori', spec.nom_sis_ori],
    ['has_row_number', String(spec.has_row_number)], ['is_delta', String(spec.is_delta)],
    ['colunas', String(spec.columns.length)], ['lookups', String(spec.lookups.length)],
  ]

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 20, alignItems: 'start' }}>
      <div className="card">
        <SectionLabel>spec_preview</SectionLabel>
        <table style={{ width: '100%', fontSize: 'var(--font-size-xs)', borderCollapse: 'collapse' }}>
          <tbody>
            {specRows.map(([k, v]) => (
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
          {generate.error && (
            <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 8 }}>
              ✗ {generate.error}
            </div>
          )}
          <button className="btn btn--primary" onClick={() => generate.handleGenerate(spec)} disabled={generate.generating} style={{ width: '100%' }}>
            {generate.generating ? '// gerando...' : '> gerar_sql'}
          </button>
        </div>
      </div>

      <div>
        {!generate.result ? (
          <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
            <div style={{ color: 'var(--accent-muted)' }}>// aguardando geração</div>
          </div>
        ) : (
          <div className="card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
              <SectionLabel>sqls_gerados — {generate.result.target_table} · fl_mn={generate.result.fl_mn}</SectionLabel>
              {generate.result.record_id && (
                <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                  id #{generate.result.record_id}
                </span>
              )}
            </div>
            {Object.entries(generate.result.sqls).map(([key, sql]) => (
              <SqlBlock key={key} label={key.replace(/_/g, ' ')} sql={sql} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Tab: Homologate ───────────────────────────────────────────────────────────

function HomologateTab({
  spec,
  homo,
}: {
  spec: DimensionSpec | null
  homo: ReturnType<typeof useDimensionHomologate>
}) {
  if (!spec) {
    return (
      <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
        <div style={{ color: 'var(--accent-muted)' }}>// nenhum spec carregado</div>
      </div>
    )
  }

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 20, alignItems: 'start' }}>
      <div className="card">
        <SectionLabel>homologation_config</SectionLabel>
        <div className="form-group" style={{ marginBottom: 16 }}>
          <label className="form-label">tabela PROD (schema.tabela)</label>
          <input className="form-input" placeholder="DWADM.D_ORDEM_SRV_TECNICA" value={homo.prodTable} onChange={e => homo.setProdTable(e.target.value)} />
        </div>
        <div className="form-group" style={{ marginBottom: 20 }}>
          <label className="form-label">Time Travel offset (segundos, ≤ 0)</label>
          <input className="form-input" type="number" max={0} value={homo.timeTravelOffset} onChange={e => homo.setTimeTravelOffset(Number(e.target.value))} />
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>Ex: -3600 = PROD de 1h atrás. 0 = sem Time Travel.</div>
        </div>
        {homo.error && (
          <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginBottom: 8 }}>✗ {homo.error}</div>
        )}
        <button className="btn btn--primary" onClick={() => homo.handleHomologate(spec)} disabled={homo.homologating} style={{ width: '100%' }}>
          {homo.homologating ? '// gerando...' : '> gerar_queries'}
        </button>
      </div>

      <div>
        {!homo.result ? (
          <div className="card" style={{ textAlign: 'center', padding: '40px 24px' }}>
            <div style={{ color: 'var(--accent-muted)' }}>// aguardando parâmetros</div>
          </div>
        ) : (
          <div className="card">
            <div style={{ marginBottom: 16, fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
              DEV: <span style={{ color: 'var(--accent)' }}>{homo.result.dev_table}</span>
              {' vs '}
              PROD: <span style={{ color: 'var(--accent)' }}>{homo.result.prod_table}</span>
              {homo.result.time_travel_offset < 0 && (
                <span> · AT OFFSET {homo.result.time_travel_offset}s</span>
              )}
            </div>
            {Object.entries(homo.result.queries).map(([key, sql]) => (
              <SqlBlock key={key} label={key.replace(/_/g, ' ')} sql={sql} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Tab: History ──────────────────────────────────────────────────────────────

function HistoryTab({ history }: { history: ReturnType<typeof useDimensionHistory> }) {
  return (
    <div className="card">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <SectionLabel>dimension_jobs</SectionLabel>
        <button className="btn btn--ghost" onClick={history.handleLoad} disabled={history.loading}>
          {history.loading ? '// carregando...' : '↺ refresh'}
        </button>
      </div>

      {history.jobs.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '32px 0', color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)' }}>
          Nenhum job de dimensão registrado.
        </div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--font-size-xs)' }}>
          <thead>
            <tr>
              {['#', 'job_name', 'target_table', 'schema', 'fl_mn', 'nom_sis_ori', 'created_at'].map(h => (
                <th key={h} style={{ textAlign: 'left', padding: '6px 10px', borderBottom: '1px solid var(--bg-border)', color: 'var(--accent)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {history.jobs.map(j => (
              <tr key={j.id} style={{ borderBottom: '1px solid var(--bg-border)' }}>
                <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>{j.id}</td>
                <td style={{ padding: '6px 10px', color: 'var(--accent)' }}>{j.job_name}</td>
                <td style={{ padding: '6px 10px' }}>{j.target_table}</td>
                <td style={{ padding: '6px 10px', color: 'var(--text-muted)' }}>{j.schema}</td>
                <td style={{ padding: '6px 10px' }}>
                  <span style={{ ...flMnBadgeStyle(j.fl_mn), padding: '2px 8px', borderRadius: 2, fontSize: 10 }}>
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
  )
}
