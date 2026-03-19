'use client'

import { useState } from 'react'
import { analysisApi } from '@/lib/api'
import type { GapResult } from '@/lib/api'
import { SectionLabel, EmptyState, StatCard } from '@/components/ui'

export default function GapsPage() {
  const [form, setForm] = useState({
    table_name: '',
    date_from: '',
    date_to: '',
    date_column: 'DT_REFERENCIA',
    granularity: 'day',
  })
  const [results, setResults] = useState<GapResult[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const runAnalysis = async () => {
    if (!form.table_name || !form.date_from || !form.date_to) {
      setError('table_name, date_from and date_to are required')
      return
    }
    setError(null)
    setLoading(true)
    try {
      const data = await analysisApi.runGapAnalysis(form)
      setResults(data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }

  const totalDiff = results.reduce((s, r) => s + Math.abs(r.diff), 0)
  const gapDays = results.filter(r => r.diff !== 0).length
  const maxDiff = results.length ? Math.max(...results.map(r => Math.abs(r.diff))) : 0

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Volumetric gap analysis — Oracle vs Snowflake row counts per day.
      </p>

      {/* Form */}
      <SectionLabel>analysis_config</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '220px 140px 140px 160px 120px auto', gap: 12, alignItems: 'end' }}>
          <div className="form-group">
            <label className="form-label">table_name *</label>
            <input className="form-input" placeholder="F_CEL_NETWORK_EVENT"
              value={form.table_name}
              onChange={e => setForm(p => ({ ...p, table_name: e.target.value }))} />
          </div>
          <div className="form-group">
            <label className="form-label">date_from *</label>
            <input className="form-input" type="date"
              value={form.date_from}
              onChange={e => setForm(p => ({ ...p, date_from: e.target.value }))} />
          </div>
          <div className="form-group">
            <label className="form-label">date_to *</label>
            <input className="form-input" type="date"
              value={form.date_to}
              onChange={e => setForm(p => ({ ...p, date_to: e.target.value }))} />
          </div>
          <div className="form-group">
            <label className="form-label">date_column</label>
            <input className="form-input"
              value={form.date_column}
              onChange={e => setForm(p => ({ ...p, date_column: e.target.value }))} />
          </div>
          <div className="form-group">
            <label className="form-label">granularity</label>
            <select className="form-input"
              value={form.granularity}
              onChange={e => setForm(p => ({ ...p, granularity: e.target.value }))}>
              <option value="day">day</option>
              <option value="week">week</option>
              <option value="month">month</option>
            </select>
          </div>
          <button className="btn btn--primary" onClick={runAnalysis} disabled={loading}>
            {loading ? '// analyzing...' : '> run'}
          </button>
        </div>
        {error && <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', marginTop: 12 }}>✗ {error}</div>}
      </div>

      {/* Summary stats */}
      {results.length > 0 && (
        <>
          <SectionLabel>gap_summary</SectionLabel>
          <div className="grid-4" style={{ marginBottom: 24 }}>
            <StatCard label="periods_analyzed" value={results.length} />
            <StatCard label="periods_with_gap"  value={gapDays}      status={gapDays > 0 ? 'warn' : 'ok'} />
            <StatCard label="total_diff_rows"   value={totalDiff.toLocaleString()} status={totalDiff > 0 ? 'error' : 'ok'} />
            <StatCard label="max_single_gap"    value={maxDiff.toLocaleString()} />
          </div>
        </>
      )}

      {/* Visual bar chart */}
      {results.length > 0 && (
        <>
          <SectionLabel>gap_chart // oracle (green) vs snowflake (blue)</SectionLabel>
          <div className="card" style={{ marginBottom: 24, overflowX: 'auto' }}>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 2, height: 120, minWidth: results.length * 18 }}>
              {results.map((r) => {
                const maxVal = Math.max(...results.map(x => Math.max(x.oracle_count, x.snowflake_count)), 1)
                const oracleH = Math.round((r.oracle_count / maxVal) * 100)
                const snowH   = Math.round((r.snowflake_count / maxVal) * 100)
                const hasDiff = r.diff !== 0
                return (
                  <div key={r.date} style={{ display: 'flex', gap: 1, alignItems: 'flex-end' }} title={`${r.date}\nOracle: ${r.oracle_count.toLocaleString()}\nSnow: ${r.snowflake_count.toLocaleString()}\nDiff: ${r.diff}`}>
                    <div style={{ width: 7, height: `${oracleH}%`, background: hasDiff ? 'var(--status-error)' : 'var(--accent-primary)', opacity: 0.8 }} />
                    <div style={{ width: 7, height: `${snowH}%`,   background: 'var(--status-info)', opacity: 0.8 }} />
                  </div>
                )
              })}
            </div>
          </div>
        </>
      )}

      {/* Results table */}
      <SectionLabel>gap_detail</SectionLabel>
      {results.length === 0 ? (
        <EmptyState message="run analysis to see gap results" />
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>date</th>
                <th>oracle_count</th>
                <th>snowflake_count</th>
                <th>diff</th>
                <th>diff_%</th>
                <th>status</th>
              </tr>
            </thead>
            <tbody>
              {results.map(r => (
                <tr key={r.date}>
                  <td style={{ color: 'var(--text-secondary)', fontVariantNumeric: 'tabular-nums' }}>{r.date}</td>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>{r.oracle_count.toLocaleString()}</td>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>{r.snowflake_count.toLocaleString()}</td>
                  <td style={{ color: r.diff !== 0 ? 'var(--status-error)' : 'var(--accent-primary)', fontVariantNumeric: 'tabular-nums', fontWeight: r.diff !== 0 ? 600 : 400 }}>
                    {r.diff > 0 ? '+' : ''}{r.diff.toLocaleString()}
                  </td>
                  <td style={{ color: 'var(--text-muted)', fontVariantNumeric: 'tabular-nums' }}>
                    {r.diff_pct.toFixed(2)}%
                  </td>
                  <td>
                    {r.diff === 0
                      ? <span className="badge badge--done">✓ match</span>
                      : <span className="badge badge--error">✗ gap</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
