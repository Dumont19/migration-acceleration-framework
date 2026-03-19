'use client'

import { useState } from 'react'
import { validationApi } from '@/lib/api'
import type { ValidationResult } from '@/lib/api'
import { SectionLabel, StatCard, EmptyState } from '@/components/ui'

export default function ValidationPage() {
  const [form, setForm] = useState({
    table_name: '',
    sample_size: 100,
    check_schema: true,
    check_counts: true,
    check_sample: true,
    date_filter: '',
  })
  const [result, setResult] = useState<ValidationResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const run = async () => {
    if (!form.table_name.trim()) { setError('table_name is required'); return }
    setError(null)
    setLoading(true)
    try {
      const data = await validationApi.run({
        ...form,
        table_name: form.table_name.toUpperCase(),
        ...(form.date_filter ? { date_filter: form.date_filter } : {}),
      })
      setResult(data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Validation failed')
    } finally {
      setLoading(false)
    }
  }

  const checkIcon = (val: boolean | null | undefined) =>
    val === true ? <span style={{ color: 'var(--status-success)' }}>✓ pass</span>
    : val === false ? <span style={{ color: 'var(--status-error)' }}>✗ fail</span>
    : <span style={{ color: 'var(--text-muted)' }}>— n/a</span>

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Full Oracle vs Snowflake comparison — counts, schema, and sample rows.
      </p>

      {/* Form */}
      <SectionLabel>validation_config</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '220px 120px 1fr', gap: 16, alignItems: 'start' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div className="form-group">
              <label className="form-label">table_name *</label>
              <input className="form-input" placeholder="F_CEL_NETWORK_EVENT"
                value={form.table_name}
                onChange={e => setForm(p => ({ ...p, table_name: e.target.value }))} />
            </div>
            <div className="form-group">
              <label className="form-label">sample_size</label>
              <input className="form-input" type="number" min={10} max={10000}
                value={form.sample_size}
                onChange={e => setForm(p => ({ ...p, sample_size: Number(e.target.value) }))} />
            </div>
            <div className="form-group">
              <label className="form-label">date_filter (optional)</label>
              <input className="form-input" placeholder="DT_REFERENCIA >= '2024-01-01'"
                value={form.date_filter}
                onChange={e => setForm(p => ({ ...p, date_filter: e.target.value }))} />
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {[
              { key: 'check_counts', label: 'count_check' },
              { key: 'check_schema', label: 'schema_check' },
              { key: 'check_sample', label: 'sample_check' },
            ].map(({ key, label }) => (
              <label key={key} style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', marginTop: key === 'check_counts' ? 20 : 0 }}>
                <input type="checkbox"
                  checked={form[key as keyof typeof form] as boolean}
                  onChange={e => setForm(p => ({ ...p, [key]: e.target.checked }))}
                  style={{ accentColor: 'var(--accent-primary)', width: 14, height: 14 }}
                />
                <span style={{ fontSize: 'var(--font-size-sm)', color: 'var(--text-secondary)' }}>{label}</span>
              </label>
            ))}
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {error && <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>✗ {error}</div>}
            <button className="btn btn--primary" onClick={run} disabled={loading} style={{ alignSelf: 'flex-start', marginTop: 20 }}>
              {loading ? '// validating...' : '> run_validation'}
            </button>
          </div>
        </div>
      </div>

      {/* Results */}
      {!result ? (
        <EmptyState message="run validation to compare Oracle vs Snowflake" />
      ) : (
        <>
          {/* Overall */}
          <SectionLabel>validation_result — {result.table_name}</SectionLabel>
          <div className="grid-4" style={{ marginBottom: 24 }}>
            <StatCard
              label="overall"
              value={result.passed === true ? '✓ PASS' : result.passed === false ? '✗ FAIL' : '— N/A'}
              accent={result.passed === true}
              status={result.passed === true ? 'ok' : result.passed === false ? 'error' : undefined}
            />
            <StatCard
              label="oracle_count"
              value={result.oracle_count?.toLocaleString() ?? '—'}
            />
            <StatCard
              label="snowflake_count"
              value={result.snowflake_count?.toLocaleString() ?? '—'}
            />
            <StatCard
              label="count_diff"
              value={result.count_diff !== null ? (result.count_diff > 0 ? '+' : '') + result.count_diff.toLocaleString() : '—'}
              status={result.count_diff === 0 ? 'ok' : result.count_diff !== null ? 'error' : undefined}
            />
          </div>

          {/* Check breakdown */}
          <div className="card" style={{ marginBottom: 20 }}>
            <SectionLabel>check_breakdown</SectionLabel>
            <table className="data-table">
              <thead><tr><th>check</th><th>result</th><th>detail</th></tr></thead>
              <tbody>
                <tr>
                  <td>count_match</td>
                  <td>{checkIcon(result.count_match)}</td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    diff: {result.count_diff?.toLocaleString() ?? '—'} rows
                  </td>
                </tr>
                <tr>
                  <td>schema_match</td>
                  <td>{checkIcon(result.schema_match)}</td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {result.schema_diff
                      ? `missing: ${(result.schema_diff as { missing_in_snowflake?: string[] }).missing_in_snowflake?.join(', ') ?? '—'}`
                      : 'schemas identical'}
                  </td>
                </tr>
                <tr>
                  <td>sample_match</td>
                  <td>{checkIcon(result.sample_match_rate !== null ? result.sample_match_rate >= 0.99 : null)}</td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {result.sample_match_rate !== null
                      ? `${(result.sample_match_rate * 100).toFixed(2)}% match rate (n=${result.sample_size})`
                      : '—'}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          {/* Schema diff detail */}
          {result.schema_diff && (
            <div className="card card--error">
              <SectionLabel>schema_diff</SectionLabel>
              <pre style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-secondary)', whiteSpace: 'pre-wrap' }}>
                {JSON.stringify(result.schema_diff, null, 2)}
              </pre>
            </div>
          )}
        </>
      )}
    </div>
  )
}
