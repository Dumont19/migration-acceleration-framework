'use client'

import { useEffect, useState, useCallback } from 'react'
import { logsApi } from '@/lib/api'
import type { LogEntry, LogLevel, LogsResponse } from '@/lib/api'
import { SectionLabel, EmptyState, StatCard } from '@/components/ui'

const LEVEL_COLORS: Record<LogLevel, string> = {
  DEBUG:    'var(--text-muted)',
  INFO:     'var(--text-secondary)',
  WARNING:  'var(--status-warn)',
  ERROR:    'var(--status-error)',
  CRITICAL: 'var(--status-error)',
}

export default function LogsPage() {
  const [data, setData] = useState<LogsResponse | null>(null)
  const [stats, setStats] = useState<{ by_level: Record<string, number>; top_error_tables: { table: string; errors: number }[]; total: number } | null>(null)
  const [loading, setLoading] = useState(false)

  const [filters, setFilters] = useState({
    level: '' as LogLevel | '',
    table_name: '',
    search: '',
    page: 1,
    page_size: 100,
  })

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [logs, s] = await Promise.all([
        logsApi.query({
          ...(filters.level && { level: filters.level as LogLevel }),
          ...(filters.table_name && { table_name: filters.table_name }),
          ...(filters.search && { search: filters.search }),
          page: filters.page,
          page_size: filters.page_size,
        }),
        logsApi.getStats(),
      ])
      setData(logs)
      setStats(s)
    } finally {
      setLoading(false)
    }
  }, [filters])

  useEffect(() => { load() }, [load])

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Persistent audit trail — all execution events stored in PostgreSQL.
      </p>

      {/* Stats */}
      <SectionLabel>log_summary</SectionLabel>
      <div className="grid-4" style={{ marginBottom: 32 }}>
        <StatCard label="total_entries"    value={stats?.total?.toLocaleString() ?? '—'} />
        <StatCard label="info_events"      value={stats?.by_level?.INFO?.toLocaleString() ?? '0'} />
        <StatCard label="warning_events"   value={stats?.by_level?.WARNING?.toLocaleString() ?? '0'} status={(stats?.by_level?.WARNING ?? 0) > 0 ? 'warn' : undefined} />
        <StatCard label="error_events"     value={stats?.by_level?.ERROR?.toLocaleString() ?? '0'}  status={(stats?.by_level?.ERROR ?? 0) > 0 ? 'error' : undefined} />
      </div>

      {/* Filters */}
      <SectionLabel>filter_logs</SectionLabel>
      <div className="card" style={{ marginBottom: 24 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '140px 200px 1fr auto', gap: 12, alignItems: 'end' }}>
          <div className="form-group">
            <label className="form-label">level</label>
            <select className="form-input"
              value={filters.level}
              onChange={e => setFilters(p => ({ ...p, level: e.target.value as LogLevel | '', page: 1 }))}
            >
              <option value="">all levels</option>
              {(['DEBUG','INFO','WARNING','ERROR','CRITICAL'] as LogLevel[]).map(l => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>
          <div className="form-group">
            <label className="form-label">table_name</label>
            <input className="form-input" placeholder="F_CEL_NETWORK_EVENT"
              value={filters.table_name}
              onChange={e => setFilters(p => ({ ...p, table_name: e.target.value, page: 1 }))}
            />
          </div>
          <div className="form-group">
            <label className="form-label">search_message</label>
            <input className="form-input" placeholder="search log messages..."
              value={filters.search}
              onChange={e => setFilters(p => ({ ...p, search: e.target.value, page: 1 }))}
            />
          </div>
          <button className="btn btn--outline" onClick={load} disabled={loading}>
            {loading ? 'loading...' : '> query'}
          </button>
        </div>
      </div>

      {/* Log table */}
      <SectionLabel>log_entries ({data?.total?.toLocaleString() ?? 0} total)</SectionLabel>
      {!data || data.items.length === 0 ? (
        <EmptyState message="no log entries match the current filters" />
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>timestamp</th>
                <th>level</th>
                <th>table</th>
                <th>operation</th>
                <th>message</th>
                <th>job_id</th>
              </tr>
            </thead>
            <tbody>
              {data.items.map((log: LogEntry) => (
                <tr key={log.id}>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)', whiteSpace: 'nowrap', fontVariantNumeric: 'tabular-nums' }}>
                    {log.created_at.substring(0, 19).replace('T', ' ')}
                  </td>
                  <td>
                    <span style={{ color: LEVEL_COLORS[log.level], fontSize: 'var(--font-size-xs)', fontWeight: 600 }}>
                      {log.level}
                    </span>
                  </td>
                  <td style={{ color: 'var(--accent-primary)', fontSize: 'var(--font-size-xs)' }}>
                    {log.table_name ?? '—'}
                  </td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {log.operation ?? '—'}
                  </td>
                  <td style={{ maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 'var(--font-size-sm)' }}>
                    {log.message}
                  </td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 9, fontVariantNumeric: 'tabular-nums' }}>
                    {log.job_id?.substring(0, 8) ?? '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Pagination */}
          {data.pages > 1 && (
            <div style={{ padding: '12px 16px', borderTop: '1px solid var(--bg-border)', display: 'flex', gap: 8, alignItems: 'center' }}>
              <span style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                page {data.page} of {data.pages}
              </span>
              <button className="btn btn--ghost" disabled={filters.page <= 1}
                onClick={() => setFilters(p => ({ ...p, page: p.page - 1 }))}>
                ← prev
              </button>
              <button className="btn btn--ghost" disabled={filters.page >= data.pages}
                onClick={() => setFilters(p => ({ ...p, page: p.page + 1 }))}>
                next →
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
