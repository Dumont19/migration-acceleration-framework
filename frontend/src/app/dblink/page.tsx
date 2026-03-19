'use client'

/**
 * /dblink — DB Link migration page
 * Identical to /migration but forces use_dblink: true and uses migration_dblink operation.
 * Kept as a separate route for UX clarity.
 */

import { useState, useCallback } from 'react'
import { migrationApi } from '@/lib/api'
import type { MigrationRequest } from '@/lib/api'
import { useJobProgress } from '@/lib/useJobProgress'
import { ProgressBar, LogStream, StatusBadge, SectionLabel, StatCard } from '@/components/ui'

const DEFAULT_FORM: MigrationRequest = {
  table_name: '',
  operation: 'migration_dblink',
  date_from: '',
  date_to: '',
  partition_column: 'DT_REFERENCIA',
  batch_size_days: 1,
  max_workers: 4,
  use_dblink: true,
  schema_source: 'DWADM',
  schema_target: 'DWADM',
}

export default function DBLinkPage() {
  const [form, setForm] = useState<MigrationRequest>(DEFAULT_FORM)
  const [activeJobId, setActiveJobId] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const { progress, logs, connected } = useJobProgress(activeJobId)

  const handleSubmit = useCallback(async () => {
    if (!form.table_name.trim()) { setFormError('table_name is required'); return }
    setFormError(null)
    setSubmitting(true)
    try {
      const res = await migrationApi.start({ ...form, use_dblink: true, operation: 'migration_dblink' })
      setActiveJobId(res.job_id)
    } catch (err: unknown) {
      setFormError(err instanceof Error ? err.message : 'Failed to start migration')
    } finally {
      setSubmitting(false)
    }
  }, [form])

  return (
    <div>
      <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 16 }}>
        Direct Oracle → Snowflake migration via DB Link. No S3 staging required.
      </div>
      <div className="card" style={{ marginBottom: 24, borderLeft: '2px solid var(--status-info)', padding: '10px 16px' }}>
        <span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--status-info)' }}>
          ℹ DB Link mode bypasses S3 — requires SNOWFLAKE_DB_LINK configured in Oracle.
          Useful for smaller tables or when S3 credentials are unavailable.
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '380px 1fr', gap: 24, alignItems: 'start' }}>
        <div className="card">
          <SectionLabel>dblink_config</SectionLabel>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div className="form-group">
              <label className="form-label">table_name *</label>
              <input className="form-input" placeholder="F_CEL_NETWORK_EVENT"
                value={form.table_name}
                onChange={e => setForm(p => ({ ...p, table_name: e.target.value }))} />
            </div>
            <div className="form-group">
              <label className="form-label">partition_column</label>
              <input className="form-input"
                value={form.partition_column}
                onChange={e => setForm(p => ({ ...p, partition_column: e.target.value }))} />
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div className="form-group">
                <label className="form-label">date_from</label>
                <input className="form-input" type="date"
                  value={form.date_from}
                  onChange={e => setForm(p => ({ ...p, date_from: e.target.value }))} />
              </div>
              <div className="form-group">
                <label className="form-label">date_to</label>
                <input className="form-input" type="date"
                  value={form.date_to}
                  onChange={e => setForm(p => ({ ...p, date_to: e.target.value }))} />
              </div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div className="form-group">
                <label className="form-label">batch_size_days</label>
                <input className="form-input" type="number" min={1} max={365}
                  value={form.batch_size_days}
                  onChange={e => setForm(p => ({ ...p, batch_size_days: Number(e.target.value) }))} />
              </div>
              <div className="form-group">
                <label className="form-label">max_workers</label>
                <input className="form-input" type="number" min={1} max={16}
                  value={form.max_workers}
                  onChange={e => setForm(p => ({ ...p, max_workers: Number(e.target.value) }))} />
              </div>
            </div>
            {formError && <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>✗ {formError}</div>}
            <button className="btn btn--primary" onClick={handleSubmit}
              disabled={submitting || progress?.status === 'running'}>
              {submitting ? '// initializing...' : '> run_dblink_migration'}
            </button>
            {activeJobId && progress?.status !== 'running' && (
              <button className="btn btn--ghost" onClick={() => { setActiveJobId(null); setForm(DEFAULT_FORM) }}>
                new_migration
              </button>
            )}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {!activeJobId ? (
            <div className="card" style={{ textAlign: 'center', padding: '48px 24px' }}>
              <div style={{ color: 'var(--accent-muted)', marginBottom: 8 }}>// awaiting_job</div>
              <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                Configure and start a DB Link migration to see real-time progress.
              </div>
            </div>
          ) : (
            <>
              <div className="grid-4">
                <StatCard label="status" value={progress?.status ?? 'pending'} accent={progress?.status === 'running'} />
                <StatCard label="partitions" value={progress ? `${progress.done_partitions}/${progress.total_partitions ?? '?'}` : '—'} />
                <StatCard label="rows_loaded" value={progress?.loaded_rows?.toLocaleString() ?? '0'} />
                <StatCard label="ws" value={connected ? 'live' : 'reconnecting'} status={connected ? 'ok' : 'warn'} />
              </div>
              <div className="card">
                <div className="flex justify-between" style={{ marginBottom: 8 }}>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 'var(--font-size-sm)' }}>{progress?.table_name}</span>
                  {progress && <StatusBadge status={progress.status} />}
                </div>
                <ProgressBar percent={progress?.percent ?? 0} status={progress?.status} />
              </div>
              <div className="card" style={{ padding: 0 }}>
                <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--bg-border)' }}>
                  <SectionLabel>execution_log</SectionLabel>
                </div>
                <LogStream logs={logs} maxHeight={350} />
              </div>
              <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>job_id: {activeJobId}</div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
