'use client'

import { useState, useCallback } from 'react'
import { migrationApi } from '@/lib/api'
import type { MigrationRequest } from '@/lib/api'
import { useJobProgress } from '@/lib/useJobProgress'
import { ProgressBar, LogStream, StatusBadge, SectionLabel, StatCard } from '@/components/ui'

const DEFAULT_FORM: MigrationRequest = {
  table_name: '',
  operation: 'migration_partitioned',
  date_from: '',
  date_to: '',
  partition_column: 'DT_REFERENCIA',
  batch_size_days: 1,
  max_workers: 4,
  use_dblink: false,
  schema_source: 'DWADM',
  schema_target: 'DWADM',
}

export default function MigrationPage() {
  const [form, setForm] = useState<MigrationRequest>(DEFAULT_FORM)
  const [activeJobId, setActiveJobId] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)

  const { progress, logs, connected } = useJobProgress(activeJobId)

  const handleSubmit = useCallback(async () => {
    if (!form.table_name.trim()) {
      setFormError('table_name is required')
      return
    }
    setFormError(null)
    setSubmitting(true)
    try {
      const res = await migrationApi.start(form)
      setActiveJobId(res.job_id)
    } catch (err: unknown) {
      setFormError(err instanceof Error ? err.message : 'Failed to start migration')
    } finally {
      setSubmitting(false)
    }
  }, [form])

  const field = (key: keyof MigrationRequest) => ({
    value: String(form[key] ?? ''),
    onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
      setForm(prev => ({ ...prev, [key]: e.target.value })),
  })

  return (
    <div>
      <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Partitioned Oracle → Snowflake migration with parallel workers and resumable state.
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '380px 1fr', gap: 24, alignItems: 'start' }}>

        {/* Form */}
        <div className="card">
          <SectionLabel>migration_config</SectionLabel>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div className="form-group">
              <label className="form-label">table_name *</label>
              <input className="form-input" placeholder="F_CEL_NETWORK_EVENT" {...field('table_name')} />
            </div>

            <div className="form-group">
              <label className="form-label">partition_column</label>
              <input className="form-input" {...field('partition_column')} />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div className="form-group">
                <label className="form-label">date_from</label>
                <input className="form-input" type="date" {...field('date_from')} />
              </div>
              <div className="form-group">
                <label className="form-label">date_to</label>
                <input className="form-input" type="date" {...field('date_to')} />
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div className="form-group">
                <label className="form-label">batch_size_days</label>
                <input className="form-input" type="number" min={1} max={365}
                  value={form.batch_size_days}
                  onChange={e => setForm(p => ({ ...p, batch_size_days: Number(e.target.value) }))}
                />
              </div>
              <div className="form-group">
                <label className="form-label">max_workers</label>
                <input className="form-input" type="number" min={1} max={16}
                  value={form.max_workers}
                  onChange={e => setForm(p => ({ ...p, max_workers: Number(e.target.value) }))}
                />
              </div>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div className="form-group">
                <label className="form-label">schema_source</label>
                <input className="form-input" {...field('schema_source')} />
              </div>
              <div className="form-group">
                <label className="form-label">schema_target</label>
                <input className="form-input" {...field('schema_target')} />
              </div>
            </div>

            <div className="form-group">
              <label className="form-label">operation</label>
              <select className="form-input"
                value={form.operation}
                onChange={e => setForm(p => ({ ...p, operation: e.target.value as MigrationRequest['operation'] }))}
              >
                <option value="migration_partitioned">partitioned (S3)</option>
                <option value="migration_fast">fast (parallel S3)</option>
                <option value="migration_simple">simple (full table)</option>
              </select>
            </div>

            {formError && (
              <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)' }}>
                ✗ {formError}
              </div>
            )}

            <button
              className="btn btn--primary"
              onClick={handleSubmit}
              disabled={submitting || progress?.status === 'running'}
            >
              {submitting ? '// initializing...' : '> run_migration'}
            </button>

            {activeJobId && progress?.status !== 'running' && (
              <button
                className="btn btn--ghost"
                onClick={() => { setActiveJobId(null); setForm(DEFAULT_FORM) }}
              >
                new_migration
              </button>
            )}
          </div>
        </div>

        {/* Progress panel */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {!activeJobId ? (
            <div className="card" style={{ textAlign: 'center', padding: '48px 24px' }}>
              <div style={{ color: 'var(--accent-muted)', marginBottom: 8 }}>// awaiting_job</div>
              <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                Configure and run a migration to see real-time progress here.
              </div>
            </div>
          ) : (
            <>
              {/* Stats */}
              <div className="grid-4">
                <StatCard label="status"
                  value={progress?.status ?? 'pending'}
                  accent={progress?.status === 'running'}
                />
                <StatCard label="partitions"
                  value={progress ? `${progress.done_partitions}/${progress.total_partitions ?? '?'}` : '—'}
                  sub={`${progress?.failed_partitions ?? 0} failed`}
                  status={progress?.failed_partitions ? 'warn' : undefined}
                />
                <StatCard label="rows_loaded"
                  value={progress?.loaded_rows?.toLocaleString() ?? '0'}
                />
                <StatCard label="ws_connection"
                  value={connected ? 'connected' : 'reconnecting'}
                  status={connected ? 'ok' : 'warn'}
                />
              </div>

              {/* Progress bar */}
              <div className="card">
                <div className="flex justify-between" style={{ marginBottom: 8 }}>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 'var(--font-size-sm)' }}>
                    {progress?.table_name}
                  </span>
                  {progress && <StatusBadge status={progress.status} />}
                </div>
                <ProgressBar percent={progress?.percent ?? 0} status={progress?.status} />
                {progress?.current_partition && (
                  <div style={{ marginTop: 8, fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                    current partition: {progress.current_partition}
                  </div>
                )}
              </div>

              {/* Log stream */}
              <div className="card" style={{ padding: 0 }}>
                <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--bg-border)' }}>
                  <SectionLabel>execution_log</SectionLabel>
                </div>
                <LogStream logs={logs} maxHeight={350} />
              </div>

              {/* Job ID */}
              <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                job_id: {activeJobId}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
