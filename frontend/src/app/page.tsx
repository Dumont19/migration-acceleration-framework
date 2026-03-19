'use client'

import { useEffect, useState } from 'react'
import { healthApi, migrationApi, logsApi } from '@/lib/api'
import type { HealthResponse, JobSummary } from '@/lib/api'
import { StatCard, SectionLabel, StatusBadge, ConnectionDot, EmptyState } from '@/components/ui'
import Link from 'next/link'
import styles from './page.module.css'

export default function DashboardPage() {
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [recentJobs, setRecentJobs] = useState<JobSummary[]>([])
  const [logStats, setLogStats] = useState<{ by_level: Record<string, number>; total: number } | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const load = async () => {
      try {
        const [h, jobs, stats] = await Promise.allSettled([
          healthApi.all(),
          migrationApi.listJobs({ page: 1 }),
          logsApi.getStats(),
        ])
        if (h.status === 'fulfilled') setHealth(h.value)
        if (jobs.status === 'fulfilled') setRecentJobs(jobs.value.items.slice(0, 8))
        if (stats.status === 'fulfilled') setLogStats(stats.value)
      } finally {
        setLoading(false)
      }
    }
    load()
    const interval = setInterval(load, 30_000)
    return () => clearInterval(interval)
  }, [])

  const runningJobs = recentJobs.filter(j => j.status === 'running').length
  const errorJobs = recentJobs.filter(j => j.status === 'error').length

  return (
    <div>
      {/* Boot header */}
      <div className={styles.bootHeader}>
        <span className={styles.bootPrefix}>&gt; </span>
        <span className={styles.bootText}>system_initialize</span>
        <span className={styles.bootCursor} />
      </div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Migration Acceleration Framework // Oracle → Snowflake // Algar Telecom
      </p>

      {/* Stats row */}
      <SectionLabel>system_status</SectionLabel>
      <div className="grid-4" style={{ marginBottom: 32 }}>
        <StatCard
          label="active_jobs"
          value={runningJobs}
          sub="currently running"
          accent={runningJobs > 0}
          status={runningJobs > 0 ? 'ok' : undefined}
        />
        <StatCard
          label="failed_jobs"
          value={errorJobs}
          sub="need attention"
          status={errorJobs > 0 ? 'error' : undefined}
        />
        <StatCard
          label="total_log_entries"
          value={logStats?.total?.toLocaleString() ?? '—'}
          sub="audit trail"
        />
        <StatCard
          label="log_errors"
          value={logStats?.by_level?.ERROR?.toLocaleString() ?? '0'}
          sub="error events"
          status={(logStats?.by_level?.ERROR ?? 0) > 0 ? 'error' : 'ok'}
        />
      </div>

      {/* Connections */}
      <SectionLabel>connection_health</SectionLabel>
      <div className="grid-4" style={{ marginBottom: 32 }}>
        {(['oracle', 'snowflake', 's3', 'database'] as const).map((svc) => {
          const conn = health?.[svc]
          const status = conn?.status === 'ok' ? 'ok' : conn ? 'error' : 'pending'
          return (
            <div key={svc} className="card" style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <ConnectionDot status={status} />
              <div>
                <div style={{ fontSize: 'var(--font-size-sm)', color: 'var(--text-primary)' }}>
                  {svc}
                </div>
                <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                  {conn?.status === 'ok'
                    ? `${conn.latency_ms}ms`
                    : conn?.error ?? 'connecting...'}
                </div>
              </div>
              <Link href="/settings" style={{ marginLeft: 'auto', fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)' }}>
                config →
              </Link>
            </div>
          )
        })}
      </div>

      {/* Recent jobs */}
      <SectionLabel>recent_jobs</SectionLabel>
      {recentJobs.length === 0 && !loading ? (
        <EmptyState message="no jobs found — start a migration to begin" />
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>table</th>
                <th>operation</th>
                <th>status</th>
                <th>rows_loaded</th>
                <th>duration</th>
                <th>started</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {recentJobs.map((job) => (
                <tr key={job.job_id}>
                  <td style={{ color: 'var(--accent-primary)' }}>{job.table_name}</td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {job.operation.replace(/_/g, ' ')}
                  </td>
                  <td><StatusBadge status={job.status} /></td>
                  <td style={{ fontVariantNumeric: 'tabular-nums' }}>
                    {job.loaded_rows.toLocaleString()}
                  </td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {job.duration_seconds != null
                      ? `${job.duration_seconds.toFixed(1)}s`
                      : '—'}
                  </td>
                  <td style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    {job.created_at.substring(0, 16).replace('T', ' ')}
                  </td>
                  <td>
                    <Link
                      href={`/migration?job=${job.job_id}`}
                      style={{ color: 'var(--accent-primary)', fontSize: 'var(--font-size-xs)' }}
                    >
                      view →
                    </Link>
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
