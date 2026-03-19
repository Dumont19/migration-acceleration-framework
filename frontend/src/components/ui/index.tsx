'use client'

/**
 * components/ui/index.tsx
 * -----------------------
 * Shared terminal-style UI primitives.
 * All styled with CSS variables from globals.css.
 */

import { useEffect, useRef } from 'react'
import type { JobStatus, LogLevel } from '@/lib/api'
import type { ProgressLog } from '@/lib/useJobProgress'

// ── StatusBadge ───────────────────────────────────────────────────────────────

const STATUS_LABELS: Record<JobStatus, string> = {
  pending:   '○ pending',
  running:   '● running',
  done:      '✓ done',
  error:     '✗ error',
  cancelled: '— cancelled',
}

export function StatusBadge({ status }: { status: JobStatus }) {
  const cls = `badge badge--${status}`
  return <span className={cls}>{STATUS_LABELS[status]}</span>
}

// ── ProgressBar ───────────────────────────────────────────────────────────────

interface ProgressBarProps {
  percent: number
  status?: JobStatus
  showLabel?: boolean
  className?: string
}

export function ProgressBar({ percent, status, showLabel = true, className = '' }: ProgressBarProps) {
  const isError = status === 'error'
  const clamped = Math.min(100, Math.max(0, percent))

  return (
    <div className={className}>
      {showLabel && (
        <div className="flex justify-between text-xs mb-4" style={{ marginBottom: 6 }}>
          <span style={{ color: 'var(--text-muted)' }}>progress</span>
          <span style={{ color: isError ? 'var(--status-error)' : 'var(--accent-primary)' }}>
            {clamped.toFixed(1)}%
          </span>
        </div>
      )}
      <div className="progress-track">
        <div
          className={`progress-fill${isError ? ' progress-fill--error' : ''}`}
          style={{ width: `${clamped}%` }}
        />
      </div>
    </div>
  )
}

// ── LogStream ─────────────────────────────────────────────────────────────────

interface LogStreamProps {
  logs: ProgressLog[]
  maxHeight?: number
  className?: string
}

const LOG_LEVEL_CLASS: Record<ProgressLog['type'], string> = {
  info:    'log-entry--info',
  success: 'log-entry--success',
  warn:    'log-entry--warning',
  error:   'log-entry--error',
}

export function LogStream({ logs, maxHeight = 320, className = '' }: LogStreamProps) {
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [logs])

  if (logs.length === 0) {
    return (
      <div className={`log-stream ${className}`} style={{ maxHeight }}>
        <span className="log-entry log-entry--info">
          <span className="log-timestamp">--:--:--</span>
          waiting for events...
        </span>
      </div>
    )
  }

  return (
    <div className={`log-stream ${className}`} style={{ maxHeight }}>
      {logs.map((log, i) => (
        <span key={i} className={`log-entry ${LOG_LEVEL_CLASS[log.type]}`}>
          <span className="log-timestamp">
            {log.timestamp.substring(11, 19)}
          </span>
          {log.message}
          {'\n'}
        </span>
      ))}
      <div ref={bottomRef} />
    </div>
  )
}

// ── StatCard ──────────────────────────────────────────────────────────────────

interface StatCardProps {
  label: string
  value: string | number
  sub?: string
  accent?: boolean
  status?: 'ok' | 'error' | 'warn'
}

export function StatCard({ label, value, sub, accent, status }: StatCardProps) {
  const borderColor = status === 'ok' ? 'var(--accent-primary)'
    : status === 'error' ? 'var(--status-error)'
    : status === 'warn' ? 'var(--status-warn)'
    : undefined

  return (
    <div
      className="card"
      style={borderColor ? { borderLeftColor: borderColor, borderLeftWidth: 2 } : undefined}
    >
      <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginBottom: 8 }}>
        {label}
      </div>
      <div style={{
        fontSize: 'var(--font-size-2xl)',
        fontWeight: 700,
        color: accent ? 'var(--accent-primary)' : 'var(--text-primary)',
        lineHeight: 1,
      }}>
        {value}
      </div>
      {sub && (
        <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginTop: 6 }}>
          {sub}
        </div>
      )}
    </div>
  )
}

// ── SectionLabel ──────────────────────────────────────────────────────────────

export function SectionLabel({ children }: { children: React.ReactNode }) {
  return <div className="section-label">{children}</div>
}

// ── ConnectionDot ─────────────────────────────────────────────────────────────

export function ConnectionDot({ status }: { status: 'ok' | 'error' | 'pending' | 'warn' }) {
  return <span className={`status-dot status-dot--${status}`} />
}

// ── CodeBlock ─────────────────────────────────────────────────────────────────

export function CodeBlock({ children, language = '' }: { children: string; language?: string }) {
  return (
    <pre style={{
      background: 'var(--bg-primary)',
      border: '1px solid var(--bg-border)',
      borderRadius: 'var(--radius)',
      padding: '12px 16px',
      fontSize: 'var(--font-size-xs)',
      color: 'var(--accent-primary)',
      overflowX: 'auto',
      lineHeight: 1.7,
    }}>
      <code>{children}</code>
    </pre>
  )
}

// ── EmptyState ────────────────────────────────────────────────────────────────

export function EmptyState({ message = 'no data available' }: { message?: string }) {
  return (
    <div style={{
      textAlign: 'center',
      padding: '48px 24px',
      color: 'var(--text-muted)',
      fontSize: 'var(--font-size-sm)',
    }}>
      <div style={{ marginBottom: 8, color: 'var(--accent-muted)' }}>{'// '}{message}</div>
      <div style={{ fontSize: 'var(--font-size-xs)' }}>_</div>
    </div>
  )
}
