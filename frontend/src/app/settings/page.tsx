'use client'

import { useEffect, useState } from 'react'
import { healthApi } from '@/lib/api'
import type { HealthResponse } from '@/lib/api'
import { SectionLabel, ConnectionDot, StatCard } from '@/components/ui'

export default function SettingsPage() {
  const [health, setHealth] = useState<HealthResponse | null>(null)
  const [testing, setTesting] = useState(false)

  const runHealthCheck = async () => {
    setTesting(true)
    try {
      const h = await healthApi.all()
      setHealth(h)
    } finally {
      setTesting(false)
    }
  }

  useEffect(() => { runHealthCheck() }, [])

  const connections = [
    { key: 'oracle' as const,    label: 'Oracle DB',   envPrefix: 'ORACLE_' },
    { key: 'snowflake' as const, label: 'Snowflake',   envPrefix: 'SNOWFLAKE_' },
    { key: 's3' as const,        label: 'AWS S3',      envPrefix: 'AWS_' },
    { key: 'database' as const,  label: 'PostgreSQL',  envPrefix: 'DB_' },
  ]

  return (
    <div>
      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-sm)', marginBottom: 32 }}>
        Connection configuration is managed via the <code style={{ color: 'var(--accent-primary)' }}>.env</code> file.
        Use this page to test and verify all connections.
      </p>

      {/* Connection health */}
      <SectionLabel>connection_health</SectionLabel>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 32 }}>
        {connections.map(({ key, label, envPrefix }) => {
          const conn = health?.[key]
          const status = conn?.status === 'ok' ? 'ok' : conn ? 'error' : 'pending'
          return (
            <div key={key} className="card" style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
              <ConnectionDot status={status} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 'var(--font-size-sm)', fontWeight: 500 }}>{label}</div>
                <div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--text-muted)', marginTop: 2 }}>
                  env prefix: <span style={{ color: 'var(--accent-primary)' }}>{envPrefix}</span>
                </div>
              </div>
              <div style={{ textAlign: 'right' }}>
                {conn?.status === 'ok' ? (
                  <>
                    <div style={{ color: 'var(--status-success)', fontSize: 'var(--font-size-xs)' }}>
                      ✓ connected
                    </div>
                    <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                      {conn.latency_ms}ms latency
                    </div>
                  </>
                ) : conn?.error ? (
                  <div style={{ color: 'var(--status-error)', fontSize: 'var(--font-size-xs)', maxWidth: 320, textAlign: 'right' }}>
                    ✗ {conn.error}
                  </div>
                ) : (
                  <div style={{ color: 'var(--text-muted)', fontSize: 'var(--font-size-xs)' }}>
                    checking...
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>

      <button className="btn btn--outline" onClick={runHealthCheck} disabled={testing}>
        {testing ? '// running checks...' : '> retest_all_connections'}
      </button>

      {/* .env reference */}
      <div style={{ marginTop: 40 }}>
        <SectionLabel>env_reference</SectionLabel>
        <div className="card">
          <pre style={{
            fontSize: 'var(--font-size-xs)',
            color: 'var(--text-secondary)',
            lineHeight: 2,
            overflowX: 'auto',
          }}>{`# Oracle
ORACLE_HOST=servidor.oracle.com
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=seu_usuario
ORACLE_PASSWORD=sua_senha
ORACLE_SCHEMA=DWADM

# Snowflake
SNOWFLAKE_ACCOUNT=conta.snowflakecomputing.com
SNOWFLAKE_USER=usuario@algar.com.br
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=WH_COMPUTE
SNOWFLAKE_DATABASE=DWDEV
SNOWFLAKE_SCHEMA=DWADM
SNOWFLAKE_AUTHENTICATOR=externalbrowser

# AWS S3
AWS_ACCESS_KEY_ID=sua_access_key
AWS_SECRET_ACCESS_KEY=sua_secret_key
AWS_REGION=us-east-1
S3_BUCKET=bucket-migracao

# PostgreSQL (log database)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=maf_logs
DB_USER=maf_user
DB_PASSWORD=sua_senha`}
          </pre>
        </div>
      </div>

      {/* App info */}
      <div style={{ marginTop: 32 }}>
        <SectionLabel>app_info</SectionLabel>
        <div className="grid-3">
          <StatCard label="version"      value={health?.app_version ?? '—'} />
          <StatCard label="environment"  value={health?.environment ?? '—'} />
          <StatCard label="last_check"   value={health?.timestamp?.substring(11, 19) ?? '—'} sub="UTC" />
        </div>
      </div>
    </div>
  )
}
