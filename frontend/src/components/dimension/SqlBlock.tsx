'use client'

import { useState } from 'react'

const COPY_RESET_MS = 1800

/** Botão que copia texto para a área de transferência e exibe confirmação temporária. */
export function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = () => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), COPY_RESET_MS)
    })
  }

  return (
    <button
      onClick={handleCopy}
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

const SQL_BLOCK_MAX_HEIGHT = 320

/** Bloco de código SQL com label e botão de cópia. */
export function SqlBlock({ label, sql }: { label: string; sql: string }) {
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
        maxHeight: SQL_BLOCK_MAX_HEIGHT,
        overflowY: 'auto',
      }}>
        <code>{sql}</code>
      </pre>
    </div>
  )
}
