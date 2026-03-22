'use client'

import { useEffect, useRef, useState, useCallback } from 'react'
import type { JobProgress, JobStatus } from './api'

const WS_BASE = process.env.NEXT_PUBLIC_WS_URL ?? 'ws://localhost:8000'

export interface ProgressLog {
  timestamp: string
  message: string
  type: 'info' | 'success' | 'error' | 'warn'
}

export interface UseJobProgressResult {
  progress: JobProgress | null
  logs: ProgressLog[]
  connected: boolean
  error: string | null
}

const MAX_LOG_LINES = 500

export function useJobProgress(jobId: string | null): UseJobProgressResult {
  const [progress, setProgress] = useState<JobProgress | null>(null)
  const [logs, setLogs] = useState<ProgressLog[]>([])
  const [connected, setConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const wsRef = useRef<WebSocket | null>(null)
  const pingInterval = useRef<ReturnType<typeof setInterval> | null>(null)
  const reconnectTimeout = useRef<ReturnType<typeof setTimeout> | null>(null)
  const reconnectAttempts = useRef(0)
  const maxReconnects = 5

  const addLog = useCallback((message: string, type: ProgressLog['type'] = 'info') => {
    setLogs(prev => {
      const entry: ProgressLog = {
        timestamp: new Date().toISOString(),
        message,
        type,
      }
      const next = [...prev, entry]
      return next.length > MAX_LOG_LINES ? next.slice(-MAX_LOG_LINES) : next
    })
  }, [])

  const connect = useCallback(() => {
    if (!jobId) return
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    const url = `${WS_BASE}/ws/progress/${jobId}`
    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setConnected(true)
      setError(null)
      reconnectAttempts.current = 0
      addLog('Connection established', 'success')

      // Client-side heartbeat
      pingInterval.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send('ping')
        }
      }, 25_000)
    }

    ws.onmessage = (event: MessageEvent) => {
      try {
        const msg = JSON.parse(event.data as string)

        switch (msg.type) {
          case 'progress':
            setProgress(msg.data as JobProgress)
            if (msg.log) addLog(msg.log, 'info')
            break

          case 'done':
            setProgress(prev => prev ? { ...prev, status: 'done' as JobStatus, percent: 100 } : prev)
            addLog('Migration completed successfully', 'success')
            ws.close(1000, 'Job done')
            break

          case 'error':
            setError(msg.error as string)
            addLog(`Error: ${msg.error}`, 'error')
            setProgress(prev => prev ? { ...prev, status: 'error' as JobStatus } : prev)
            break

          case 'pong':
            // Server ack — do nothing
            break

          default:
            break
        }
      } catch {
        // Non-JSON message — ignore
      }
    }

    ws.onerror = () => {
      setConnected(false)
      addLog('WebSocket error', 'error')
    }

    ws.onclose = (event) => {
      setConnected(false)
      if (pingInterval.current) clearInterval(pingInterval.current)

      if (event.code !== 1000 && reconnectAttempts.current < maxReconnects) {
        const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 15_000)
        reconnectAttempts.current++
        addLog(`Reconnecting in ${delay / 1000}s... (attempt ${reconnectAttempts.current})`, 'warn')
        reconnectTimeout.current = setTimeout(connect, delay)
      }
    }
  }, [jobId, addLog])

  useEffect(() => {
    if (!jobId) return
    connect()

    return () => {
      if (pingInterval.current) clearInterval(pingInterval.current)
      if (reconnectTimeout.current) clearTimeout(reconnectTimeout.current)
      wsRef.current?.close(1000, 'Component unmounted')
    }
  }, [jobId, connect])

  return { progress, logs, connected, error }
}
