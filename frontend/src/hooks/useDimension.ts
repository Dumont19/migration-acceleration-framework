'use client'

import { useState, useCallback, useRef } from 'react'
import { dimensionApi } from '@/lib/api'
import type {
  DimensionSpec,
  DimensionGenerateResult,
  DimensionHomologateResult,
  DimensionJobSummary,
} from '@/lib/api'

export type DimensionTab = 'upload' | 'generate' | 'homologate' | 'history'

/** Estado e handlers para o fluxo de upload e análise de DSX. */
export function useDimensionUpload(onSuccess: (spec: DimensionSpec, tab: DimensionTab) => void) {
  const fileRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleUpload = useCallback(async () => {
    const file = fileRef.current?.files?.[0]
    if (!file) { setError('Selecione um arquivo .dsx ou .xml'); return }
    setError(null)
    setUploading(true)
    try {
      const form = new FormData()
      form.append('file', file)
      const spec = await dimensionApi.analyze(form)
      onSuccess(spec, 'generate')
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Erro ao analisar XML')
    } finally {
      setUploading(false)
    }
  }, [onSuccess])

  return { fileRef, uploading, error, handleUpload }
}

/** Estado e handlers para geração de SQLs a partir do DimensionSpec. */
export function useDimensionGenerate() {
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<DimensionGenerateResult | null>(null)

  const handleGenerate = useCallback(async (spec: DimensionSpec) => {
    setError(null)
    setGenerating(true)
    try {
      const res = await dimensionApi.generate(spec)
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Erro ao gerar SQL')
    } finally {
      setGenerating(false)
    }
  }, [])

  return { generating, error, result, handleGenerate }
}

/** Estado e handlers para geração de queries de homologação. */
export function useDimensionHomologate() {
  const [prodTable, setProdTable] = useState('')
  const [timeTravelOffset, setTimeTravelOffset] = useState(0)
  const [homologating, setHomologating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<DimensionHomologateResult | null>(null)

  const handleHomologate = useCallback(async (spec: DimensionSpec) => {
    if (!prodTable.trim()) { setError('Informe a tabela PROD'); return }
    setError(null)
    setHomologating(true)
    try {
      const form = new FormData()
      Object.entries(spec).forEach(([k, v]) => form.append(k, JSON.stringify(v)))
      form.set('prod_table', prodTable.trim())
      form.set('time_travel_offset', String(timeTravelOffset))
      const res = await dimensionApi.homologate(form)
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Erro ao gerar queries')
    } finally {
      setHomologating(false)
    }
  }, [prodTable, timeTravelOffset])

  return {
    prodTable, setProdTable,
    timeTravelOffset, setTimeTravelOffset,
    homologating, error, result,
    handleHomologate,
  }
}

/** Estado e handlers para o histórico de jobs de dimensão. */
export function useDimensionHistory() {
  const [jobs, setJobs] = useState<DimensionJobSummary[]>([])
  const [loading, setLoading] = useState(false)

  const handleLoad = useCallback(async () => {
    setLoading(true)
    try {
      const list = await dimensionApi.listJobs()
      setJobs(list)
    } catch {
      // Falha silenciosa — histórico não é crítico
    } finally {
      setLoading(false)
    }
  }, [])

  return { jobs, loading, handleLoad }
}
