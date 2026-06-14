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
  const [dataTeste, setDataTeste] = useState(() => {
    const d = new Date()
    d.setDate(d.getDate() - 1)
    return d.toISOString().slice(0, 10)
  })
  const [bskId, setBskId] = useState('')
  const [offsetHours, setOffsetHours] = useState(23)
  const [homologating, setHomologating] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<DimensionHomologateResult | null>(null)

  const handleHomologate = useCallback(async (spec: DimensionSpec) => {
    if (!dataTeste) { setError('Informe a data de teste'); return }
    setError(null)
    setHomologating(true)
    try {
      const form = new FormData()
      form.append('spec_json', JSON.stringify(spec))
      form.append('data_teste', dataTeste)
      form.append('bsk_id', bskId)
      form.append('offset_hours', String(offsetHours))
      const res = await dimensionApi.homologate(form)
      setResult(res)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Erro ao gerar queries')
    } finally {
      setHomologating(false)
    }
  }, [dataTeste, bskId, offsetHours])

  return {
    dataTeste, setDataTeste,
    bskId, setBskId,
    offsetHours, setOffsetHours,
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
