const BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

// ── Types (mirrors app/models/schemas.py) ────────────────────────────────────

export type JobStatus = 'pending' | 'running' | 'done' | 'error' | 'cancelled'
export type OperationType =
  | 'migration_partitioned' | 'migration_fast' | 'migration_dblink'
  | 'migration_simple' | 'validation' | 'gap_analysis'
  | 'datastage_doc' | 'lineage_build' | 'metadata_extract'
  | 'table_create' | 'merge_run' | 'copy_s3'
export type LogLevel = 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'CRITICAL'

export interface MigrationRequest {
  table_name: string
  operation?: OperationType
  date_from?: string
  date_to?: string
  partition_column?: string
  batch_size_days?: number
  max_workers?: number
  use_dblink?: boolean
  schema_source?: string
  schema_target?: string
}

export interface JobProgress {
  job_id: string
  table_name: string
  operation: OperationType
  status: JobStatus
  total_partitions: number | null
  done_partitions: number
  failed_partitions: number
  percent: number
  total_rows: number | null
  loaded_rows: number
  started_at: string | null
  estimated_completion: string | null
  current_partition: string | null
  last_log_message: string | null
  updated_at: string
}

export interface JobSummary {
  job_id: string
  table_name: string
  operation: OperationType
  status: JobStatus
  duration_seconds: number | null
  loaded_rows: number
  failed_partitions: number
  created_at: string
}

export interface JobListResponse {
  items: JobSummary[]
  total: number
  page: number
  page_size: number
}

export interface PartitionStatus {
  partition_key: string
  status: JobStatus
  rows_loaded: number
  attempts: number
  error_message: string | null
  started_at: string | null
  finished_at: string | null
}

export interface ValidationRequest {
  table_name: string
  sample_size?: number
  check_schema?: boolean
  check_counts?: boolean
  check_sample?: boolean
  date_filter?: string
}

export interface ValidationResult {
  id: string
  table_name: string
  oracle_count: number | null
  snowflake_count: number | null
  count_diff: number | null
  count_match: boolean | null
  schema_match: boolean | null
  schema_diff: Record<string, unknown> | null
  sample_size: number | null
  sample_match_rate: number | null
  passed: boolean | null
  notes: string | null
  created_at: string
}

export interface ConnectionHealth {
  status: 'ok' | 'error'
  latency_ms: number | null
  error: string | null
  extra: Record<string, unknown> | null
}

export interface HealthResponse {
  oracle: ConnectionHealth
  snowflake: ConnectionHealth
  s3: ConnectionHealth
  database: ConnectionHealth
  app_version: string
  environment: string
  timestamp: string
}

export interface LogEntry {
  id: number
  job_id: string | null
  table_name: string | null
  operation: string | null
  level: LogLevel
  message: string
  extra: Record<string, unknown> | null
  created_at: string
}

export interface LogsResponse {
  items: LogEntry[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface GapResult {
  table_name: string
  date: string
  oracle_count: number
  snowflake_count: number
  diff: number
  diff_pct: number
}

export interface LineageNode {
  id: string
  label: string
  type: 'source' | 'job' | 'target'
  schema: string | null
  extra: Record<string, unknown> | null
}

export interface LineageEdge {
  source: string
  target: string
  label: string | null
}

export interface LineageGraph {
  nodes: LineageNode[]
  edges: LineageEdge[]
  job_name: string
  generated_at: string
}

// ── Core fetch helper ─────────────────────────────────────────────────────────

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message)
    this.name = 'ApiError'
  }
}

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  })
  if (!res.ok) {
    const body = await res.text()
    let detail = body
    try { detail = JSON.parse(body).detail ?? body } catch { /* noop */ }
    throw new ApiError(res.status, detail)
  }
  const text = await res.text()
  return text ? JSON.parse(text) : ({} as T)
}

/**
 * Envia multipart/form-data para endpoints FastAPI que usam File(...).
 * NÃO usa o helper request() — ele força Content-Type: application/json,
 * o que quebra o parsing de multipart no FastAPI.
 * O browser define o boundary automaticamente quando body é FormData.
 */
async function requestForm<T>(path: string, form: FormData): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, { method: 'POST', body: form })
  if (!res.ok) {
    const body = await res.text()
    let detail = body
    try { detail = JSON.parse(body).detail ?? body } catch { /* noop */ }
    throw new ApiError(res.status, detail)
  }
  const text = await res.text()
  return text ? JSON.parse(text) : ({} as T)
}

// ── Migration ─────────────────────────────────────────────────────────────────

export const migrationApi = {
  start: (req: MigrationRequest) =>
    request<{ job_id: string; ws_url: string; poll_url: string }>(
      '/api/migration/start', { method: 'POST', body: JSON.stringify(req) }
    ),

  listJobs: (params?: { page?: number; table_name?: string; status?: JobStatus }) => {
    const qs = new URLSearchParams()
    if (params?.page)       qs.set('page', String(params.page))
    if (params?.table_name) qs.set('table_name', params.table_name)
    if (params?.status)     qs.set('status', params.status)
    return request<JobListResponse>(`/api/migration/jobs?${qs}`)
  },

  getProgress:   (jobId: string) => request<JobProgress>(`/api/migration/jobs/${jobId}`),
  getPartitions: (jobId: string) => request<PartitionStatus[]>(`/api/migration/jobs/${jobId}/partitions`),
  cancelJob:     (jobId: string) => request<{ status: string }>(`/api/migration/jobs/${jobId}/cancel`, { method: 'POST' }),
}

// ── Validation ────────────────────────────────────────────────────────────────

export const validationApi = {
  run: (req: ValidationRequest) =>
    request<ValidationResult>('/api/validation/run', { method: 'POST', body: JSON.stringify(req) }),

  getHistory: (tableName?: string) => {
    const qs = tableName ? `?table_name=${tableName}` : ''
    return request<ValidationResult[]>(`/api/validation/history${qs}`)
  },
}

// ── DataStage — todas as rotas sob /api/datastage ─────────────────────────────
// Routers registrados em main.py: health, migration, logs, datastage.
// NÃO existe /api/analysis — gap analysis e lineage vivem em /api/datastage.

export const datastageApi = {
  /**
   * POST /api/datastage/analyze
   * Analisa um .dsx/.xml e retorna metadados estruturados dos jobs.
   */
  analyze: (form: FormData) =>
    requestForm<{ jobs: unknown[] }>('/api/datastage/analyze', form),

  /**
   * POST /api/datastage/lineage
   * Gera o grafo de linhagem SOURCE → JOB → TARGET a partir de um .dsx/.xml.
   */
  lineage: (form: FormData) =>
    requestForm<LineageGraph>('/api/datastage/lineage', form),

  /**
   * POST /api/datastage/report
   * Retorna HTML bruto — use fetch direto para blob (ver job_docs page).
   */
  reportRaw: async (form: FormData): Promise<Blob> => {
    const res = await fetch(`${BASE_URL}/api/datastage/report`, { method: 'POST', body: form })
    if (!res.ok) throw new ApiError(res.status, `HTTP ${res.status}`)
    return res.blob()
  },
}

// ── Analysis (somente gap analysis — lineage migrada para datastageApi) ────────

export const analysisApi = {
  runGapAnalysis: (req: {
    table_name: string
    date_from: string
    date_to: string
    date_column?: string
    granularity?: string
  }) =>
    request<GapResult[]>('/api/analysis/gaps', { method: 'POST', body: JSON.stringify(req) }),
}

// ── Logs ──────────────────────────────────────────────────────────────────────

export const logsApi = {
  query: (params: {
    job_id?: string
    table_name?: string
    level?: LogLevel
    from_dt?: string
    to_dt?: string
    search?: string
    page?: number
    page_size?: number
  }) => {
    const qs = new URLSearchParams()
    Object.entries(params).forEach(([k, v]) => { if (v) qs.set(k, String(v)) })
    return request<LogsResponse>(`/api/logs?${qs}`)
  },

  getJobLogs: (jobId: string, level?: LogLevel) => {
    const qs = level ? `?level=${level}` : ''
    return request<LogEntry[]>(`/api/logs/job/${jobId}${qs}`)
  },

  getStats: () =>
    request<{
      by_level: Record<string, number>
      top_error_tables: { table: string; errors: number }[]
      total: number
    }>('/api/logs/stats'),
}

// ── Health ────────────────────────────────────────────────────────────────────

export const healthApi = {
  all:       () => request<HealthResponse>('/api/health'),
  oracle:    () => request<ConnectionHealth>('/api/health/oracle'),
  snowflake: () => request<ConnectionHealth>('/api/health/snowflake'),
  s3:        () => request<ConnectionHealth>('/api/health/s3'),
  database:  () => request<ConnectionHealth>('/api/health/database'),
}

export { ApiError }