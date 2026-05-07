/**
 * Type-safe client for Murmur's admin REST API. Mirrors pkg/admin/server.go's wire
 * shapes; keep these in sync if you add endpoints there.
 */

export type PipelineInfo = {
  name: string
  monoid_kind: string
  windowed: boolean
  window_granularity_seconds?: number
  window_retention_seconds?: number
  store_type: string
  cache_type?: string
  source_type?: string
}

export type LatencyJSON = {
  n: number
  p50_ms: number
  p95_ms: number
  p99_ms: number
  max_ms: number
}

export type PipelineStats = {
  pipeline: string
  events_processed: number
  errors: number
  last_event_at?: string
  last_error_at?: string
  last_error?: string
  latencies: Record<string, LatencyJSON>
}

export type StateValue = {
  present: boolean
  /** Base64-encoded bytes; decoders are monoid-specific. */
  data?: string
}

const base = '/api'

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${base}${path}`)
  if (!r.ok) {
    throw new Error(`${r.status} ${r.statusText}: ${await r.text().catch(() => '')}`)
  }
  return r.json() as Promise<T>
}

export const api = {
  health: () => fetch(`${base}/health`).then((r) => r.ok),

  listPipelines: () => get<PipelineInfo[]>('/pipelines'),

  pipelineMetrics: (name: string) =>
    get<PipelineStats>(`/pipelines/${encodeURIComponent(name)}/metrics`),

  getState: (name: string, entity: string, bucket?: number) => {
    const q = new URLSearchParams({ entity })
    if (bucket !== undefined) q.set('bucket', String(bucket))
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/state?${q}`)
  },

  getWindow: (name: string, entity: string, durationSeconds: number) => {
    const q = new URLSearchParams({ entity, duration_s: String(durationSeconds) })
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/window?${q}`)
  },

  getRange: (name: string, entity: string, startUnix: number, endUnix: number) => {
    const q = new URLSearchParams({
      entity,
      start: String(startUnix),
      end: String(endUnix),
    })
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/range?${q}`)
  },
}

/** Decode an int64-little-endian-encoded base64 value (Sum / Count / Min / Max monoids). */
export function decodeInt64LE(data: string | undefined): bigint | null {
  if (!data) return null
  const bin = atob(data)
  if (bin.length < 8) return null
  let v = 0n
  for (let i = 0; i < 8; i++) {
    v |= BigInt(bin.charCodeAt(i)) << (BigInt(i) * 8n)
  }
  // Sign-extend.
  const sign = 1n << 63n
  if (v & sign) v = v - (1n << 64n)
  return v
}

/** Format a number with comma separators. Works on number | bigint | null. */
export function formatNumber(n: number | bigint | null | undefined): string {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
}

export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return '—'
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`
  if (ms < 1000) return `${ms.toFixed(2)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}
