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
  /** Server-decoded rendering keyed by monoid kind. Present when the request set decode=true. */
  decoded?: { int64?: number; byte_len?: number; kind?: string }
}

const base = '/api'

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const r = await fetch(`${base}${path}`, { signal })
  if (!r.ok) {
    throw new Error(`${r.status} ${r.statusText}: ${await r.text().catch(() => '')}`)
  }
  return r.json() as Promise<T>
}

export const api = {
  health: (signal?: AbortSignal) =>
    fetch(`${base}/health`, { signal }).then((r) => r.ok),

  listPipelines: (signal?: AbortSignal) =>
    get<PipelineInfo[]>('/pipelines', signal),

  pipelineMetrics: (name: string, signal?: AbortSignal) =>
    get<PipelineStats>(`/pipelines/${encodeURIComponent(name)}/metrics`, signal),

  getState: (name: string, entity: string, bucket?: number, decode = true) => {
    const q = new URLSearchParams({ entity })
    if (bucket !== undefined) q.set('bucket', String(bucket))
    if (decode) q.set('decode', 'true')
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/state?${q}`)
  },

  getWindow: (name: string, entity: string, durationSeconds: number, decode = true) => {
    const q = new URLSearchParams({ entity, duration_s: String(durationSeconds) })
    if (decode) q.set('decode', 'true')
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/window?${q}`)
  },

  getRange: (name: string, entity: string, startUnix: number, endUnix: number, decode = true) => {
    const q = new URLSearchParams({
      entity,
      start: String(startUnix),
      end: String(endUnix),
    })
    if (decode) q.set('decode', 'true')
    return get<StateValue>(`/pipelines/${encodeURIComponent(name)}/range?${q}`)
  },
}

/**
 * Render a thrown error or rejected response as a single human-readable string.
 * Distinguishes network failures (TypeError: Failed to fetch) from HTTP errors,
 * and falls back to JSON-stringified objects for everything else.
 */
export function formatError(err: unknown): string {
  if (err instanceof TypeError && err.message.includes('fetch')) {
    return 'Network error: could not reach the admin server. Is murmur-ui running?'
  }
  if (err instanceof Error) return err.message
  if (typeof err === 'string') return err
  try {
    return JSON.stringify(err)
  } catch {
    return String(err)
  }
}

/** Decode an int64-little-endian-encoded base64 value (Sum / Count / Min / Max monoids).
 *  Returns null on missing input. Uses BigInt.asIntN to sign-extend correctly across
 *  the full int64 range, including INT64_MIN.
 */
export function decodeInt64LE(data: string | undefined): bigint | null {
  if (!data) return null
  const bin = atob(data)
  if (bin.length < 8) return null
  let v = 0n
  for (let i = 0; i < 8; i++) {
    v |= BigInt(bin.charCodeAt(i)) << (BigInt(i) * 8n)
  }
  return BigInt.asIntN(64, v)
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
