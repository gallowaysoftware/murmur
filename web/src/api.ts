/**
 * Murmur admin API client for the web UI.
 *
 * The wire contract is defined in `proto/murmur/admin/v1/admin.proto` and
 * compiled to TypeScript via `npx buf generate`. The Go server in
 * `pkg/admin` implements the same `AdminService` over Connect-RPC, which
 * means this client speaks plain HTTP+JSON to the server — no proxy, no
 * gRPC-Web transcoding, no special transport. Anyone can swap this UI for
 * their own client by importing the generated `AdminService` and pointing
 * `createConnectTransport` at the same baseUrl.
 *
 * The hand-rolled types and helpers below preserve the small surface area
 * the rest of the UI consumed before the proto migration so the page
 * components don't need invasive changes.
 */

import { createClient, type Client } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import {
  AdminService,
  type PipelineInfo as PbPipelineInfo,
  type PipelineStats as PbPipelineStats,
  type StateValue as PbStateValue,
  type LatencyStats as PbLatencyStats,
} from './gen/murmur/admin/v1/admin_pb.js'

const transport = createConnectTransport({
  // Same-origin in production (UI embedded by murmur-ui); the dev server
  // proxies /api → :8080 via vite.config.ts.
  baseUrl: '/api',
})

const client: Client<typeof AdminService> = createClient(AdminService, transport)

// --- Public types preserved for the rest of the UI ---

/** Pipeline metadata as displayed in the dashboard. */
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
  /** Base64-encoded raw bytes. */
  data?: string
  decoded?: { int64?: number; byte_len?: number; kind?: string }
}

// --- Conversion helpers (proto → UI types) ---

function pipelineInfoFromPb(p: PbPipelineInfo): PipelineInfo {
  return {
    name: p.name,
    monoid_kind: p.monoidKind,
    windowed: p.windowed,
    window_granularity_seconds: Number(p.windowGranularitySeconds),
    window_retention_seconds: Number(p.windowRetentionSeconds),
    store_type: p.storeType,
    cache_type: p.cacheType || undefined,
    source_type: p.sourceType || undefined,
  }
}

function latencyFromPb(l: PbLatencyStats): LatencyJSON {
  return {
    n: Number(l.n),
    p50_ms: l.p50Ms,
    p95_ms: l.p95Ms,
    p99_ms: l.p99Ms,
    max_ms: l.maxMs,
  }
}

function statsFromPb(s: PbPipelineStats): PipelineStats {
  const latencies: Record<string, LatencyJSON> = {}
  for (const [op, lat] of Object.entries(s.latencies)) {
    latencies[op] = latencyFromPb(lat)
  }
  return {
    pipeline: s.pipeline,
    events_processed: Number(s.eventsProcessed),
    errors: Number(s.errors),
    last_event_at: s.lastEventAt || undefined,
    last_error_at: s.lastErrorAt || undefined,
    last_error: s.lastError || undefined,
    latencies,
  }
}

function stateFromPb(v: PbStateValue): StateValue {
  const out: StateValue = { present: v.present }
  if (v.data && v.data.length > 0) {
    out.data = bytesToBase64(v.data)
  }
  if (v.decoded) {
    const d = v.decoded.value
    if (d?.case === 'int64Value') {
      out.decoded = { int64: Number(d.value) }
    } else if (d?.case === 'opaque') {
      out.decoded = { byte_len: Number(d.value.byteLen), kind: d.value.kind }
    }
  }
  return out
}

function bytesToBase64(bytes: Uint8Array): string {
  let s = ''
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i])
  return btoa(s)
}

// --- Public API: thin wrapper around the generated Connect client ---

export const api = {
  health: async (signal?: AbortSignal): Promise<boolean> => {
    try {
      const r = await client.health({}, { signal })
      return r.status === 'ok'
    } catch {
      return false
    }
  },

  listPipelines: async (signal?: AbortSignal): Promise<PipelineInfo[]> => {
    const r = await client.listPipelines({}, { signal })
    return r.pipelines.map(pipelineInfoFromPb)
  },

  pipelineMetrics: async (name: string, signal?: AbortSignal): Promise<PipelineStats> => {
    const r = await client.getPipelineMetrics({ name }, { signal })
    return statsFromPb(r)
  },

  getState: async (
    name: string,
    entity: string,
    bucket?: number,
    decode = true,
  ): Promise<StateValue> => {
    const r = await client.getState({
      name,
      entity,
      bucket: bucket !== undefined ? BigInt(bucket) : 0n,
      decode,
    })
    return stateFromPb(r)
  },

  getWindow: async (
    name: string,
    entity: string,
    durationSeconds: number,
    decode = true,
  ): Promise<StateValue> => {
    const r = await client.getWindow({
      name,
      entity,
      durationSeconds: BigInt(durationSeconds),
      decode,
    })
    return stateFromPb(r)
  },

  getRange: async (
    name: string,
    entity: string,
    startUnix: number,
    endUnix: number,
    decode = true,
  ): Promise<StateValue> => {
    const r = await client.getRange({
      name,
      entity,
      startUnix: BigInt(startUnix),
      endUnix: BigInt(endUnix),
      decode,
    })
    return stateFromPb(r)
  },
}

// --- Error rendering ---

/**
 * Render a thrown Connect error or any other failure as a single
 * human-readable string. Connect errors carry a `code` and `message`; we
 * surface the message and translate the most common network/AbortError cases.
 */
export function formatError(err: unknown): string {
  if (err instanceof TypeError && err.message.includes('fetch')) {
    return 'Network error: could not reach the admin server. Is murmur-ui running?'
  }
  if (err && typeof err === 'object' && 'message' in err) {
    return String((err as { message: unknown }).message)
  }
  if (typeof err === 'string') return err
  try {
    return JSON.stringify(err)
  } catch {
    return String(err)
  }
}

// --- Display helpers ---

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

/**
 * decodeInt64LE survives in the public surface for any caller that still
 * decodes raw `data` bytes. The server-side decode=true path now returns the
 * int64 directly via DecodedValue, so this helper is rarely needed.
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
