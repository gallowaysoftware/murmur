import { Link } from 'react-router-dom'
import { useEffect, useState } from 'react'
import { api, type PipelineInfo, type PipelineStats, formatNumber, formatDuration } from '../api'

type Row = { info: PipelineInfo; stats: PipelineStats | null }

export function Pipelines() {
  const [rows, setRows] = useState<Row[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const infos = await api.listPipelines()
        const enriched = await Promise.all(
          infos.map(async (info) => {
            try {
              const stats = await api.pipelineMetrics(info.name)
              return { info, stats }
            } catch {
              return { info, stats: null }
            }
          }),
        )
        if (!cancelled) {
          setRows(enriched)
          setError(null)
        }
      } catch (e) {
        if (!cancelled) setError(String(e))
      }
    }
    tick()
    const id = setInterval(tick, 2000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  return (
    <div className="px-10 py-8 max-w-6xl">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-fg-strong tracking-tight">Pipelines</h1>
        <p className="text-fg-muted text-sm mt-1">
          Live metrics across registered pipelines. Refreshes every 2 s.
        </p>
      </header>

      {error && (
        <div className="rounded-lg border border-bad bg-bad-bg text-fg-strong p-4 mb-4 text-sm">
          {error}
        </div>
      )}

      {rows === null && !error && (
        <div className="text-fg-muted text-sm">Loading…</div>
      )}

      {rows && rows.length === 0 && (
        <EmptyState />
      )}

      {rows && rows.length > 0 && (
        <div className="grid gap-3">
          {rows.map((r) => (
            <PipelineCard key={r.info.name} row={r} />
          ))}
        </div>
      )}
    </div>
  )
}

function PipelineCard({ row }: { row: Row }) {
  const { info, stats } = row
  const eps = (() => {
    if (!stats || !stats.last_event_at) return null
    // Approximate events/sec from cumulative count + run window — best done with rate-of-change,
    // but for the dashboard we just expose totals + most-recent latency.
    return null
  })()

  const storeMerge = stats?.latencies?.['store_merge']
  const cacheMerge = stats?.latencies?.['cache_merge']

  return (
    <Link
      to={`/pipelines/${encodeURIComponent(info.name)}`}
      className="block rounded-lg border border-border bg-surface hover:bg-surface-2 hover:border-border-strong transition-colors p-5"
    >
      <div className="flex items-start justify-between gap-6">
        <div>
          <div className="font-mono text-fg-strong text-base">{info.name}</div>
          <div className="text-xs text-fg-muted mt-1 flex flex-wrap gap-2">
            <Badge>{info.monoid_kind}</Badge>
            {info.windowed && (
              <Badge>
                windowed · {info.window_granularity_seconds}s buckets · {info.window_retention_seconds}s retention
              </Badge>
            )}
            <Badge>store: {info.store_type}</Badge>
            {info.cache_type && <Badge>cache: {info.cache_type}</Badge>}
            {info.source_type && <Badge>source: {info.source_type}</Badge>}
          </div>
        </div>
        <StatusDot stats={stats} />
      </div>

      <div className="mt-4 grid grid-cols-4 gap-6">
        <Stat label="events" value={formatNumber(stats?.events_processed)} />
        <Stat
          label="errors"
          value={formatNumber(stats?.errors)}
          tone={stats && stats.errors > 0 ? 'bad' : 'neutral'}
        />
        <Stat label="store p95" value={formatDuration(storeMerge?.p95_ms)} mono />
        <Stat label="cache p95" value={cacheMerge ? formatDuration(cacheMerge.p95_ms) : '—'} mono />
      </div>

      {stats?.last_error && (
        <div className="mt-3 text-xs text-bad font-mono truncate" title={stats.last_error}>
          last error: {stats.last_error}
        </div>
      )}
      {eps !== null && (
        <div className="mt-1 text-xs text-fg-muted">
          {eps} events/sec
        </div>
      )}
    </Link>
  )
}

function StatusDot({ stats }: { stats: PipelineStats | null }) {
  if (!stats) {
    return <span className="text-xs text-fg-faint">no data</span>
  }
  const fresh = stats.last_event_at
    ? Date.now() - new Date(stats.last_event_at).getTime() < 30_000
    : false
  const tone = stats.errors > 0 ? 'warn' : fresh ? 'good' : 'neutral'
  const color = tone === 'good' ? 'bg-good' : tone === 'warn' ? 'bg-warn' : 'bg-fg-faint'
  return (
    <span className="inline-flex items-center gap-2 text-xs text-fg-muted">
      <span className={`inline-block w-2 h-2 rounded-full ${color}`} />
      {tone === 'good' ? 'live' : tone === 'warn' ? 'errors' : 'idle'}
    </span>
  )
}

function Badge({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-md bg-surface-2 border border-border text-fg-muted font-mono text-[11px]">
      {children}
    </span>
  )
}

function Stat({
  label,
  value,
  mono = false,
  tone = 'neutral',
}: {
  label: string
  value: string
  mono?: boolean
  tone?: 'neutral' | 'bad'
}) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wider text-fg-faint">{label}</div>
      <div
        className={`mt-0.5 text-lg ${mono ? 'font-mono' : 'font-medium'} ${
          tone === 'bad' ? 'text-bad' : 'text-fg-strong'
        }`}
      >
        {value}
      </div>
    </div>
  )
}

function EmptyState() {
  return (
    <div className="rounded-lg border border-dashed border-border-strong bg-surface p-10 text-center">
      <div className="text-fg-strong font-medium">No pipelines registered</div>
      <p className="text-fg-muted text-sm mt-2 max-w-md mx-auto">
        Start a worker process and register the pipeline with the admin server. The
        page-view-counters example does this when you set <code className="font-mono text-fg-strong">MURMUR_ADMIN_ADDR</code>.
      </p>
    </div>
  )
}
