import { Link, useSearchParams } from 'react-router-dom'
import { useEffect, useMemo } from 'react'
import {
  api,
  formatError,
  formatDuration,
  formatNumber,
  type PipelineInfo,
  type PipelineStats,
} from '../api'
import { useLivePolling, useNow } from '../hooks/useLivePolling'

type Row = { info: PipelineInfo; stats: PipelineStats | null }

export function Pipelines() {
  useEffect(() => {
    document.title = 'Murmur · Pipelines'
  }, [])

  const [search, setSearch] = useSearchParams()
  const q = search.get('q') ?? ''
  const kindFilter = search.get('kind') ?? ''
  const onlyErrors = search.get('errors') === '1'

  const setParam = (k: string, v: string) => {
    const next = new URLSearchParams(search)
    if (v) next.set(k, v)
    else next.delete(k)
    setSearch(next, { replace: false })
  }

  const { data: rows, error } = useLivePolling<Row[]>(async (signal) => {
    const infos = await api.listPipelines(signal)
    const enriched = await Promise.all(
      infos.map(async (info) => {
        try {
          const stats = await api.pipelineMetrics(info.name, signal)
          return { info, stats }
        } catch {
          return { info, stats: null as PipelineStats | null }
        }
      }),
    )
    return enriched
  }, 2000)

  // Distinct monoid kinds present in the current pipeline set, sorted. Used to
  // populate the kind quick-filter chips.
  const kinds = useMemo(() => {
    const set = new Set<string>()
    for (const r of rows ?? []) set.add(r.info.monoid_kind)
    return [...set].sort()
  }, [rows])

  // Filter rows client-side. Server-side filtering is a future move once we
  // get past ~hundreds of pipelines; today the entire list fits in one
  // ListPipelines RPC and filtering in the browser is cheaper than round-trips.
  const filtered = useMemo(() => {
    if (!rows) return null
    const needle = q.trim().toLowerCase()
    return rows.filter(({ info, stats }) => {
      if (kindFilter && info.monoid_kind !== kindFilter) return false
      if (onlyErrors && (!stats || stats.errors === 0)) return false
      if (!needle) return true
      const haystack = [
        info.name,
        info.monoid_kind,
        info.store_type,
        info.cache_type ?? '',
        info.source_type ?? '',
      ]
        .join(' ')
        .toLowerCase()
      return haystack.includes(needle)
    })
  }, [rows, q, kindFilter, onlyErrors])

  return (
    <div className="px-6 sm:px-10 py-8 max-w-6xl">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-fg-strong tracking-tight">Pipelines</h1>
        <p className="text-fg-muted text-sm mt-1">
          Live metrics across registered pipelines. Refreshes every 2 s; pauses when the
          tab is hidden.
        </p>
      </header>

      {rows && rows.length > 0 && (
        <div className="mb-4 grid gap-3">
          <input
            type="search"
            value={q}
            onChange={(e) => setParam('q', e.target.value)}
            placeholder="Filter by name, monoid kind, store, source, cache…"
            aria-label="Filter pipelines"
            className="w-full bg-surface border border-border rounded-md px-3 py-2 text-fg-strong placeholder:text-fg-faint"
          />
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <button
              type="button"
              onClick={() => setParam('kind', '')}
              className={chipClass(!kindFilter)}
            >
              all kinds
            </button>
            {kinds.map((k) => (
              <button
                key={k}
                type="button"
                onClick={() => setParam('kind', k === kindFilter ? '' : k)}
                className={chipClass(kindFilter === k)}
              >
                {k}
              </button>
            ))}
            <span className="mx-1 text-fg-faint">·</span>
            <button
              type="button"
              onClick={() => setParam('errors', onlyErrors ? '' : '1')}
              className={chipClass(onlyErrors)}
            >
              with errors
            </button>
            {(q || kindFilter || onlyErrors) && filtered && rows && (
              <span className="ml-auto text-fg-muted tabular-nums">
                {filtered.length} of {rows.length}
              </span>
            )}
          </div>
        </div>
      )}

      {error && (
        <div
          role="alert"
          className="rounded-lg border border-bad bg-bad-bg text-fg-strong p-4 mb-4 text-sm"
        >
          {formatError(error)}
        </div>
      )}

      {rows === null && !error && <div className="text-fg-muted text-sm">Loading…</div>}

      {rows && rows.length === 0 && <EmptyState />}

      {rows && rows.length > 0 && filtered && filtered.length === 0 && (
        <FilteredOutState />
      )}

      {filtered && filtered.length > 0 && (
        <div className="grid gap-3" aria-live="polite">
          {filtered.map((r) => (
            <PipelineCard key={r.info.name} row={r} />
          ))}
        </div>
      )}
    </div>
  )
}

function chipClass(active: boolean): string {
  return (
    'px-2 py-0.5 rounded-md border font-mono transition-colors ' +
    (active
      ? 'bg-accent-bg border-accent-border text-accent-strong'
      : 'bg-surface border-border text-fg-muted hover:text-fg-strong hover:bg-surface-2')
  )
}

function FilteredOutState() {
  return (
    <div className="rounded-lg border border-dashed border-border-strong bg-surface p-8 text-center">
      <div className="text-fg-strong font-medium">No pipelines match the current filter.</div>
      <p className="text-fg-muted text-sm mt-2">
        Clear the search box or pick a different kind chip to widen the view.
      </p>
    </div>
  )
}

function PipelineCard({ row }: { row: Row }) {
  const { info, stats } = row
  const storeMerge = stats?.latencies?.['store_merge']
  const cacheMerge = stats?.latencies?.['cache_merge']
  // Compute events/sec via wall-clock-driven re-render so the value freshens
  // independent of polling cadence.
  const now = useNow(2000)
  const eps = useEpsEstimate(stats, now)

  return (
    <Link
      to={`/pipelines/${encodeURIComponent(info.name)}`}
      aria-label={`Pipeline ${info.name}, ${info.monoid_kind}, ${
        stats?.events_processed ?? 'unknown'
      } events processed`}
      className="block rounded-lg border border-border bg-surface hover:bg-surface-2 hover:border-border-strong transition-colors p-5"
    >
      <div className="flex items-start justify-between gap-6">
        <div>
          <div className="font-mono text-fg-strong text-base">{info.name}</div>
          <div className="text-xs text-fg-muted mt-1 flex flex-wrap gap-2">
            <Badge>{info.monoid_kind}</Badge>
            {info.windowed && (
              <Badge>
                windowed · {formatNumber(info.window_granularity_seconds)}s buckets ·{' '}
                {formatNumber(info.window_retention_seconds)}s retention
              </Badge>
            )}
            <Badge>store: {info.store_type}</Badge>
            {info.cache_type && <Badge>cache: {info.cache_type}</Badge>}
            {info.source_type && <Badge>source: {info.source_type}</Badge>}
          </div>
        </div>
        <StatusDot stats={stats} now={now} />
      </div>

      <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-4 sm:gap-6">
        <Stat label="events" value={formatNumber(stats?.events_processed)} />
        <Stat
          label="errors"
          value={formatNumber(stats?.errors)}
          tone={stats && stats.errors > 0 ? 'bad' : 'neutral'}
        />
        <Stat label="store p95" value={formatDuration(storeMerge?.p95_ms)} mono />
        <Stat
          label="cache p95"
          value={cacheMerge ? formatDuration(cacheMerge.p95_ms) : '—'}
          mono
        />
      </div>

      {stats?.last_error && (
        <div className="mt-3 text-xs text-bad font-mono truncate" title={stats.last_error}>
          last error: {stats.last_error}
        </div>
      )}
      {eps !== null && (
        <div className="mt-1 text-xs text-fg-muted tabular-nums">{eps} events/sec</div>
      )}
    </Link>
  )
}

/**
 * Tracks each pipeline's previous (events, timestamp) sample to estimate the
 * processing rate. Stored in module-level WeakMap-by-stats-object so the value
 * survives card re-renders without polluting React state.
 */
const lastSample = new Map<string, { events: number; at: number }>()
function useEpsEstimate(stats: PipelineStats | null, now: number): string | null {
  return useMemo(() => {
    if (!stats || stats.last_event_at === undefined) return null
    const cur = { events: Number(stats.events_processed), at: now }
    const prev = lastSample.get(stats.pipeline)
    lastSample.set(stats.pipeline, cur)
    if (!prev || prev.at === cur.at) return null
    const dt = (cur.at - prev.at) / 1000
    if (dt <= 0) return null
    const rate = (cur.events - prev.events) / dt
    if (rate <= 0) return null
    return rate >= 1 ? rate.toFixed(0) : rate.toFixed(2)
  }, [stats, now])
}

function StatusDot({ stats, now }: { stats: PipelineStats | null; now: number }) {
  if (!stats) {
    return <span className="text-xs text-fg-faint">no data</span>
  }
  // `now` is the parent's useNow tick (already pure in render); using it
  // instead of Date.now() keeps react-hooks/purity happy and tracks the
  // 2s polling cadence.
  const fresh = stats.last_event_at
    ? now - new Date(stats.last_event_at).getTime() < 30_000
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
        className={`mt-0.5 text-lg tabular-nums ${
          mono ? 'font-mono' : 'font-medium'
        } ${tone === 'bad' ? 'text-bad' : 'text-fg-strong'}`}
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
        page-view-counters example does this when you set{' '}
        <code className="font-mono text-fg-strong">MURMUR_ADMIN_ADDR</code>.
      </p>
    </div>
  )
}
