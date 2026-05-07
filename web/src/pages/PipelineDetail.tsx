import { Link, useParams } from 'react-router-dom'
import { useEffect, useMemo, useState } from 'react'
import ReactFlow, {
  Background,
  Controls,
  Position,
  type Edge,
  type Node,
} from 'reactflow'
import 'reactflow/dist/style.css'
import {
  api,
  decodeInt64LE,
  formatDuration,
  formatNumber,
  type PipelineInfo,
  type PipelineStats,
} from '../api'

export function PipelineDetail() {
  const { name = '' } = useParams()
  const [info, setInfo] = useState<PipelineInfo | null>(null)
  const [stats, setStats] = useState<PipelineStats | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [series, setSeries] = useState<{ t: number; events: number; errors: number }[]>([])

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const all = await api.listPipelines()
        const found = all.find((p) => p.name === name)
        if (!cancelled) setInfo(found ?? null)

        const s = await api.pipelineMetrics(name)
        if (!cancelled) {
          setStats(s)
          setError(null)
          setSeries((prev) => {
            const next = [
              ...prev,
              { t: Date.now(), events: Number(s.events_processed), errors: Number(s.errors) },
            ]
            // Keep the last 60 samples (~2 minutes at 2 s tick).
            return next.slice(-60)
          })
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
  }, [name])

  return (
    <div className="px-10 py-8 max-w-6xl">
      <header className="mb-6">
        <Link to="/" className="text-sm text-fg-muted hover:text-accent-strong">
          ← Pipelines
        </Link>
        <h1 className="mt-2 text-2xl font-semibold text-fg-strong tracking-tight font-mono">
          {name}
        </h1>
        {info && (
          <p className="text-fg-muted text-sm mt-1">
            {info.monoid_kind} monoid
            {info.windowed
              ? ` · daily-bucket window · ${formatNumber(info.window_retention_seconds)}s retention`
              : ' · all-time'}
          </p>
        )}
      </header>

      {error && (
        <div className="rounded-lg border border-bad bg-bad-bg text-fg-strong p-4 mb-4 text-sm">
          {error}
        </div>
      )}

      <section className="grid grid-cols-4 gap-4 mb-6">
        <Card label="events processed" value={formatNumber(stats?.events_processed)} />
        <Card label="errors" value={formatNumber(stats?.errors)} tone={stats && stats.errors > 0 ? 'bad' : 'neutral'} />
        <Card label="last event" value={stats?.last_event_at ? relTime(stats.last_event_at) : '—'} />
        <Card label="store p95" value={formatDuration(stats?.latencies?.store_merge?.p95_ms)} mono />
      </section>

      <section className="grid grid-cols-2 gap-6 mb-6">
        <Panel title="Pipeline DAG" subtitle="Source → Combine → Store flow.">
          <Dag info={info} />
        </Panel>
        <Panel title="Latency breakdown" subtitle="Recent operation percentiles.">
          <LatencyTable stats={stats} />
        </Panel>
      </section>

      <section>
        <Panel title="Throughput" subtitle="Cumulative events over the last ~2 minutes.">
          <Sparkline series={series} />
        </Panel>
      </section>
    </div>
  )
}

function Card({
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
    <div className="rounded-lg border border-border bg-surface p-5">
      <div className="text-[11px] uppercase tracking-wider text-fg-faint">{label}</div>
      <div
        className={`mt-1 text-2xl ${mono ? 'font-mono' : 'font-semibold'} ${
          tone === 'bad' ? 'text-bad' : 'text-fg-strong'
        }`}
      >
        {value}
      </div>
    </div>
  )
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle?: string
  children: React.ReactNode
}) {
  return (
    <div className="rounded-lg border border-border bg-surface">
      <div className="px-5 pt-4 pb-3 border-b border-border">
        <div className="text-fg-strong font-medium">{title}</div>
        {subtitle && <div className="text-xs text-fg-muted mt-1">{subtitle}</div>}
      </div>
      <div className="p-5">{children}</div>
    </div>
  )
}

function Dag({ info }: { info: PipelineInfo | null }) {
  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    const cardStyle = {
      background: 'var(--surface-2)',
      color: 'var(--fg-strong)',
      border: '1px solid var(--border-strong)',
      borderRadius: 8,
      padding: 8,
      width: 160,
      fontFamily: 'var(--mono)',
      fontSize: 12,
    }
    const accentCard = { ...cardStyle, borderColor: 'var(--accent)', background: 'var(--accent-bg)' }
    const sourceLabel = info?.source_type ?? 'source'
    const monoidLabel = info?.monoid_kind ?? 'aggregate'
    const storeLabel = info?.store_type ?? 'state'
    const cacheLabel = info?.cache_type
    return {
      nodes: [
        {
          id: 'source',
          position: { x: 0, y: 60 },
          data: { label: `Source · ${sourceLabel}` },
          style: cardStyle,
          sourcePosition: Position.Right,
        },
        {
          id: 'agg',
          position: { x: 200, y: 60 },
          data: { label: `Combine · ${monoidLabel}` },
          style: accentCard,
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        },
        {
          id: 'store',
          position: { x: 400, y: 0 },
          data: { label: `Store · ${storeLabel}` },
          style: cardStyle,
          targetPosition: Position.Left,
        },
        ...(cacheLabel
          ? [
              {
                id: 'cache',
                position: { x: 400, y: 120 },
                data: { label: `Cache · ${cacheLabel}` },
                style: cardStyle,
                targetPosition: Position.Left,
              } as Node,
            ]
          : []),
      ],
      edges: [
        { id: 'e1', source: 'source', target: 'agg', animated: true },
        { id: 'e2', source: 'agg', target: 'store' },
        ...(cacheLabel ? [{ id: 'e3', source: 'agg', target: 'cache' } as Edge] : []),
      ],
    }
  }, [info])

  return (
    <div style={{ height: 240 }} className="rounded-md bg-bg border border-border">
      <ReactFlow nodes={nodes} edges={edges} fitView proOptions={{ hideAttribution: true }}>
        <Background color="var(--border)" gap={16} />
        <Controls className="!bg-surface !border-border" />
      </ReactFlow>
    </div>
  )
}

function LatencyTable({ stats }: { stats: PipelineStats | null }) {
  const ops = stats ? Object.entries(stats.latencies) : []
  if (ops.length === 0) {
    return <div className="text-fg-muted text-sm">No latency samples yet.</div>
  }
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-left text-[11px] uppercase tracking-wider text-fg-faint">
          <th className="py-2 font-normal">op</th>
          <th className="py-2 font-normal text-right">samples</th>
          <th className="py-2 font-normal text-right">p50</th>
          <th className="py-2 font-normal text-right">p95</th>
          <th className="py-2 font-normal text-right">p99</th>
          <th className="py-2 font-normal text-right">max</th>
        </tr>
      </thead>
      <tbody>
        {ops.map(([op, lat]) => (
          <tr key={op} className="border-t border-border">
            <td className="py-2 font-mono text-fg-strong">{op}</td>
            <td className="py-2 text-right tabular-nums text-fg-muted">{formatNumber(lat.n)}</td>
            <td className="py-2 text-right tabular-nums">{formatDuration(lat.p50_ms)}</td>
            <td className="py-2 text-right tabular-nums">{formatDuration(lat.p95_ms)}</td>
            <td className="py-2 text-right tabular-nums">{formatDuration(lat.p99_ms)}</td>
            <td className="py-2 text-right tabular-nums text-fg-muted">{formatDuration(lat.max_ms)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function Sparkline({
  series,
}: {
  series: { t: number; events: number; errors: number }[]
}) {
  if (series.length < 2) {
    return <div className="text-fg-muted text-sm">Collecting samples…</div>
  }
  const w = 720
  const h = 120
  const pad = 12
  const xs = series.map((s) => s.t)
  const ys = series.map((s) => s.events)
  const xmin = Math.min(...xs)
  const xmax = Math.max(...xs)
  const ymin = Math.min(...ys)
  const ymax = Math.max(...ys)
  const xRange = xmax - xmin || 1
  const yRange = ymax - ymin || 1
  const path = series
    .map((s, i) => {
      const x = pad + ((s.t - xmin) / xRange) * (w - pad * 2)
      const y = h - pad - ((s.events - ymin) / yRange) * (h - pad * 2)
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join(' ')
  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-32">
      <path d={path} fill="none" stroke="var(--accent)" strokeWidth="2" />
    </svg>
  )
}

function relTime(iso: string): string {
  const d = new Date(iso)
  const ms = Date.now() - d.getTime()
  if (ms < 1000) return 'just now'
  if (ms < 60_000) return `${Math.round(ms / 1000)}s ago`
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)}m ago`
  return `${Math.round(ms / 3_600_000)}h ago`
}

// We keep decodeInt64LE accessible from the detail page in case Phase 3 adds a sparkline of
// the current state value over time.
export const _internal = { decodeInt64LE }
