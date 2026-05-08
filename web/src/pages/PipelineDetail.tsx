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
  formatDuration,
  formatError,
  formatNumber,
  type PipelineInfo,
  type PipelineStats,
} from '../api'
import { useLivePolling, useNow } from '../hooks/useLivePolling'

type DetailData = { info: PipelineInfo | null; stats: PipelineStats }
type Sample = { t: number; events: number }

export function PipelineDetail() {
  const { name = '' } = useParams()
  useEffect(() => {
    document.title = `Murmur · ${name}`
  }, [name])

  const { data, error } = useLivePolling<DetailData>(async (signal) => {
    const all = await api.listPipelines(signal)
    const info = all.find((p) => p.name === name) ?? null
    const stats = await api.pipelineMetrics(name, signal)
    return { info, stats }
  }, 2000)

  // Sliding-window of recent samples for the sparkline. We need to
  // synthesize a *history* (current series ⊕ new data) from external
  // polling — useLivePolling is the external system. The throttle
  // (≥500ms gap) returns prev unchanged so cascading renders are bounded
  // to one per polling tick.
  const [series, setSeries] = useState<Sample[]>([])
  useEffect(() => {
    if (!data) return
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setSeries((prev) => {
      const last = prev.at(-1)
      const next: Sample = { t: Date.now(), events: Number(data.stats.events_processed) }
      if (!last || next.t - last.t > 500) {
        return [...prev, next].slice(-60)
      }
      return prev
    })
  }, [data])

  return (
    <div className="px-6 sm:px-10 py-8 max-w-6xl">
      <header className="mb-6">
        <Link to="/" className="text-sm text-fg-muted hover:text-accent-strong">
          ← Pipelines
        </Link>
        <h1 className="mt-2 text-2xl font-semibold text-fg-strong tracking-tight font-mono">
          {name}
        </h1>
        {data?.info && (
          <p className="text-fg-muted text-sm mt-1">
            {data.info.monoid_kind} monoid
            {data.info.windowed
              ? ` · daily-bucket window · ${formatNumber(
                  data.info.window_retention_seconds,
                )}s retention`
              : ' · all-time'}
          </p>
        )}
      </header>

      {error && (
        <div
          role="alert"
          className="rounded-lg border border-bad bg-bad-bg text-fg-strong p-4 mb-4 text-sm"
        >
          {formatError(error)}
        </div>
      )}

      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <Card label="events processed" value={formatNumber(data?.stats.events_processed)} />
        <Card
          label="errors"
          value={formatNumber(data?.stats.errors)}
          tone={data?.stats && data.stats.errors > 0 ? 'bad' : 'neutral'}
        />
        <Card label="last event" value={data?.stats.last_event_at ? <RelTime iso={data.stats.last_event_at} /> : '—'} />
        <Card label="store p95" value={formatDuration(data?.stats.latencies?.store_merge?.p95_ms)} mono />
      </section>

      <section className="grid grid-cols-1 xl:grid-cols-2 gap-6 mb-6">
        <Panel title="Pipeline DAG" subtitle="Source → Combine → Store flow.">
          <Dag info={data?.info ?? null} hasErrors={(data?.stats.errors ?? 0) > 0} />
        </Panel>
        <Panel title="Latency breakdown" subtitle="Recent operation percentiles.">
          <LatencyTable stats={data?.stats ?? null} />
        </Panel>
      </section>

      <section>
        <Panel
          title="Throughput"
          subtitle="Events per second over the last ~2 minutes (rate of change)."
        >
          <RateSparkline series={series} />
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
  value: React.ReactNode
  mono?: boolean
  tone?: 'neutral' | 'bad'
}) {
  return (
    <div className="rounded-lg border border-border bg-surface p-5">
      <div className="text-[11px] uppercase tracking-wider text-fg-faint">{label}</div>
      <div
        className={`mt-1 text-2xl tabular-nums ${
          mono ? 'font-mono' : 'font-semibold'
        } ${tone === 'bad' ? 'text-bad' : 'text-fg-strong'}`}
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

function Dag({ info, hasErrors }: { info: PipelineInfo | null; hasErrors: boolean }) {
  const { nodes, edges } = useMemo<{ nodes: Node[]; edges: Edge[] }>(() => {
    const cardStyle: React.CSSProperties = {
      background: 'var(--surface-2)',
      color: 'var(--fg-strong)',
      border: '1px solid var(--border-strong)',
      borderRadius: 8,
      padding: 8,
      width: 160,
      fontFamily: 'var(--mono)',
      fontSize: 12,
    }
    const accentCard: React.CSSProperties = {
      ...cardStyle,
      borderColor: hasErrors ? 'var(--bad)' : 'var(--accent)',
      background: hasErrors ? 'var(--bad-bg)' : 'var(--accent-bg)',
    }
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
  }, [info, hasErrors])

  return (
    <div style={{ height: 240 }} className="rounded-md bg-bg border border-border dag-host">
      <ReactFlow nodes={nodes} edges={edges} fitView proOptions={{ hideAttribution: true }}>
        <Background color="var(--border)" gap={16} />
        <Controls />
      </ReactFlow>
      {/* React Flow's controls are styled with hardcoded greys; override here so the
          buttons match the dashboard's dark theme. */}
      <style>{`
        .dag-host .react-flow__controls { box-shadow: none; }
        .dag-host .react-flow__controls-button {
          background: var(--surface-2);
          border: 1px solid var(--border);
          border-bottom: 1px solid var(--border);
          color: var(--fg);
        }
        .dag-host .react-flow__controls-button:hover {
          background: var(--surface-3);
        }
        .dag-host .react-flow__controls-button svg { fill: currentColor; }
      `}</style>
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

function RateSparkline({ series }: { series: Sample[] }) {
  if (series.length < 3) {
    return <div className="text-fg-muted text-sm">Collecting samples…</div>
  }
  // First-difference: rate = Δevents / Δt.
  const rates: { t: number; rate: number }[] = []
  for (let i = 1; i < series.length; i++) {
    const dt = (series[i].t - series[i - 1].t) / 1000
    if (dt <= 0) continue
    rates.push({ t: series[i].t, rate: (series[i].events - series[i - 1].events) / dt })
  }
  if (rates.length === 0) {
    return <div className="text-fg-muted text-sm">Collecting samples…</div>
  }
  const w = 720
  const h = 120
  const pad = 16
  const maxRate = Math.max(1, ...rates.map((r) => r.rate))
  const tmin = rates[0].t
  const tmax = rates[rates.length - 1].t
  const xRange = Math.max(1, tmax - tmin)
  const path = rates
    .map((r, i) => {
      const x = pad + ((r.t - tmin) / xRange) * (w - pad * 2)
      const y = h - pad - (r.rate / maxRate) * (h - pad * 2)
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join(' ')
  // y-axis tick at maxRate.
  const last = rates[rates.length - 1]
  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-32" role="img" aria-label="Throughput chart">
        <line
          x1={pad}
          x2={w - pad}
          y1={h - pad}
          y2={h - pad}
          stroke="var(--border)"
          strokeWidth="1"
        />
        <path d={path} fill="none" stroke="var(--accent)" strokeWidth="2" />
        <text x={w - pad} y={pad} textAnchor="end" fill="var(--fg-muted)" fontSize="11" fontFamily="var(--mono)">
          {maxRate.toFixed(0)} ev/s
        </text>
      </svg>
      <div className="text-xs text-fg-muted tabular-nums mt-1">
        latest: {last.rate.toFixed(2)} events/sec
      </div>
    </div>
  )
}

function RelTime({ iso }: { iso: string }) {
  // Re-render every second so the relative time stays fresh between polling ticks.
  const now = useNow(1000)
  const ms = now - new Date(iso).getTime()
  if (ms < 1000) return <>just now</>
  if (ms < 60_000) return <>{Math.round(ms / 1000)}s ago</>
  if (ms < 3_600_000) return <>{Math.round(ms / 60_000)}m ago</>
  return <>{Math.round(ms / 3_600_000)}h ago</>
}

