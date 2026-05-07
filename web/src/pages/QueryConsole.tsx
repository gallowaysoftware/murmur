import { useEffect, useState } from 'react'
import { api, decodeInt64LE, type PipelineInfo } from '../api'

type Mode = 'get' | 'window' | 'range'

export function QueryConsole() {
  const [pipelines, setPipelines] = useState<PipelineInfo[]>([])
  const [pipeline, setPipeline] = useState('')
  const [mode, setMode] = useState<Mode>('get')
  const [entity, setEntity] = useState('')
  const [durationS, setDurationS] = useState(86400)
  const [startUnix, setStartUnix] = useState(() => Math.floor(Date.now() / 1000) - 86400 * 7)
  const [endUnix, setEndUnix] = useState(() => Math.floor(Date.now() / 1000))
  const [resp, setResp] = useState<{ value: string; raw: string } | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    api.listPipelines().then((p) => {
      setPipelines(p)
      if (!pipeline && p.length > 0) setPipeline(p[0].name)
    })
  }, [])

  const selected = pipelines.find((p) => p.name === pipeline)

  const run = async () => {
    if (!pipeline || !entity) return
    setErr(null)
    setLoading(true)
    try {
      let result
      if (mode === 'get') result = await api.getState(pipeline, entity)
      else if (mode === 'window') result = await api.getWindow(pipeline, entity, durationS)
      else result = await api.getRange(pipeline, entity, startUnix, endUnix)

      const formatted = formatValue(result.data, selected?.monoid_kind)
      setResp({ value: formatted, raw: result.data ?? '' })
    } catch (e) {
      setErr(String(e))
      setResp(null)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="px-10 py-8 max-w-4xl">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-fg-strong tracking-tight">Query console</h1>
        <p className="text-fg-muted text-sm mt-1">
          Issue ad-hoc reads against a pipeline's state. Same merge logic the gRPC service
          uses, served via the admin REST API.
        </p>
      </header>

      <div className="rounded-lg border border-border bg-surface p-6 grid gap-4">
        <Field label="Pipeline">
          <select
            className="ctl"
            value={pipeline}
            onChange={(e) => setPipeline(e.target.value)}
          >
            {pipelines.length === 0 && <option value="">(no pipelines registered)</option>}
            {pipelines.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name} · {p.monoid_kind}
              </option>
            ))}
          </select>
        </Field>

        <Field label="Mode">
          <div className="flex gap-2">
            {(['get', 'window', 'range'] as const).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setMode(m)}
                className={
                  'px-3 py-1.5 rounded-md text-sm border transition-colors ' +
                  (mode === m
                    ? 'bg-accent-bg border-accent-border text-accent-strong'
                    : 'border-border text-fg-muted hover:text-fg-strong hover:bg-surface-2')
                }
              >
                {m}
              </button>
            ))}
          </div>
        </Field>

        <Field label="Entity">
          <input
            className="ctl"
            value={entity}
            onChange={(e) => setEntity(e.target.value)}
            placeholder="e.g. page-A"
          />
        </Field>

        {mode === 'window' && (
          <Field label="Duration (seconds)">
            <input
              className="ctl"
              type="number"
              value={durationS}
              onChange={(e) => setDurationS(Number(e.target.value))}
            />
          </Field>
        )}

        {mode === 'range' && (
          <>
            <Field label="Start (unix seconds)">
              <input
                className="ctl"
                type="number"
                value={startUnix}
                onChange={(e) => setStartUnix(Number(e.target.value))}
              />
            </Field>
            <Field label="End (unix seconds)">
              <input
                className="ctl"
                type="number"
                value={endUnix}
                onChange={(e) => setEndUnix(Number(e.target.value))}
              />
            </Field>
          </>
        )}

        <div>
          <button
            type="button"
            onClick={run}
            disabled={loading || !pipeline || !entity}
            className="px-4 py-2 rounded-md bg-accent text-white font-medium hover:bg-accent-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'querying…' : 'Query'}
          </button>
        </div>
      </div>

      {err && (
        <div className="mt-4 rounded-lg border border-bad bg-bad-bg p-4 text-sm">{err}</div>
      )}

      {resp && (
        <div className="mt-4 rounded-lg border border-border bg-surface p-6">
          <div className="text-[11px] uppercase tracking-wider text-fg-faint">Result</div>
          <div className="mt-2 text-3xl font-mono text-fg-strong">{resp.value}</div>
          {resp.raw && (
            <div className="mt-3 text-xs text-fg-muted font-mono break-all">
              raw: {resp.raw}
            </div>
          )}
        </div>
      )}

      <style>{`
        .ctl {
          background: var(--surface-2);
          border: 1px solid var(--border);
          border-radius: 6px;
          color: var(--fg-strong);
          padding: 8px 10px;
          width: 100%;
          font: inherit;
        }
        .ctl:focus { outline: 2px solid var(--accent); border-color: var(--accent); }
      `}</style>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <div className="text-xs uppercase tracking-wider text-fg-faint mb-1">{label}</div>
      {children}
    </label>
  )
}

function formatValue(b64: string | undefined, kind: string | undefined): string {
  if (!b64) return '(absent)'
  switch (kind) {
    case 'sum':
    case 'count':
    case 'min':
    case 'max': {
      const n = decodeInt64LE(b64)
      return n === null ? '(absent)' : n.toLocaleString()
    }
    case 'hll':
      return `HLL sketch (${atob(b64).length} bytes)`
    case 'topk':
      return `TopK sketch (${atob(b64).length} bytes)`
    case 'bloom':
      return `Bloom filter (${atob(b64).length} bytes)`
    default:
      return `${atob(b64).length} bytes`
  }
}
