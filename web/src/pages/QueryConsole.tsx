import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, formatError, type PipelineInfo, type StateValue } from '../api'
import { savedQueries, type SavedQuery } from '../lib/savedQueries'

type Mode = 'get' | 'window' | 'range'

export function QueryConsole() {
  useEffect(() => {
    document.title = 'Murmur · Query console'
  }, [])

  const [pipelines, setPipelines] = useState<PipelineInfo[]>([])
  const [search, setSearch] = useSearchParams()
  // Initial mount-time `now` for default ranges. useState with an
  // initializer keeps this stable across re-renders, so default
  // start/end don't drift on every render. react-hooks/purity flags
  // direct Date.now() calls in render.
  const [nowSec] = useState(() => Math.floor(Date.now() / 1000))

  const pipeline = search.get('pipeline') ?? ''
  const mode = (search.get('mode') as Mode) || 'get'
  const entity = search.get('entity') ?? ''
  const durationS = Number(search.get('duration_s') ?? 86400)
  const startUnix = Number(search.get('start') ?? nowSec - 86400 * 7)
  const endUnix = Number(search.get('end') ?? nowSec)

  const [resp, setResp] = useState<StateValue | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [saved, setSaved] = useState<SavedQuery[]>(() => savedQueries.list())

  useEffect(() => {
    let cancelled = false
    api.listPipelines().then((p) => {
      if (cancelled) return
      setPipelines(p)
      if (!pipeline && p.length > 0) {
        const next = new URLSearchParams(search)
        next.set('pipeline', p[0].name)
        setSearch(next, { replace: true })
      }
    })
    return () => {
      cancelled = true
    }
    // We deliberately depend on no values — this initialization runs once.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const selected = pipelines.find((p) => p.name === pipeline)

  const run = async () => {
    if (!pipeline || !entity) return
    setErr(null)
    setLoading(true)
    try {
      let result: StateValue
      if (mode === 'get') result = await api.getState(pipeline, entity)
      else if (mode === 'window') result = await api.getWindow(pipeline, entity, durationS)
      else result = await api.getRange(pipeline, entity, startUnix, endUnix)
      setResp(result)
    } catch (e) {
      setErr(formatError(e))
      setResp(null)
    } finally {
      setLoading(false)
    }
  }

  const setParam = (k: string, v: string | number) => {
    const next = new URLSearchParams(search)
    next.set(k, String(v))
    setSearch(next, { replace: false })
  }

  const saveCurrent = () => {
    if (!pipeline || !entity) return
    const suggested = `${pipeline} · ${mode} · ${entity}`
    const name = window.prompt('Save this query as:', suggested)
    if (!name) return
    setSaved(savedQueries.save(name, search.toString()))
  }

  const loadSaved = (q: SavedQuery) => {
    setSearch(new URLSearchParams(q.params), { replace: false })
  }

  const deleteSaved = (name: string) => {
    if (!window.confirm(`Delete saved query "${name}"?`)) return
    setSaved(savedQueries.delete(name))
  }

  return (
    <div className="px-6 sm:px-10 py-8 max-w-4xl">
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-fg-strong tracking-tight">Query console</h1>
        <p className="text-fg-muted text-sm mt-1">
          Issue ad-hoc reads against a pipeline's state. Same merge logic the gRPC service
          uses, served via the admin REST API. URL state is shareable.
        </p>
      </header>

      <div className="rounded-lg border border-border bg-surface p-6 grid gap-4">
        <Field label="Pipeline">
          <select
            className="ctl"
            value={pipeline}
            onChange={(e) => setParam('pipeline', e.target.value)}
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
                onClick={() => setParam('mode', m)}
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
            onChange={(e) => setParam('entity', e.target.value)}
            placeholder="e.g. page-A"
          />
        </Field>

        {mode === 'window' && (
          <Field label="Duration (seconds)">
            <input
              className="ctl"
              type="number"
              value={durationS}
              onChange={(e) => setParam('duration_s', e.target.value)}
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
                onChange={(e) => setParam('start', e.target.value)}
              />
            </Field>
            <Field label="End (unix seconds)">
              <input
                className="ctl"
                type="number"
                value={endUnix}
                onChange={(e) => setParam('end', e.target.value)}
              />
            </Field>
          </>
        )}

        <div className="flex gap-2">
          <button
            type="button"
            onClick={run}
            disabled={loading || !pipeline || !entity}
            className="px-4 py-2 rounded-md bg-accent text-white font-medium hover:bg-accent-strong transition-colors disabled:bg-surface-3 disabled:text-fg-muted disabled:cursor-not-allowed"
          >
            {loading ? 'querying…' : 'Query'}
          </button>
          <button
            type="button"
            onClick={saveCurrent}
            disabled={!pipeline || !entity}
            title="Persist this query under a name (kept in browser localStorage)"
            className="px-4 py-2 rounded-md border border-border text-fg-muted font-medium hover:bg-surface-2 hover:text-fg-strong transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Save…
          </button>
        </div>
      </div>

      {saved.length > 0 && (
        <SavedQueriesPanel saved={saved} onLoad={loadSaved} onDelete={deleteSaved} />
      )}

      {err && (
        <div role="alert" className="mt-4 rounded-lg border border-bad bg-bad-bg p-4 text-sm">
          {err}
        </div>
      )}

      {resp && <ResultPanel resp={resp} kind={selected?.monoid_kind} />}

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
        .ctl:focus { outline: 2px solid var(--accent); outline-offset: 0; border-color: var(--accent); }
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

function SavedQueriesPanel({
  saved,
  onLoad,
  onDelete,
}: {
  saved: SavedQuery[]
  onLoad: (q: SavedQuery) => void
  onDelete: (name: string) => void
}) {
  return (
    <div className="mt-4 rounded-lg border border-border bg-surface">
      <div className="px-5 pt-4 pb-3 border-b border-border">
        <div className="text-fg-strong font-medium">Saved queries</div>
        <div className="text-xs text-fg-muted mt-1">
          Stored in your browser via localStorage. Click a name to load; delete with ✕.
        </div>
      </div>
      <ul className="divide-y divide-border">
        {saved.map((q) => (
          <li key={q.name} className="flex items-center gap-3 px-5 py-3">
            <button
              type="button"
              onClick={() => onLoad(q)}
              className="flex-1 text-left hover:text-accent-strong transition-colors"
            >
              <div className="font-mono text-fg-strong text-sm">{q.name}</div>
              <div className="text-xs text-fg-faint truncate font-mono">?{q.params}</div>
            </button>
            <time
              dateTime={q.savedAt}
              className="text-xs text-fg-faint tabular-nums"
              title={q.savedAt}
            >
              {new Date(q.savedAt).toLocaleDateString()}
            </time>
            <button
              type="button"
              onClick={() => onDelete(q.name)}
              aria-label={`Delete saved query ${q.name}`}
              className="text-fg-muted hover:text-bad transition-colors px-2"
            >
              ✕
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}

function ResultPanel({ resp, kind }: { resp: StateValue; kind: string | undefined }) {
  if (!resp.present) {
    return (
      <div className="mt-4 rounded-lg border border-border bg-surface p-6">
        <div className="text-[11px] uppercase tracking-wider text-fg-faint">Result</div>
        <div className="mt-2 text-fg-muted">absent — no value at that key</div>
      </div>
    )
  }

  let primary: React.ReactNode
  let aux: React.ReactNode = null

  switch (kind) {
    case 'sum':
    case 'count':
    case 'min':
    case 'max': {
      const v = resp.decoded?.int64
      primary =
        v === undefined ? (
          <span className="text-fg-muted">decoded value missing</span>
        ) : (
          v.toLocaleString()
        )
      aux = <KindBadge kind={kind} encoding="int64 little-endian" />
      break
    }
    case 'hll': {
      if (resp.decoded?.hll) {
        primary = (
          <div>
            <div className="text-fg-strong">~{resp.decoded.hll.cardinality_estimate.toLocaleString()}</div>
            <div className="text-fg-faint text-sm mt-1 tabular-nums font-sans">
              estimated unique elements · {resp.decoded.hll.byte_len.toLocaleString()} byte sketch
              · ~1.6% standard error
            </div>
          </div>
        )
      } else {
        primary = <span className="text-fg-muted">decoded value missing</span>
      }
      aux = <KindBadge kind={kind} encoding="HLL++" />
      break
    }

    case 'topk': {
      if (resp.decoded?.topk) {
        primary = <TopKList topk={resp.decoded.topk} />
      } else {
        primary = <span className="text-fg-muted">decoded value missing</span>
      }
      aux = <KindBadge kind={kind} encoding="Misra-Gries" />
      break
    }

    case 'bloom': {
      if (resp.decoded?.bloom) {
        const b = resp.decoded.bloom
        primary = (
          <div>
            <div className="text-fg-strong">~{b.approx_size.toLocaleString()}</div>
            <div className="text-fg-faint text-sm mt-1 tabular-nums font-sans">
              estimated insertions · {b.capacity_bits.toLocaleString()} bits ·
              {' '}{b.hash_functions} hashes
            </div>
          </div>
        )
      } else {
        primary = <span className="text-fg-muted">decoded value missing</span>
      }
      aux = <KindBadge kind={kind} encoding="Bloom" />
      break
    }

    default:
      primary = <span className="font-mono">{resp.data ?? '(absent)'}</span>
  }

  return (
    <div className="mt-4 rounded-lg border border-border bg-surface p-6">
      <div className="flex items-center justify-between">
        <div className="text-[11px] uppercase tracking-wider text-fg-faint">Result</div>
        {aux}
      </div>
      <div className="mt-2 text-3xl font-mono text-fg-strong tabular-nums">{primary}</div>
      {resp.data && (
        <details className="mt-3">
          <summary className="text-xs text-fg-muted cursor-pointer hover:text-fg-strong">
            raw bytes
          </summary>
          <div className="mt-2 text-xs text-fg-muted font-mono break-all">{resp.data}</div>
        </details>
      )}
    </div>
  )
}

function KindBadge({ kind, encoding }: { kind: string; encoding: string }) {
  return (
    <span className="text-xs text-fg-muted font-mono">
      {kind} · {encoding}
    </span>
  )
}

/** Renders a Misra-Gries TopK as a ranked bar chart. The longest count is
 *  scaled to 100% width; other rows scale proportionally so visual rank
 *  follows numeric rank. Counts are tabular-nums so digits don't dance.
 */
function TopKList({ topk }: { topk: import('../api').DecodedTopK }) {
  const items = topk.items
  if (items.length === 0) {
    return <span className="text-fg-muted">empty sketch</span>
  }
  const max = items[0]?.count ?? 1
  return (
    <div className="space-y-2 font-sans text-base">
      <div className="text-fg-faint text-xs uppercase tracking-wider">
        Top {items.length} of K={topk.k}
      </div>
      {items.map((it, i) => {
        const pct = max > 0 ? Math.max(2, (it.count / max) * 100) : 0
        return (
          <div key={it.key} className="grid grid-cols-[1.5rem_1fr_auto] items-center gap-3">
            <div className="text-fg-faint tabular-nums text-sm">{i + 1}.</div>
            <div className="relative">
              <div
                className="absolute inset-0 rounded bg-accent-bg border border-accent-border"
                style={{ width: `${pct}%` }}
                aria-hidden
              />
              <div className="relative px-2 py-1 font-mono text-fg-strong text-sm truncate">
                {it.key}
              </div>
            </div>
            <div className="font-mono tabular-nums text-fg-strong text-sm">
              {it.count.toLocaleString()}
            </div>
          </div>
        )
      })}
    </div>
  )
}
