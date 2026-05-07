import { useEffect, useRef, useState } from 'react'

/**
 * Drive a periodic async fetch with three production-grade behaviors a `setInterval`
 * loop doesn't give you for free:
 *
 *   1. Cancellation. Each tick gets a fresh AbortSignal; an in-flight request is
 *      aborted when the next tick fires or the component unmounts.
 *   2. Backoff. Consecutive failures double the interval (capped at 30s) so the
 *      app doesn't hammer a struggling backend.
 *   3. Visibility-awareness. When the tab is hidden, polling pauses; when it
 *      becomes visible again, an immediate tick runs.
 *
 * `fetcher` should accept the signal and return a fresh value. Throws are caught
 * and exposed via `error`; `data` retains the last successful value during failures.
 */
export function useLivePolling<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  intervalMs: number = 2000,
): { data: T | null; error: Error | null; loading: boolean } {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(true)
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher

  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | null = null
    let aborter: AbortController | null = null
    let cancelled = false
    let consecutiveFailures = 0

    const tick = async () => {
      if (cancelled) return
      if (document.hidden) {
        // Re-schedule a quick re-check; the visibility listener will wake us.
        timer = setTimeout(tick, intervalMs)
        return
      }
      aborter?.abort()
      aborter = new AbortController()
      const signal = aborter.signal
      try {
        const v = await fetcherRef.current(signal)
        if (cancelled || signal.aborted) return
        setData(v)
        setError(null)
        setLoading(false)
        consecutiveFailures = 0
        timer = setTimeout(tick, intervalMs)
      } catch (e) {
        if (cancelled || signal.aborted) return
        // AbortError is expected on rapid re-render — don't surface it.
        if ((e as { name?: string })?.name === 'AbortError') {
          return
        }
        setError(e instanceof Error ? e : new Error(String(e)))
        setLoading(false)
        consecutiveFailures += 1
        // Exponential backoff capped at 30s.
        const backoff = Math.min(intervalMs * 2 ** consecutiveFailures, 30_000)
        timer = setTimeout(tick, backoff)
      }
    }

    const onVisibility = () => {
      if (!document.hidden) {
        if (timer) clearTimeout(timer)
        tick()
      }
    }
    document.addEventListener('visibilitychange', onVisibility)
    tick()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
      aborter?.abort()
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [intervalMs])

  return { data, error, loading }
}

/**
 * Wall-clock state that ticks every second, used to keep `relTime`-style displays
 * fresh without coupling them to data polling.
 */
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}
