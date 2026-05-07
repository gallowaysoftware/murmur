/**
 * localStorage-backed saved queries for the Query Console. Inspired by
 * Temporal's "Saved Views": persist a (pipeline, mode, params) combo under a
 * user-supplied name so you can come back to "last 7 days of page-A" in one
 * click rather than retyping it.
 *
 * Storage is local to the browser — by design. Saved queries belong to the
 * person at the keyboard, not to the deployment. Cross-user sharing happens
 * via the URL (already shareable per PR 4).
 */

const KEY = 'murmur:saved-queries:v1'

export type SavedQuery = {
  /** User-supplied name; uniqueness enforced at save time. */
  name: string
  /** ISO-8601 timestamp of last save. */
  savedAt: string
  /** The full query: stored as a URLSearchParams string so we can round-trip
   *  through the same code path that powers shareable URLs. */
  params: string
}

function read(): SavedQuery[] {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as unknown
    if (!Array.isArray(parsed)) return []
    return parsed.filter(
      (q): q is SavedQuery =>
        !!q &&
        typeof q === 'object' &&
        typeof (q as SavedQuery).name === 'string' &&
        typeof (q as SavedQuery).params === 'string',
    )
  } catch {
    return []
  }
}

function write(qs: SavedQuery[]): void {
  try {
    localStorage.setItem(KEY, JSON.stringify(qs))
  } catch {
    // Quota / private-mode failures are non-fatal — the in-memory list still works.
  }
}

export const savedQueries = {
  list(): SavedQuery[] {
    return read().slice().sort((a, b) => a.name.localeCompare(b.name))
  },

  /** Insert or replace by name. Returns the new list. */
  save(name: string, params: string): SavedQuery[] {
    const trimmed = name.trim()
    if (!trimmed) return read()
    const existing = read().filter((q) => q.name !== trimmed)
    const next: SavedQuery = {
      name: trimmed,
      savedAt: new Date().toISOString(),
      params,
    }
    const out = [...existing, next]
    write(out)
    return out
  },

  delete(name: string): SavedQuery[] {
    const out = read().filter((q) => q.name !== name)
    write(out)
    return out
  },

  /** Find by name; undefined if missing. */
  get(name: string): SavedQuery | undefined {
    return read().find((q) => q.name === name)
  },
}
