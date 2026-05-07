import { NavLink, Outlet } from 'react-router-dom'
import { clsx } from 'clsx'
import { useEffect, useState } from 'react'
import { api } from '../api'

export function Layout() {
  const [healthy, setHealthy] = useState<boolean | null>(null)
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const ok = await api.health()
        if (!cancelled) setHealthy(ok)
      } catch {
        if (!cancelled) setHealthy(false)
      }
    }
    tick()
    const id = setInterval(tick, 5000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  return (
    <div className="flex min-h-svh">
      <aside className="w-60 shrink-0 border-r border-border bg-surface flex flex-col">
        <div className="px-5 py-5 flex items-center gap-3 border-b border-border">
          <Logo />
          <div>
            <div className="font-semibold text-fg-strong tracking-tight">Murmur</div>
            <div className="text-xs text-fg-muted">aggregation pipelines</div>
          </div>
        </div>
        <nav className="flex flex-col px-3 py-4 gap-1 text-sm">
          <NavItem to="/">Pipelines</NavItem>
          <NavItem to="/query">Query console</NavItem>
          <NavItem to="/about">About</NavItem>
        </nav>
        <div className="mt-auto px-5 py-4 text-xs text-fg-muted border-t border-border">
          <div className="flex items-center gap-2">
            <span
              className={clsx(
                'inline-block w-2 h-2 rounded-full',
                healthy === null && 'bg-fg-faint',
                healthy === true && 'bg-good',
                healthy === false && 'bg-bad',
              )}
              aria-hidden
            />
            <span>
              {healthy === null
                ? 'connecting…'
                : healthy
                  ? 'admin online'
                  : 'admin offline'}
            </span>
          </div>
          <div className="mt-2 text-fg-faint">v0.1 · phase 2</div>
        </div>
      </aside>
      <main className="flex-1 overflow-auto">
        <Outlet />
      </main>
    </div>
  )
}

function NavItem({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      className={({ isActive }) =>
        clsx(
          'px-3 py-2 rounded-md transition-colors',
          isActive
            ? 'bg-accent-bg text-accent-strong'
            : 'text-fg-muted hover:text-fg-strong hover:bg-surface-2',
        )
      }
    >
      {children}
    </NavLink>
  )
}

function Logo() {
  // Stylized "M" — a tiny abstract murmuration.
  return (
    <svg width="32" height="32" viewBox="0 0 32 32" aria-hidden>
      <defs>
        <linearGradient id="m-grad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="var(--accent-strong)" />
          <stop offset="100%" stopColor="var(--accent)" />
        </linearGradient>
      </defs>
      <rect x="2" y="2" width="28" height="28" rx="7" fill="url(#m-grad)" />
      <path
        d="M9 22 V12 L16 19 L23 12 V22"
        stroke="white"
        strokeWidth="2.4"
        strokeLinecap="round"
        strokeLinejoin="round"
        fill="none"
      />
    </svg>
  )
}
