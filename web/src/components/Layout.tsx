import { NavLink, Outlet } from 'react-router-dom'
import { clsx } from 'clsx'
import { useState } from 'react'
import { api } from '../api'
import { useLivePolling } from '../hooks/useLivePolling'
import { ErrorBoundary } from './ErrorBoundary'

export function Layout() {
  const { data: healthy } = useLivePolling<boolean>(
    (signal) => api.health(signal),
    5000,
  )
  const [navOpen, setNavOpen] = useState(false)

  return (
    <div className="flex min-h-svh">
      {/* Sidebar — overlay drawer at narrow widths, fixed at md+. */}
      <aside
        className={clsx(
          'fixed md:static z-30 inset-y-0 left-0 w-60 shrink-0 border-r border-border bg-surface flex flex-col transition-transform',
          navOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0',
        )}
      >
        <div className="px-5 py-5 flex items-center gap-3 border-b border-border">
          <Logo />
          <div>
            <div className="font-semibold text-fg-strong tracking-tight">Murmur</div>
            <div className="text-xs text-fg-muted">aggregation pipelines</div>
          </div>
        </div>
        <nav className="flex flex-col px-3 py-4 gap-1 text-sm">
          <NavItem to="/" onClick={() => setNavOpen(false)}>
            Pipelines
          </NavItem>
          <NavItem to="/query" onClick={() => setNavOpen(false)}>
            Query console
          </NavItem>
          <NavItem to="/about" onClick={() => setNavOpen(false)}>
            About
          </NavItem>
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
            <span aria-live="polite">
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

      {navOpen && (
        <button
          type="button"
          aria-label="Close navigation"
          className="fixed inset-0 bg-black/50 z-20 md:hidden"
          onClick={() => setNavOpen(false)}
        />
      )}

      <main className="flex-1 overflow-auto">
        {/* Mobile top bar with hamburger. */}
        <div className="md:hidden border-b border-border bg-surface px-4 py-3 flex items-center gap-3">
          <button
            type="button"
            aria-label="Open navigation"
            onClick={() => setNavOpen(true)}
            className="p-2 -ml-2 text-fg hover:text-fg-strong"
          >
            <svg
              width="20"
              height="20"
              viewBox="0 0 20 20"
              aria-hidden
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
            >
              <path d="M3 6h14M3 10h14M3 14h14" />
            </svg>
          </button>
          <span className="font-semibold text-fg-strong">Murmur</span>
        </div>

        <ErrorBoundary>
          <Outlet />
        </ErrorBoundary>
      </main>
    </div>
  )
}

function NavItem({
  to,
  children,
  onClick,
}: {
  to: string
  children: React.ReactNode
  onClick?: () => void
}) {
  return (
    <NavLink
      to={to}
      end={to === '/'}
      onClick={onClick}
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
