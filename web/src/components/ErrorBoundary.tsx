import { Component, type ErrorInfo, type ReactNode } from 'react'

type Props = { children: ReactNode }
type State = { error: Error | null }

/**
 * Top-level error boundary. Renders a recoverable panel matching the dashboard's
 * dark theme rather than letting a thrown render error blank the page. Use one of
 * these around `<Outlet />` (or any sub-tree that pulls live data) so a transient
 * server-side glitch doesn't take down navigation.
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // eslint-disable-next-line no-console
    console.error('ErrorBoundary caught:', error, info)
  }

  reset = () => this.setState({ error: null })

  render() {
    if (!this.state.error) return this.props.children
    return (
      <div className="px-10 py-12 max-w-3xl">
        <div
          role="alert"
          className="rounded-lg border border-bad bg-bad-bg p-6"
        >
          <div className="text-fg-strong font-medium text-lg">
            Something went wrong rendering this view.
          </div>
          <p className="text-fg-muted text-sm mt-2">
            The dashboard caught an unhandled error. You can retry, or refresh the page if
            this keeps happening.
          </p>
          <pre className="mt-4 max-h-64 overflow-auto bg-surface-2 border border-border rounded-md p-3 text-xs font-mono text-fg-muted whitespace-pre-wrap">
            {this.state.error.message}
            {'\n\n'}
            {this.state.error.stack ?? ''}
          </pre>
          <button
            type="button"
            onClick={this.reset}
            className="mt-4 px-4 py-2 rounded-md bg-accent text-white font-medium hover:bg-accent-strong transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    )
  }
}
