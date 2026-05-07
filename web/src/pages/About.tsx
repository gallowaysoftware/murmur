export function About() {
  return (
    <div className="px-10 py-8 max-w-3xl">
      <h1 className="text-2xl font-semibold text-fg-strong tracking-tight">About Murmur</h1>
      <p className="text-fg-muted mt-3">
        Lambda-architecture-aware streaming aggregation framework for Go. One pipeline DSL,
        three execution modes (live stream, snapshot bootstrap, archive replay), monoid-typed
        state in DynamoDB with optional Valkey acceleration, gRPC query layer.
      </p>
      <p className="text-fg-muted mt-3">
        This UI is the read-side surface — registered pipelines report metrics through{' '}
        <code className="font-mono text-fg-strong">pkg/metrics</code>, and the admin REST
        server in <code className="font-mono text-fg-strong">pkg/admin</code> exposes them
        here. Source:{' '}
        <a
          className="text-accent hover:text-accent-strong"
          href="https://github.com/gallowaysoftware/murmur"
        >
          github.com/gallowaysoftware/murmur
        </a>
        .
      </p>
    </div>
  )
}
