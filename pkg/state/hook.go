package state

import (
	"context"
	"log/slog"
	"time"
)

// OnMergeFunc is the post-merge hook signature. It is invoked after a
// successful MergeUpdate on the wrapped Store with the same key and
// delta that were applied. Hook implementations should be fast and
// non-blocking — they run on the streaming runtime's hot path.
//
// Typical use cases:
//
//   - Emit a downstream "merged" event to Kafka/SQS so consumers can react
//     without setting up a DDB Streams → Lambda chain.
//   - Update a secondary index or in-memory cache that mirrors merge
//     activity.
//   - Drive change-data-capture style projections.
type OnMergeFunc[V any] func(ctx context.Context, key Key, delta V) error

// OnMergeOption configures a WithOnMerge wrapper.
type OnMergeOption func(*onMergeConfig)

type onMergeConfig struct {
	propagateErrors bool
	logger          *slog.Logger
}

// WithHookErrorPropagation controls whether a hook error is returned from
// MergeUpdate or merely logged. Default: false (log only). When true, a
// non-nil error from the hook is returned to the caller AFTER the inner
// MergeUpdate has already succeeded — the merge itself is NOT rolled back.
func WithHookErrorPropagation(propagate bool) OnMergeOption {
	return func(c *onMergeConfig) {
		c.propagateErrors = propagate
	}
}

// WithHookLogger overrides the slog.Logger used for hook-error log lines.
// Defaults to slog.Default().
func WithHookLogger(logger *slog.Logger) OnMergeOption {
	return func(c *onMergeConfig) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithOnMerge wraps inner with a post-write hook. fn is invoked after every
// successful MergeUpdate with the same key and delta that were applied. The
// hook is NOT invoked when MergeUpdate returns a non-nil error.
//
// By default, hook errors are logged via slog and swallowed — the wrapper's
// MergeUpdate returns nil to its caller as long as the inner merge
// succeeded. Pass WithHookErrorPropagation(true) to surface hook errors to
// the caller instead. Either way, the inner merge is NOT rolled back: by
// the time the hook runs, the merge has already been durably written.
//
// Nil fn is safe: WithOnMerge returns inner unwrapped (zero overhead).
//
// All other Store methods (Get, GetMany, Close) pass through to inner
// unchanged.
//
// Typical use case: count-core wants to emit a
// BotInteractionCountIntervalBackendEvent after each merge without setting
// up a DDB Streams → Lambda chain. The hook is fire-and-forget from the
// merge's perspective; durability of the merge itself is the inner Store's
// responsibility (DDB UpdateItem is atomic).
func WithOnMerge[V any](inner Store[V], fn OnMergeFunc[V], opts ...OnMergeOption) Store[V] {
	if fn == nil {
		return inner
	}
	cfg := onMergeConfig{
		propagateErrors: false,
		logger:          slog.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &onMergeStore[V]{
		inner: inner,
		fn:    fn,
		cfg:   cfg,
	}
}

type onMergeStore[V any] struct {
	inner Store[V]
	fn    OnMergeFunc[V]
	cfg   onMergeConfig
}

// Get passes through to the inner store.
func (s *onMergeStore[V]) Get(ctx context.Context, k Key) (V, bool, error) {
	return s.inner.Get(ctx, k)
}

// GetMany passes through to the inner store.
func (s *onMergeStore[V]) GetMany(ctx context.Context, ks []Key) ([]V, []bool, error) {
	return s.inner.GetMany(ctx, ks)
}

// MergeUpdate delegates to the inner store, then invokes the hook on
// success. Hook errors are logged by default and propagated only when
// WithHookErrorPropagation(true) was set.
func (s *onMergeStore[V]) MergeUpdate(ctx context.Context, k Key, delta V, ttl time.Duration) error {
	if err := s.inner.MergeUpdate(ctx, k, delta, ttl); err != nil {
		return err
	}
	if err := s.fn(ctx, k, delta); err != nil {
		if s.cfg.propagateErrors {
			return err
		}
		s.cfg.logger.LogAttrs(ctx, slog.LevelWarn, "state.WithOnMerge: post-write hook failed",
			slog.String("entity", k.Entity),
			slog.Int64("bucket", k.Bucket),
			slog.String("error", err.Error()),
		)
	}
	return nil
}

// Close passes through to the inner store.
func (s *onMergeStore[V]) Close() error { return s.inner.Close() }
