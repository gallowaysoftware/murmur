package state

import (
	"context"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

// Instrumented wraps a Store[V] (or Cache[V]) with metrics.Recorder
// hooks for every operation. The wrapped store sees identical traffic;
// the recorder gets per-op latency and error counts.
//
// Operation names follow the streaming runtime's convention:
//
//   - "<label>:store_get"          (Get)
//   - "<label>:store_get_many"     (GetMany)
//   - "<label>:store_merge_update" (MergeUpdate)
//
// where `label` is the supplied namespace (typically the pipeline name).
// Wrap the SAME label as the streaming/query layer uses so latency
// histograms aggregate cleanly.
//
// Usage:
//
//	rawStore := mddb.NewInt64SumStore(client, "page_views")
//	store := state.Instrumented[int64](rawStore, recorder, "page_views")
//	pipeline.NewPipeline[Event, int64](...).StoreIn(store)
//
// Cost: one time.Now() at op entry + one RecordLatency at op exit.
// Sub-microsecond when the recorder is a Noop; ~100ns + lock contention
// for the InMemory recorder. Negligible against any DDB / Valkey
// operation cost.
//
// For Cache[V], see InstrumentedCache below — same pattern, adds the
// Repopulate latency op.
type Instrumented[V any] struct {
	inner    Store[V]
	recorder metrics.Recorder
	label    string
}

// NewInstrumented wraps store with recorder-driven latency reporting
// under the given label. Defaults: when recorder is nil, returns the
// inner store unwrapped (zero overhead path).
func NewInstrumented[V any](inner Store[V], recorder metrics.Recorder, label string) Store[V] {
	if recorder == nil {
		return inner
	}
	return &Instrumented[V]{inner: inner, recorder: recorder, label: label}
}

// Get records latency under "<label>:store_get" and propagates the
// inner Store's result.
func (s *Instrumented[V]) Get(ctx context.Context, k Key) (V, bool, error) {
	t0 := time.Now()
	v, ok, err := s.inner.Get(ctx, k)
	s.recorder.RecordLatency(s.label, "store_get", time.Since(t0))
	if err != nil {
		s.recorder.RecordError(s.label, err)
	}
	return v, ok, err
}

// GetMany records latency under "<label>:store_get_many".
func (s *Instrumented[V]) GetMany(ctx context.Context, ks []Key) ([]V, []bool, error) {
	t0 := time.Now()
	v, ok, err := s.inner.GetMany(ctx, ks)
	s.recorder.RecordLatency(s.label, "store_get_many", time.Since(t0))
	if err != nil {
		s.recorder.RecordError(s.label, err)
	}
	return v, ok, err
}

// MergeUpdate records latency under "<label>:store_merge_update".
//
// Note: the streaming runtime ALSO records "store_merge" latency from
// its own clock, capturing the runtime's view of the operation
// (including any wrapping the runtime applies). The two op names are
// distinct so they aggregate to separate histograms — each tells a
// different story (store-side cost vs runtime-side wrapped cost).
func (s *Instrumented[V]) MergeUpdate(ctx context.Context, k Key, delta V, ttl time.Duration) error {
	t0 := time.Now()
	err := s.inner.MergeUpdate(ctx, k, delta, ttl)
	s.recorder.RecordLatency(s.label, "store_merge_update", time.Since(t0))
	if err != nil {
		s.recorder.RecordError(s.label, err)
	}
	return err
}

// Close passes through.
func (s *Instrumented[V]) Close() error { return s.inner.Close() }

// InstrumentedCache wraps a Cache[V] with recorder hooks. Same op name
// scheme as Instrumented[V] plus "<label>:cache_repopulate".
type InstrumentedCache[V any] struct {
	inner    Cache[V]
	recorder metrics.Recorder
	label    string
}

// NewInstrumentedCache wraps cache with recorder-driven latency
// reporting. Defaults: nil recorder → inner cache unwrapped.
func NewInstrumentedCache[V any](inner Cache[V], recorder metrics.Recorder, label string) Cache[V] {
	if recorder == nil {
		return inner
	}
	return &InstrumentedCache[V]{inner: inner, recorder: recorder, label: label}
}

// Get records latency under "<label>:cache_get".
func (c *InstrumentedCache[V]) Get(ctx context.Context, k Key) (V, bool, error) {
	t0 := time.Now()
	v, ok, err := c.inner.Get(ctx, k)
	c.recorder.RecordLatency(c.label, "cache_get", time.Since(t0))
	if err != nil {
		c.recorder.RecordError(c.label, err)
	}
	return v, ok, err
}

// GetMany records latency under "<label>:cache_get_many".
func (c *InstrumentedCache[V]) GetMany(ctx context.Context, ks []Key) ([]V, []bool, error) {
	t0 := time.Now()
	v, ok, err := c.inner.GetMany(ctx, ks)
	c.recorder.RecordLatency(c.label, "cache_get_many", time.Since(t0))
	if err != nil {
		c.recorder.RecordError(c.label, err)
	}
	return v, ok, err
}

// MergeUpdate records latency under "<label>:cache_merge_update".
func (c *InstrumentedCache[V]) MergeUpdate(ctx context.Context, k Key, delta V, ttl time.Duration) error {
	t0 := time.Now()
	err := c.inner.MergeUpdate(ctx, k, delta, ttl)
	c.recorder.RecordLatency(c.label, "cache_merge_update", time.Since(t0))
	if err != nil {
		c.recorder.RecordError(c.label, err)
	}
	return err
}

// Repopulate records latency under "<label>:cache_repopulate".
func (c *InstrumentedCache[V]) Repopulate(ctx context.Context, src Store[V], keys []Key) error {
	t0 := time.Now()
	err := c.inner.Repopulate(ctx, src, keys)
	c.recorder.RecordLatency(c.label, "cache_repopulate", time.Since(t0))
	if err != nil {
		c.recorder.RecordError(c.label, err)
	}
	return err
}

// Close passes through.
func (c *InstrumentedCache[V]) Close() error { return c.inner.Close() }
