package pipeline

import (
	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
)

// WithCoalesce enables in-memory delta coalescing across a source batch. For additive
// monoids (KindSum, KindCount, and any KindCustom monoid that opts in via
// monoid.Additive — including compose.DecayedSum), the runtime accumulates
// per-key deltas in memory and emits a single MergeUpdate per unique key when the
// batch closes. This collapses N events that share a key into 1 wire call against
// DynamoDB — a 1–2 orders-of-magnitude throughput win on hot rows.
//
// The default tuning (DefaultCoalesceConfig) is sensible for most batch-shaped
// sources:
//
//   - MaxKeys: 10_000 distinct (entity, bucket) pairs before an inline flush.
//   - FlushTick: 1 s wall-clock interval between forced flushes — protects
//     long-running bootstrap scans from holding deltas indefinitely.
//
// Supply CoalesceOption arguments (WithCoalesceMaxKeys, WithCoalesceFlushTick from
// pkg/exec/processor) to override per pipeline. For non-additive monoids
// (Min/Max/Set/HLL/TopK/Bloom/...) the runtime transparently falls through to
// per-event MergeMany — calling WithCoalesce on such a pipeline is a no-op but
// safe.
//
// At-least-once semantics are preserved: Dedup is consulted once per event before
// any delta enters the pending map, and ack/checkpoint happens only after the
// batch's final flush succeeds. See pkg/exec/processor/coalesce.go for the
// underlying mechanics.
func (p *Pipeline[T, V]) WithCoalesce(opts ...processor.CoalesceOption) *Pipeline[T, V] {
	cfg := processor.DefaultCoalesceConfig()
	for _, o := range opts {
		o(&cfg)
	}
	cfg.Enabled = true
	p.coalesce = cfg
	return p
}

// Coalesce returns the configured CoalesceConfig. Returns the zero CoalesceConfig
// (Enabled=false) when the user has not called WithCoalesce.
//
// Runtimes consult this to decide whether to use processor.NewCoalescer or fall
// back to per-event processor.MergeMany.
func (p *Pipeline[T, V]) Coalesce() processor.CoalesceConfig {
	return p.coalesce
}
