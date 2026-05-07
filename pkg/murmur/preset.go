// Package murmur is a thin facade over pkg/pipeline for the most common pipeline
// shapes. The verbose lower-level builder (pkg/pipeline) is still available for
// pipelines that don't fit a preset; the facades here trade flexibility for
// dramatically less boilerplate on the 90% case.
//
// Presets:
//
//	Counter[T]        — Sum monoid; each event contributes 1
//	UniqueCount[T]    — HLL monoid; one element per event
//	TopN[T]           — TopK monoid; one (key, 1) per event
//
// Each preset builds a pkg/pipeline.Pipeline you can then hand to streaming.Run,
// bootstrap.Run, or replay.Run — same lifecycle as a hand-built pipeline.
package murmur

import (
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Counter is a Sum-monoid counter pipeline preset. Each event contributes 1; the
// pipeline's value type is int64.
func Counter[T any](name string) *CounterBuilder[T] {
	return &CounterBuilder[T]{name: name}
}

// CounterBuilder builds a Counter pipeline. Required: From, KeyBy, StoreIn.
// Optional: Daily/Hourly windowing, Cache.
type CounterBuilder[T any] struct {
	name   string
	keyFn  func(T) string
	src    source.Source[T]
	store  state.Store[int64]
	cache  state.Cache[int64]
	window *windowed.Config
}

// From sets the live event source. Required for streaming.Run; can be omitted
// when the same builder is used to construct a pipeline that will only run in
// bootstrap or replay mode.
func (b *CounterBuilder[T]) From(s source.Source[T]) *CounterBuilder[T] {
	b.src = s
	return b
}

// KeyBy sets the function that derives the aggregation key from each event.
// The returned string is the entity used in the state store and any queries.
// Required.
func (b *CounterBuilder[T]) KeyBy(fn func(T) string) *CounterBuilder[T] {
	b.keyFn = fn
	return b
}

// StoreIn sets the state store the pipeline writes through. Required.
// Typically state.Store[int64] backed by [pkg/state/dynamodb.Int64SumStore]
// for production, or a fakeStore for tests.
func (b *CounterBuilder[T]) StoreIn(s state.Store[int64]) *CounterBuilder[T] {
	b.store = s
	return b
}

// Cache sets the optional read accelerator (typically a Valkey-backed cache).
// Writes are mirrored to the cache after the primary store has accepted them;
// reads can short-circuit through the cache. Cache loss never affects
// correctness — see pkg/state/valkey.
func (b *CounterBuilder[T]) Cache(c state.Cache[int64]) *CounterBuilder[T] {
	b.cache = c
	return b
}

// Daily configures daily tumbling buckets with the given retention.
func (b *CounterBuilder[T]) Daily(retention time.Duration) *CounterBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

// Hourly configures hourly tumbling buckets with the given retention.
func (b *CounterBuilder[T]) Hourly(retention time.Duration) *CounterBuilder[T] {
	w := windowed.Hourly(retention)
	b.window = &w
	return b
}

// Build returns the assembled Pipeline. The returned pipeline is ready for
// streaming.Run / bootstrap.Run / replay.Run; Build itself does not execute it.
func (b *CounterBuilder[T]) Build() *pipeline.Pipeline[T, int64] {
	p := pipeline.NewPipeline[T, int64](b.name).
		Key(b.keyFn).
		Value(func(T) int64 { return 1 }).
		StoreIn(b.store)
	if b.window != nil {
		p = p.Aggregate(core.Sum[int64](), *b.window)
	} else {
		p = p.Aggregate(core.Sum[int64]())
	}
	if b.src != nil {
		p = p.From(b.src)
	}
	if b.cache != nil {
		p = p.Cache(b.cache)
	}
	return p
}

// UniqueCount is an HLL-monoid unique-cardinality pipeline preset. Each event
// contributes one element to the sketch — supplied by the elementFn argument
// (e.g., return e.UserID for unique-visitors-per-page).
func UniqueCount[T any](name string, elementFn func(T) []byte) *UniqueCountBuilder[T] {
	return &UniqueCountBuilder[T]{name: name, elementFn: elementFn}
}

// UniqueCountBuilder builds a UniqueCount (HLL) pipeline. Required: KeyBy,
// StoreIn. Optional: From, Daily/Hourly windowing.
type UniqueCountBuilder[T any] struct {
	name      string
	keyFn     func(T) string
	elementFn func(T) []byte
	src       source.Source[T]
	store     state.Store[[]byte]
	window    *windowed.Config
}

// From sets the live event source. Same semantics as CounterBuilder.From.
func (b *UniqueCountBuilder[T]) From(s source.Source[T]) *UniqueCountBuilder[T] {
	b.src = s
	return b
}

// KeyBy sets the function that derives the aggregation key from each event.
func (b *UniqueCountBuilder[T]) KeyBy(fn func(T) string) *UniqueCountBuilder[T] {
	b.keyFn = fn
	return b
}

// StoreIn sets the state store. HLL sketch state is stored as []byte; pair
// with [pkg/state/dynamodb.BytesStore] for production or a synthetic store
// for tests.
func (b *UniqueCountBuilder[T]) StoreIn(s state.Store[[]byte]) *UniqueCountBuilder[T] {
	b.store = s
	return b
}

// Daily configures daily tumbling buckets with the given retention.
func (b *UniqueCountBuilder[T]) Daily(retention time.Duration) *UniqueCountBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

// Hourly configures hourly tumbling buckets with the given retention.
func (b *UniqueCountBuilder[T]) Hourly(retention time.Duration) *UniqueCountBuilder[T] {
	w := windowed.Hourly(retention)
	b.window = &w
	return b
}

// Build assembles a *pipeline.Pipeline ready for streaming.Run / bootstrap.Run /
// replay.Run. The HLL value extractor is wired automatically: each event's
// elementFn output is lifted into a one-element sketch via hll.Single.
func (b *UniqueCountBuilder[T]) Build() *pipeline.Pipeline[T, []byte] {
	p := pipeline.NewPipeline[T, []byte](b.name).
		Key(b.keyFn).
		Value(func(t T) []byte { return hll.Single(b.elementFn(t)) }).
		StoreIn(b.store)
	if b.window != nil {
		p = p.Aggregate(hll.HLL(), *b.window)
	} else {
		p = p.Aggregate(hll.HLL())
	}
	if b.src != nil {
		p = p.From(b.src)
	}
	return p
}

// TopN is a TopK-monoid pipeline preset. Each event contributes one (key, 1)
// observation to the running top-K sketch.
func TopN[T any](name string, k uint32, elementFn func(T) string) *TopNBuilder[T] {
	return &TopNBuilder[T]{name: name, k: k, elementFn: elementFn}
}

// TopNBuilder builds a TopN (Misra-Gries) pipeline. Required: KeyBy, StoreIn.
// Optional: From, Daily windowing.
type TopNBuilder[T any] struct {
	name      string
	k         uint32
	keyFn     func(T) string
	elementFn func(T) string
	src       source.Source[T]
	store     state.Store[[]byte]
	window    *windowed.Config
}

// From sets the live event source.
func (b *TopNBuilder[T]) From(s source.Source[T]) *TopNBuilder[T] { b.src = s; return b }

// KeyBy sets the function that derives the aggregation key from each event
// (e.g. "global" for a single Top-N over all events, or a per-tenant ID).
func (b *TopNBuilder[T]) KeyBy(fn func(T) string) *TopNBuilder[T] { b.keyFn = fn; return b }

// StoreIn sets the state store. TopK sketch state is stored as []byte; pair
// with [pkg/state/dynamodb.BytesStore].
func (b *TopNBuilder[T]) StoreIn(s state.Store[[]byte]) *TopNBuilder[T] {
	b.store = s
	return b
}

// Daily configures daily tumbling buckets with the given retention. Useful
// for "today's top 10" / "last 7 days' top 10" — each bucket gets its own
// Misra-Gries summary; the query layer merges N adjacent buckets.
func (b *TopNBuilder[T]) Daily(retention time.Duration) *TopNBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

// Build assembles a *pipeline.Pipeline ready for streaming.Run / bootstrap.Run /
// replay.Run. The value extractor lifts each event's elementFn output into a
// one-element TopK sketch via topk.SingleN at the configured K.
func (b *TopNBuilder[T]) Build() *pipeline.Pipeline[T, []byte] {
	p := pipeline.NewPipeline[T, []byte](b.name).
		Key(b.keyFn).
		Value(func(t T) []byte { return topk.SingleN(b.k, b.elementFn(t), 1) }).
		StoreIn(b.store)
	if b.window != nil {
		p = p.Aggregate(topk.New(b.k), *b.window)
	} else {
		p = p.Aggregate(topk.New(b.k))
	}
	if b.src != nil {
		p = p.From(b.src)
	}
	return p
}

// Trending is a time-decayed-sum pipeline preset. Each event contributes a
// configurable score (default 1.0) to a per-key running sum that decays
// older contributions exponentially toward the most recent observation.
// The result is "score of how much activity has happened recently, with
// older activity counting less" — the canonical building block for "hot"
// / "trending" feeds.
//
// Half-life sets the decay rate. Pass 1*time.Hour for "last hour matters
// most" / "fast-burning trends"; 6*time.Hour for "today's hot"; 24*time.Hour
// for "yesterday and today both matter."
//
// Caveat: this is NOT Reddit's `log(votes) + (time/factor)` formula
// directly — Reddit's formula isn't a monoid. Trending implements
// time-weighted summation, which is the building block; if you want
// Reddit-style log-shape ranking, apply `log(EvaluateAt(d, halfLife, now))`
// at query time.
//
// State type is []byte (DecayedSumBytes wire format); pair with
// pkg/state/dynamodb.BytesStore. Decode with compose.DecodeDecayed and
// evaluate at query time via compose.EvaluateAt.
func Trending[T any](name string, halfLife time.Duration) *TrendingBuilder[T] {
	return &TrendingBuilder[T]{
		name:     name,
		halfLife: halfLife,
		amountFn: func(T) float64 { return 1.0 }, // sensible default: each event = 1 unit
	}
}

// TrendingBuilder builds a Trending (DecayedSumBytes) pipeline. Required:
// KeyBy, StoreIn. Optional: From, Daily/Hourly windowing, Amount (override
// the default per-event score), Clock (override time.Now for tests).
type TrendingBuilder[T any] struct {
	name     string
	halfLife time.Duration
	keyFn    func(T) string
	amountFn func(T) float64
	src      source.Source[T]
	store    state.Store[[]byte]
	window   *windowed.Config
	now      func() time.Time
}

// From sets the live event source.
func (b *TrendingBuilder[T]) From(s source.Source[T]) *TrendingBuilder[T] {
	b.src = s
	return b
}

// KeyBy sets the function that derives the aggregation key from each event
// (e.g. post ID for "trending posts" or hashtag for "trending hashtags").
// Required.
func (b *TrendingBuilder[T]) KeyBy(fn func(T) string) *TrendingBuilder[T] {
	b.keyFn = fn
	return b
}

// Amount overrides the per-event contribution score. Default is 1.0 per
// event. Use this for weighted trending — e.g., a like from a verified
// account contributes 5.0 instead of 1.0:
//
//	murmur.Trending[Like]("hot_posts", time.Hour).
//	    Amount(func(l Like) float64 {
//	        if l.UserVerified { return 5.0 }
//	        return 1.0
//	    })
func (b *TrendingBuilder[T]) Amount(fn func(T) float64) *TrendingBuilder[T] {
	if fn != nil {
		b.amountFn = fn
	}
	return b
}

// StoreIn sets the state store. Pair with pkg/state/dynamodb.NewBytesStore
// using compose.DecayedSumBytes(halfLife) as the monoid.
func (b *TrendingBuilder[T]) StoreIn(s state.Store[[]byte]) *TrendingBuilder[T] {
	b.store = s
	return b
}

// Daily configures daily tumbling buckets with the given retention.
// Useful for "today's hottest" / "this week's hottest" — each bucket is a
// separate decayed-sum row; queries merge N adjacent buckets via the
// monoid's Combine.
func (b *TrendingBuilder[T]) Daily(retention time.Duration) *TrendingBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

// Hourly configures hourly tumbling buckets — typical for fast-burning
// "trending right now" feeds.
func (b *TrendingBuilder[T]) Hourly(retention time.Duration) *TrendingBuilder[T] {
	w := windowed.Hourly(retention)
	b.window = &w
	return b
}

// Clock overrides time.Now for the per-event timestamp. Useful for tests
// with deterministic clocks; production code should leave this unset.
func (b *TrendingBuilder[T]) Clock(now func() time.Time) *TrendingBuilder[T] {
	if now != nil {
		b.now = now
	}
	return b
}

// Build assembles a *pipeline.Pipeline. The value extractor lifts each
// event's amountFn output to a Decayed observation timestamped at the
// configured clock (default time.Now), encoded via compose.DecayedBytes.
func (b *TrendingBuilder[T]) Build() *pipeline.Pipeline[T, []byte] {
	now := b.now
	if now == nil {
		now = time.Now
	}
	p := pipeline.NewPipeline[T, []byte](b.name).
		Key(b.keyFn).
		Value(func(t T) []byte {
			return compose.DecayedBytes(b.amountFn(t), now())
		}).
		StoreIn(b.store)
	if b.window != nil {
		p = p.Aggregate(compose.DecayedSumBytes(b.halfLife), *b.window)
	} else {
		p = p.Aggregate(compose.DecayedSumBytes(b.halfLife))
	}
	if b.src != nil {
		p = p.From(b.src)
	}
	return p
}
