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

func (b *CounterBuilder[T]) From(s source.Source[T]) *CounterBuilder[T]      { b.src = s; return b }
func (b *CounterBuilder[T]) KeyBy(fn func(T) string) *CounterBuilder[T]      { b.keyFn = fn; return b }
func (b *CounterBuilder[T]) StoreIn(s state.Store[int64]) *CounterBuilder[T] { b.store = s; return b }
func (b *CounterBuilder[T]) Cache(c state.Cache[int64]) *CounterBuilder[T]   { b.cache = c; return b }

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

type UniqueCountBuilder[T any] struct {
	name      string
	keyFn     func(T) string
	elementFn func(T) []byte
	src       source.Source[T]
	store     state.Store[[]byte]
	window    *windowed.Config
}

func (b *UniqueCountBuilder[T]) From(s source.Source[T]) *UniqueCountBuilder[T] { b.src = s; return b }
func (b *UniqueCountBuilder[T]) KeyBy(fn func(T) string) *UniqueCountBuilder[T] {
	b.keyFn = fn
	return b
}
func (b *UniqueCountBuilder[T]) StoreIn(s state.Store[[]byte]) *UniqueCountBuilder[T] {
	b.store = s
	return b
}

func (b *UniqueCountBuilder[T]) Daily(retention time.Duration) *UniqueCountBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

func (b *UniqueCountBuilder[T]) Hourly(retention time.Duration) *UniqueCountBuilder[T] {
	w := windowed.Hourly(retention)
	b.window = &w
	return b
}

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

type TopNBuilder[T any] struct {
	name      string
	k         uint32
	keyFn     func(T) string
	elementFn func(T) string
	src       source.Source[T]
	store     state.Store[[]byte]
	window    *windowed.Config
}

func (b *TopNBuilder[T]) From(s source.Source[T]) *TopNBuilder[T]       { b.src = s; return b }
func (b *TopNBuilder[T]) KeyBy(fn func(T) string) *TopNBuilder[T]       { b.keyFn = fn; return b }
func (b *TopNBuilder[T]) StoreIn(s state.Store[[]byte]) *TopNBuilder[T] { b.store = s; return b }

func (b *TopNBuilder[T]) Daily(retention time.Duration) *TopNBuilder[T] {
	w := windowed.Daily(retention)
	b.window = &w
	return b
}

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
