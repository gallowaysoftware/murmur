// Package pipeline is the Murmur DSL surface — the entry point users touch when defining
// an aggregation. A Pipeline composes a Source, a key-extraction function, a value-
// extraction function, a structural Monoid, an optional windowing config, a primary
// State store, and an optional read Cache. Built pipelines are deployed by the runtime
// (pkg/exec/...) onto streaming/bootstrap/batch targets.
//
// Type parameters:
//
//	T — the record type emitted by the Source
//	K — the aggregation key type (e.g. string for PageID)
//	V — the aggregation value type (e.g. int64 for counters; []byte for sketch state)
//
// Generics are explicit at NewPipeline. This trades some ergonomics for a single
// builder type rather than a tower of stage-typed builders. Refining the DSL surface is
// a Phase 2 design task once we have real users.
package pipeline

import (
	"errors"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Pipeline is the assembled aggregation definition. Use the builder methods (From, Key,
// Value, Aggregate, StoreIn, Cache, ServeOn) to populate it, then Build to validate and
// freeze.
type Pipeline[T any, K comparable, V any] struct {
	name    string
	src     source.Source[T]
	keyFn   func(T) K
	valueFn func(T) V
	mon     monoid.Monoid[V]
	window  *windowed.Config
	store   state.Store[V]
	cache   state.Cache[V]
	query   QueryConfig
}

// QueryConfig describes how the auto-generated gRPC service should be served.
// Concrete generation lives in pkg/query/codegen.
type QueryConfig struct {
	GRPCAddr string
	HTTPAddr string // optional grpc-gateway HTTP/JSON listener (Phase 2)
}

// NewPipeline begins constructing a pipeline with the given name. The name is used as
// the basis for state-table names, metrics labels, and the generated gRPC service name.
func NewPipeline[T any, K comparable, V any](name string) *Pipeline[T, K, V] {
	return &Pipeline[T, K, V]{name: name}
}

// From sets the live event source.
func (p *Pipeline[T, K, V]) From(s source.Source[T]) *Pipeline[T, K, V] {
	p.src = s
	return p
}

// Key sets the function that extracts the aggregation key from an event.
func (p *Pipeline[T, K, V]) Key(fn func(T) K) *Pipeline[T, K, V] {
	p.keyFn = fn
	return p
}

// Value sets the function that extracts the aggregation value from an event. For
// counters, this is typically `func(_ T) int64 { return 1 }`. For sketches that ingest
// arbitrary keys, the value function would derive the keying byte slice.
func (p *Pipeline[T, K, V]) Value(fn func(T) V) *Pipeline[T, K, V] {
	p.valueFn = fn
	return p
}

// Aggregate sets the structural monoid and any optional windowing config. Pass at most
// one *windowed.Config — additional configs are ignored.
func (p *Pipeline[T, K, V]) Aggregate(m monoid.Monoid[V], opts ...windowed.Config) *Pipeline[T, K, V] {
	p.mon = m
	if len(opts) > 0 {
		w := opts[0]
		p.window = &w
	}
	return p
}

// StoreIn sets the primary state store. DynamoDB is the recommended default; this is
// the source of truth for aggregations.
func (p *Pipeline[T, K, V]) StoreIn(s state.Store[V]) *Pipeline[T, K, V] {
	p.store = s
	return p
}

// Cache sets an optional read cache + sketch-accelerator. Valkey is the typical choice.
// The cache is never source of truth; its contents are repopulatable from the primary
// Store at any time.
func (p *Pipeline[T, K, V]) Cache(c state.Cache[V]) *Pipeline[T, K, V] {
	p.cache = c
	return p
}

// ServeOn configures the auto-generated gRPC query service.
func (p *Pipeline[T, K, V]) ServeOn(q QueryConfig) *Pipeline[T, K, V] {
	p.query = q
	return p
}

// Errors returned by Build.
var (
	ErrMissingSource  = errors.New("pipeline: source not set (use From)")
	ErrMissingKeyFn   = errors.New("pipeline: key function not set (use Key)")
	ErrMissingValueFn = errors.New("pipeline: value function not set (use Value)")
	ErrMissingMonoid  = errors.New("pipeline: monoid not set (use Aggregate)")
	ErrMissingStore   = errors.New("pipeline: state store not set (use StoreIn)")
)

// Build validates the pipeline definition. After Build returns nil, the pipeline can be
// handed to a runtime executor (streaming, bootstrap, batch).
func (p *Pipeline[T, K, V]) Build() error {
	switch {
	case p.src == nil:
		return ErrMissingSource
	case p.keyFn == nil:
		return ErrMissingKeyFn
	case p.valueFn == nil:
		return ErrMissingValueFn
	case p.mon == nil:
		return ErrMissingMonoid
	case p.store == nil:
		return ErrMissingStore
	}
	return nil
}

// Name returns the pipeline name supplied to NewPipeline.
func (p *Pipeline[T, K, V]) Name() string { return p.name }

// Source returns the configured source (nil before From).
func (p *Pipeline[T, K, V]) Source() source.Source[T] { return p.src }

// KeyFn returns the configured key extractor (nil before Key).
func (p *Pipeline[T, K, V]) KeyFn() func(T) K { return p.keyFn }

// ValueFn returns the configured value extractor (nil before Value).
func (p *Pipeline[T, K, V]) ValueFn() func(T) V { return p.valueFn }

// Monoid returns the configured monoid (nil before Aggregate).
func (p *Pipeline[T, K, V]) Monoid() monoid.Monoid[V] { return p.mon }

// Window returns the optional windowing config (nil if not windowed).
func (p *Pipeline[T, K, V]) Window() *windowed.Config { return p.window }

// Store returns the configured state store (nil before StoreIn).
func (p *Pipeline[T, K, V]) Store() state.Store[V] { return p.store }

// CacheStore returns the optional cache (nil if none).
func (p *Pipeline[T, K, V]) CacheStore() state.Cache[V] { return p.cache }

// Query returns the gRPC query configuration.
func (p *Pipeline[T, K, V]) Query() QueryConfig { return p.query }
