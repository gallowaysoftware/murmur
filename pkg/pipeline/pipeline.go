// Package pipeline is the Murmur DSL surface — the entry point users touch when defining
// an aggregation. A Pipeline composes a Source, a key-extraction function, a value-
// extraction function, a structural Monoid, an optional windowing config, a primary
// State store, and an optional read Cache. Built pipelines are deployed by the runtime
// (pkg/exec/...) onto streaming/bootstrap/batch targets.
//
// Type parameters:
//
//	T — the record type emitted by the Source
//	V — the aggregation value type (e.g. int64 for counters; []byte for sketch state)
//
// Aggregation keys are always strings (matching the DDB partition-key shape). Composite
// keys are the user's responsibility to encode — return e.PageID + "|" + e.Region from
// the key extractor.
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
type Pipeline[T any, V any] struct {
	name    string
	src     source.Source[T]
	keyFn   func(T) string
	keysFn  func(T) []string // when set, takes precedence over keyFn (multi-key fanout)
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
func NewPipeline[T any, V any](name string) *Pipeline[T, V] {
	return &Pipeline[T, V]{name: name}
}

// From sets the live event source.
func (p *Pipeline[T, V]) From(s source.Source[T]) *Pipeline[T, V] {
	p.src = s
	return p
}

// Key sets the function that extracts the aggregation key from an event. Composite keys
// should be encoded into a single string by the caller (e.g. "<page>|<region>").
//
// Mutually exclusive with KeyByMany. If both are set, KeyByMany takes precedence — Key
// is ignored.
func (p *Pipeline[T, V]) Key(fn func(T) string) *Pipeline[T, V] {
	p.keyFn = fn
	return p
}

// KeyByMany sets a multi-key extractor: each event contributes its value to every
// key returned by fn. Used for hierarchical rollups — one "like" event might
// contribute to:
//
//	[]string{
//	    "post:" + e.PostID,                                        // per-post total
//	    "post:" + e.PostID + "|country:" + e.Country,              // per-post-per-country
//	    "country:" + e.Country,                                    // per-country total
//	    "global",                                                  // global total
//	}
//
// Each emitted key triggers an independent state.MergeUpdate against the same store,
// so an N-level hierarchy costs N store writes per event. Cost-balance against your
// query patterns: explicit rollups are cheap to query (one read per level) but
// expensive to write; query-time rollups are the inverse.
//
// Dedup is applied ONCE per event (against the EventID); on a duplicate, all N
// per-key merges are skipped together.
//
// Mutually exclusive with Key.
func (p *Pipeline[T, V]) KeyByMany(fn func(T) []string) *Pipeline[T, V] {
	p.keysFn = fn
	return p
}

// Value sets the function that extracts the aggregation value from an event. For
// counters, this is typically `func(_ T) int64 { return 1 }`. For sketches that ingest
// arbitrary keys, the value function would derive the keying byte slice.
func (p *Pipeline[T, V]) Value(fn func(T) V) *Pipeline[T, V] {
	p.valueFn = fn
	return p
}

// Aggregate sets the structural monoid and any optional windowing config. Pass at most
// one *windowed.Config — additional configs are ignored.
func (p *Pipeline[T, V]) Aggregate(m monoid.Monoid[V], opts ...windowed.Config) *Pipeline[T, V] {
	p.mon = m
	if len(opts) > 0 {
		w := opts[0]
		p.window = &w
	}
	return p
}

// StoreIn sets the primary state store. DynamoDB is the recommended default; this is
// the source of truth for aggregations.
func (p *Pipeline[T, V]) StoreIn(s state.Store[V]) *Pipeline[T, V] {
	p.store = s
	return p
}

// Cache sets an optional read cache + sketch-accelerator. Valkey is the typical choice.
// The cache is never source of truth; its contents are repopulatable from the primary
// Store at any time.
func (p *Pipeline[T, V]) Cache(c state.Cache[V]) *Pipeline[T, V] {
	p.cache = c
	return p
}

// ServeOn configures the auto-generated gRPC query service.
func (p *Pipeline[T, V]) ServeOn(q QueryConfig) *Pipeline[T, V] {
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

// Build validates the pipeline definition for fields required by every execution mode:
// key extractor (Key OR KeyByMany), value extractor, monoid, state store. The source
// is mode-specific (Live needs source.Source; Bootstrap needs snapshot.Source) and is
// checked by the runtime that consumes the pipeline.
func (p *Pipeline[T, V]) Build() error {
	switch {
	case p.keyFn == nil && p.keysFn == nil:
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
func (p *Pipeline[T, V]) Name() string { return p.name }

// Source returns the configured source (nil before From).
func (p *Pipeline[T, V]) Source() source.Source[T] { return p.src }

// KeyFn returns the configured single-key extractor (nil if KeyByMany was used
// instead, or if no extractor was set).
func (p *Pipeline[T, V]) KeyFn() func(T) string { return p.keyFn }

// KeysFn returns a function that produces all keys an event should contribute to.
// If KeyByMany was set, returns it directly; otherwise wraps the single-key Key
// extractor in a 1-element slice. Returns nil if neither was set.
//
// Runtimes (streaming.Run, lambda handlers) should call KeysFn rather than KeyFn
// so multi-key fanout works transparently.
func (p *Pipeline[T, V]) KeysFn() func(T) []string {
	if p.keysFn != nil {
		return p.keysFn
	}
	if p.keyFn != nil {
		fn := p.keyFn
		return func(t T) []string { return []string{fn(t)} }
	}
	return nil
}

// ValueFn returns the configured value extractor (nil before Value).
func (p *Pipeline[T, V]) ValueFn() func(T) V { return p.valueFn }

// Monoid returns the configured monoid (nil before Aggregate).
func (p *Pipeline[T, V]) Monoid() monoid.Monoid[V] { return p.mon }

// Window returns the optional windowing config (nil if not windowed).
func (p *Pipeline[T, V]) Window() *windowed.Config { return p.window }

// Store returns the configured state store (nil before StoreIn).
func (p *Pipeline[T, V]) Store() state.Store[V] { return p.store }

// CacheStore returns the optional cache (nil if none).
func (p *Pipeline[T, V]) CacheStore() state.Cache[V] { return p.cache }

// Query returns the gRPC query configuration.
func (p *Pipeline[T, V]) Query() QueryConfig { return p.query }
