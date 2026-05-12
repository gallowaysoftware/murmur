// Package monoid defines the structural-monoid abstraction at the heart of Murmur.
//
// A Monoid is the algebraic primitive Murmur uses to express "how aggregations combine":
// an associative Combine operation and an identity element. Monoids are *structural* —
// they carry a Kind that backend executors (Spark codegen, Valkey-native commands, DDB
// atomic adds) dispatch on to translate the abstract operation into a runtime-native
// implementation. Custom (user-defined) monoids carry KindCustom and only run on the Go
// execution backends.
package monoid

// Kind identifies a structural monoid for backend dispatch. Backends switch on Kind to
// emit native Spark plans, Valkey commands, DDB UpdateExpressions, etc. KindCustom marks
// an opaque user-defined monoid that only the Go executors can run.
type Kind string

// Built-in Kind values. Backend executors switch on these to translate the
// abstract monoid into a runtime-native operation; admin servers use them to
// pick a typed decoder for the Query Console; users adding a new well-known
// monoid should add a new Kind constant here so that switch-statements
// elsewhere fail loudly during compile rather than silently fall through to
// KindCustom.
const (
	KindSum    Kind = "sum"    // numeric addition; DDB ADD on int64 / float64
	KindCount  Kind = "count"  // event-counting flavor of Sum (always +1)
	KindMin    Kind = "min"    // running minimum; uses core.Bounded[V]
	KindMax    Kind = "max"    // running maximum; uses core.Bounded[V]
	KindFirst  Kind = "first"  // earliest-timestamp wins
	KindLast   Kind = "last"   // latest-timestamp wins
	KindSet    Kind = "set"    // map-set union
	KindHLL    Kind = "hll"    // HyperLogLog cardinality sketch
	KindTopK   Kind = "topk"   // Misra-Gries top-K heavy hitters
	KindBloom  Kind = "bloom"  // Bloom filter set membership
	KindMap    Kind = "map"    // per-key Combine via inner monoid
	KindTuple  Kind = "tuple"  // componentwise Combine via inner monoids
	KindCustom Kind = "custom" // user-defined opaque monoid; Go-only backends
)

// Monoid is an associative binary operation Combine over values of type V with an Identity
// element. Implementations must satisfy:
//
//	Combine(Identity(), x) == x
//	Combine(x, Identity()) == x
//	Combine(Combine(x, y), z) == Combine(x, Combine(y, z))
//
// Backend executors may rely on Kind() to translate Combine into a native operation
// (e.g., DynamoDB ADD, Spark sum aggregation, Valkey PFADD).
type Monoid[V any] interface {
	Identity() V
	Combine(a, b V) V
	Kind() Kind
}

// IsStructural reports whether the monoid kind is recognized by backend executors and
// can be translated to native operations on Spark, Valkey, and DynamoDB. Custom monoids
// return false and only run on Go execution backends.
func IsStructural(k Kind) bool {
	return k != KindCustom
}

// IsAdditive reports whether values produced by a monoid of the given Kind can be safely
// pre-combined client-side and applied to a state store via a single MergeUpdate per key
// without changing the result. In practice this means the monoid is commutative and its
// values are cheap to combine in memory — the canonical examples are KindSum and
// KindCount, which back DynamoDB ADD-style atomic increments.
//
// In-memory delta coalescing (pkg/exec/processor.Coalescer) consults this to decide
// whether a batch of per-event deltas can collapse to a single MergeUpdate per unique
// key. Monoids whose Kind is KindCustom may opt in to the same fast path by implementing
// the Additive interface defined below.
//
// Non-additive Kinds (KindMin / KindMax / KindFirst / KindLast / KindSet / KindMap /
// KindTuple / KindHLL / KindTopK / KindBloom) return false: their values typically
// require a read-modify-write CAS path or carry sketch state where pre-combining adds
// no wire savings and may complicate retry semantics.
func IsAdditive(k Kind) bool {
	switch k {
	case KindSum, KindCount:
		return true
	default:
		return false
	}
}

// Additive is an opt-in capability for monoids whose Kind is KindCustom but whose Combine
// is still safe to apply pre-store-write. Custom decayed-sum and other linearly-combining
// numeric monoids should implement this returning true so the coalescing path picks them
// up. Monoids whose Kind already returns true from IsAdditive(Kind) need not implement
// this interface — the Kind check wins.
type Additive interface {
	// IsAdditive reports whether the implementing monoid supports client-side
	// pre-combining for delta coalescing.
	IsAdditive() bool
}

// IsAdditiveMonoid is a convenience helper that returns true if either the monoid's Kind
// is in the additive built-in set or the monoid implements Additive with IsAdditive()
// returning true. Use this from runtimes rather than calling IsAdditive(k) directly when
// you have a full Monoid handle.
func IsAdditiveMonoid[V any](m Monoid[V]) bool {
	if IsAdditive(m.Kind()) {
		return true
	}
	if a, ok := any(m).(Additive); ok {
		return a.IsAdditive()
	}
	return false
}
