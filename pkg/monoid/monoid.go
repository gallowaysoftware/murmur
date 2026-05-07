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

const (
	KindSum    Kind = "sum"
	KindCount  Kind = "count"
	KindMin    Kind = "min"
	KindMax    Kind = "max"
	KindFirst  Kind = "first"
	KindLast   Kind = "last"
	KindSet    Kind = "set"
	KindHLL    Kind = "hll"
	KindTopK   Kind = "topk"
	KindBloom  Kind = "bloom"
	KindMap    Kind = "map"
	KindTuple  Kind = "tuple"
	KindCustom Kind = "custom"
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
