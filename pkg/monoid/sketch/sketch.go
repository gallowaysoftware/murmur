// Package sketch defines probabilistic-aggregation monoids: HLL (cardinality), TopK
// (heavy hitters), and Bloom (set membership).
//
// Each sketch type has multiple backend implementations:
//   - A pure-Go implementation used by ECS-Fargate streaming and batch workers.
//   - Valkey-native operations (PFADD/PFCOUNT/PFMERGE for HLL, sorted-sets for TopK,
//     Valkey 8 native Bloom) used when a Valkey cache is configured for performance.
//   - Spark equivalents emitted by SparkScalaCodegen / dispatched via SparkConnect for
//     batch backfill jobs.
//
// Cross-backend bit-compatibility is a real constraint: a sketch written by Spark must
// merge correctly with one written by the streaming worker. The implementations here
// standardize on a portable encoding (HLL++ register layout) for state stored in
// DynamoDB; backend-native encodings are used only inside a single execution context
// and converted at boundaries.
//
// This file declares the sketch monoid interfaces and Kind constants. Concrete
// implementations land in subpackages (hll, topk, bloom) — Phase 1 in-progress.
package sketch

import (
	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// HLL is a HyperLogLog cardinality sketch. Combine merges register-wise. Cardinality
// estimates are produced by the query layer, not Combine itself; Combine returns a new
// merged sketch.
type HLL interface {
	monoid.Monoid[[]byte]
	// Estimate returns the approximate cardinality of the sketch encoded in b.
	Estimate(b []byte) (uint64, error)
	// Add inserts an element by its hashed representation. Used by streaming workers.
	Add(sketch []byte, element []byte) ([]byte, error)
	// Precision returns the configured register precision (typical: 14 → ~1.6% error).
	Precision() uint8
}

// TopK is a heavy-hitter sketch (typical implementation: Misra-Gries / Space-Saving).
// Combine merges two sketches by combining counter sets and keeping the top-K by count.
type TopK interface {
	monoid.Monoid[[]byte]
	// K returns the configured top-K capacity.
	K() int
	// Add increments the count for the given key.
	Add(sketch []byte, key string, count uint64) ([]byte, error)
	// Items returns the top-K items with their estimated counts.
	Items(sketch []byte) ([]Item, error)
}

// Item is a (Key, Count) pair returned by TopK.Items.
type Item struct {
	Key   string
	Count uint64
}

// Bloom is a Bloom filter for approximate set membership. Combine is bitwise-OR of the
// underlying bit arrays.
type Bloom interface {
	monoid.Monoid[[]byte]
	// Add inserts an element.
	Add(sketch []byte, element []byte) ([]byte, error)
	// Contains reports whether element is (probably) in the sketch.
	Contains(sketch []byte, element []byte) (bool, error)
	// FalsePositiveRate returns the configured target FPR.
	FalsePositiveRate() float64
}
