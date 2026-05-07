package monoidlaws_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/monoidlaws"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
)

// Each of these is a "real-monoid contract" test for a built-in monoid. Adding a
// new monoid? Add it here so a regression in associativity or identity surfaces
// loudly — and so users have a copy-paste pattern for their own custom monoids.

func TestSum_Int64(t *testing.T) {
	monoidlaws.TestMonoid(t, core.Sum[int64](), func(i int) int64 {
		return int64(i*i - i*7) // mix of pos / neg
	})
}

func TestCount_Int64(t *testing.T) {
	monoidlaws.TestMonoid(t, core.Count(), func(i int) int64 {
		return int64(i)
	})
}

func TestMin_Int(t *testing.T) {
	monoidlaws.TestMonoid(t, core.Min[int](), func(i int) core.Bounded[int] {
		// Include negatives — the original Min broke here.
		return core.NewBounded(i*3 - 50)
	})
}

func TestMax_Int(t *testing.T) {
	monoidlaws.TestMonoid(t, core.Max[int](), func(i int) core.Bounded[int] {
		return core.NewBounded(i*3 - 50)
	})
}

func TestSet_String(t *testing.T) {
	// nil and empty map are both "the empty set" under Set semantics, so use a
	// length-aware equality rather than DeepEqual.
	eq := func(a, b map[string]struct{}) bool {
		if len(a) != len(b) {
			return false
		}
		for k := range a {
			if _, ok := b[k]; !ok {
				return false
			}
		}
		return true
	}
	monoidlaws.TestMonoid(t, core.Set[string](), func(i int) map[string]struct{} {
		out := map[string]struct{}{}
		for j := 0; j < i%4; j++ {
			out[string(rune('a'+i+j))] = struct{}{}
		}
		return out
	}, monoidlaws.WithEqual(eq))
}

func TestBloom(t *testing.T) {
	m := bloom.Bloom()
	monoidlaws.TestMonoid(t, m, func(i int) []byte {
		// One element per generated sketch.
		return bloom.Single([]byte{byte(i % 251), byte(i / 251)})
	})
}

func TestTopK_K10(t *testing.T) {
	m := topk.New(10)
	monoidlaws.TestMonoid(t, m, func(i int) []byte {
		return topk.SingleN(10, string(rune('a'+(i%26))), 1)
	})
}

func TestHLL(t *testing.T) {
	// HLL Combine is union of sketches: ASSOCIATIVE as a set operation, but
	// the underlying axiomhq/hyperloglog wire format stores sparse registers
	// in arrival order, not canonical order. Two byte slices representing
	// the same logical union may differ bytewise. We compare via Estimate —
	// equal cardinality estimates imply equivalent sketches at the unit-set
	// level the test exercises.
	m := hll.HLL()
	gen := func(i int) []byte {
		// Distinct element values across i give a non-trivial sketch.
		return hll.Single([]byte{byte(i % 251), byte(i / 251)})
	}
	cardEqual := func(a, b []byte) bool {
		ea, err := hll.Estimate(a)
		if err != nil {
			return false
		}
		eb, err := hll.Estimate(b)
		if err != nil {
			return false
		}
		return ea == eb
	}
	monoidlaws.TestMonoid(t, m, gen, monoidlaws.WithEqual(cardEqual))
}

func TestFirst_Stamped(t *testing.T) {
	// First keeps the lowest-timestamped Stamped value.
	m := core.First[string]()
	monoidlaws.TestMonoid(t, m, func(i int) core.Stamped[string] {
		return core.Stamped[string]{
			Value: string(rune('a' + i%26)),
			Time:  int64(i),
			Set:   true,
		}
	})
}

func TestLast_Stamped(t *testing.T) {
	m := core.Last[string]()
	monoidlaws.TestMonoid(t, m, func(i int) core.Stamped[string] {
		return core.Stamped[string]{
			Value: string(rune('a' + i%26)),
			Time:  int64(i),
			Set:   true,
		}
	})
}

func TestDecayedSumBytes_FloatTolerant(t *testing.T) {
	// Bytes-encoded variant of DecayedSum. Round-trips through encode/decode
	// at every Combine, so the comparison is on the bytes — but the underlying
	// floating-point math has the same ULP-level non-determinism as the
	// typed DecayedSum, so we decode and compare with tolerance.
	m := compose.DecayedSumBytes(30 * time.Minute)
	gen := func(i int) []byte {
		return compose.EncodeDecayed(compose.Decayed{
			Value: float64(i) * 1.5,
			T:     int64(i) * int64(time.Minute),
			Set:   true,
		})
	}
	tol := func(a, b []byte) bool {
		da := compose.DecodeDecayed(a)
		db := compose.DecodeDecayed(b)
		if da.Set != db.Set || da.T != db.T {
			return false
		}
		diff := math.Abs(da.Value - db.Value)
		return diff < 1e-12 || diff < 1e-9*math.Max(math.Abs(da.Value), math.Abs(db.Value))
	}
	monoidlaws.TestMonoid(t, m, gen,
		monoidlaws.WithSamples[[]byte](12),
		monoidlaws.WithEqual(tol),
	)
}

func TestMapMerge_StringSum(t *testing.T) {
	// MapMerge[K, V] composes any inner monoid pointwise across the map keys.
	// Identity is the empty map; Combine is a key-wise Combine via the inner
	// monoid.
	m := compose.MapMerge[string](core.Sum[int64]())
	eq := func(a, b map[string]int64) bool {
		if len(a) != len(b) {
			return false
		}
		for k, va := range a {
			if vb, ok := b[k]; !ok || va != vb {
				return false
			}
		}
		return true
	}
	monoidlaws.TestMonoid(t, m, func(i int) map[string]int64 {
		// Mix of overlapping and non-overlapping keys to exercise both
		// paths in the merge.
		out := map[string]int64{}
		for j := 0; j < 3; j++ {
			out[string(rune('a'+(i+j)%5))] = int64((i + j) * 7)
		}
		return out
	}, monoidlaws.WithEqual(eq))
}

func TestTupleMonoid2_SumAndMin(t *testing.T) {
	// Tuple of two monoids; each component combines independently.
	type pair = compose.Tuple2[int64, core.Bounded[int]]
	m := compose.TupleMonoid2[int64, core.Bounded[int]](
		core.Sum[int64](),
		core.Min[int](),
	)
	monoidlaws.TestMonoid[pair](t, m, func(i int) pair {
		return compose.Tuple2[int64, core.Bounded[int]]{
			A: int64(i*i - i*5),
			B: core.NewBounded(i*3 - 50),
		}
	})

	// Compile-time invariant: TupleMonoid2 is a monoid.Monoid over Tuple2.
	var _ monoid.Monoid[pair] = m
}

func TestDecayedSum_FloatTolerant(t *testing.T) {
	m := compose.DecayedSum(30 * time.Minute)

	// FP: associativity holds within ULPs of the magnitude. The default DeepEqual
	// would flag ~1e-9 mismatches, so wire a tolerance comparator.
	gen := func(i int) compose.Decayed {
		return compose.Decayed{
			Value: float64(i) * 1.5,
			T:     int64(i) * int64(time.Minute),
			Set:   true,
		}
	}
	tol := func(a, b compose.Decayed) bool {
		if a.Set != b.Set || a.T != b.T {
			return false
		}
		diff := math.Abs(a.Value - b.Value)
		return diff < 1e-12 || diff < 1e-9*math.Max(math.Abs(a.Value), math.Abs(b.Value))
	}
	monoidlaws.TestMonoid(t, m, gen,
		monoidlaws.WithSamples[compose.Decayed](12),
		monoidlaws.WithEqual(tol),
	)
}
