package monoidlaws_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/monoidlaws"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
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
