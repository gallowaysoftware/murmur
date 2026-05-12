package compose

import (
	"math"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Decayed is a (value, time) observation under exponential decay. Combine takes the
// most recent timestamp's reference frame and decays the older value forward to it
// before adding. With an appropriate half-life, this implements time-weighted moving
// sums and averages without windowed bucketing.
//
// Mathematically: Combine((v_a, t_a), (v_b, t_b)) where t_b ≥ t_a is
//
//	(v_a * 2^(-(t_b - t_a)/halfLife) + v_b, t_b)
//
// Identity is the unset Decayed; the Set flag distinguishes "no value yet" from a
// legitimate (0, t=0) observation. This preserves the identity law:
// Combine(Identity, x) == x for all x.
//
// Associativity is exact in real arithmetic; in IEEE-754 floats it holds within ULP
// for typical inputs but is not bitwise.
type Decayed struct {
	// Value is the current decayed sum at time T.
	Value float64
	// T is the reference timestamp (Unix nanoseconds).
	T int64
	// Set is true when this observation carries a real value; false for Identity.
	Set bool
}

// DecayedSum returns a monoid that decays older contributions toward the latest
// timestamp before summing. halfLife controls how fast contributions fade — pass
// 24*time.Hour for a "last day matters most" feel; 1h for "last hour matters most".
//
// To insert an event at processing time, lift it via DecayedAt(amount, time.Now()).
// The streaming runtime hands the resulting Decayed value through Combine, the same
// pattern used by HLL.Single and TopK.SingleN.
func DecayedSum(halfLife time.Duration) monoid.Monoid[Decayed] {
	return decayedMonoid{halfLife: halfLife.Seconds()}
}

type decayedMonoid struct {
	halfLife float64 // seconds
}

func (decayedMonoid) Identity() Decayed { return Decayed{} }

func (m decayedMonoid) Combine(a, b Decayed) Decayed {
	switch {
	case !a.Set:
		return b
	case !b.Set:
		return a
	}
	// Forward-decay the older operand to the newer reference frame.
	older, newer := a, b
	if a.T > b.T {
		older, newer = b, a
	}
	dtSec := float64(newer.T-older.T) / 1e9
	factor := math.Exp2(-dtSec / m.halfLife)
	return Decayed{
		Value: older.Value*factor + newer.Value,
		T:     newer.T,
		Set:   true,
	}
}

func (decayedMonoid) Kind() monoid.Kind { return monoid.KindCustom }

// IsAdditive opts decayed-sum into the in-memory delta-coalescing fast path
// (pkg/exec/processor.Coalescer). Combine over Decayed values is associative and
// commutative — coalescing N per-event deltas into one before MergeUpdate produces the
// same final state as N individual MergeUpdate calls, modulo IEEE-754 ULP slop.
func (decayedMonoid) IsAdditive() bool { return true }

// DecayedAt builds a Decayed observation for use as a per-event delta. amount is the
// raw contribution at time t. The returned value has Set=true so it round-trips
// through Combine(Identity, ...) correctly.
func DecayedAt(amount float64, t time.Time) Decayed {
	return Decayed{Value: amount, T: t.UnixNano(), Set: true}
}

// DecayedNow is equivalent to DecayedAt(amount, time.Now()).
func DecayedNow(amount float64) Decayed {
	return DecayedAt(amount, time.Now())
}

// EvaluateAt returns the value of d evaluated at time t. If t is in the future
// relative to d.T, the value is decayed forward; if t is in the past, the value is
// "scaled up" (rarely useful, supplied for completeness). Use this from the query
// layer when "the value as of now" matters more than the stored reference time.
//
// Returns 0 for an unset Decayed.
func EvaluateAt(d Decayed, halfLife time.Duration, t time.Time) float64 {
	if !d.Set {
		return 0
	}
	dtSec := float64(t.UnixNano()-d.T) / 1e9
	hl := halfLife.Seconds()
	if hl <= 0 {
		return d.Value
	}
	return d.Value * math.Exp2(-dtSec/hl)
}
