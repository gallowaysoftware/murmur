package compose

import (
	"math"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Decayed is a (value, time) pair under exponential decay. Combine takes the most
// recent timestamp's reference frame and decays the older value forward to it before
// adding. With an appropriate half-life, this implements time-weighted moving sums
// and averages without windowed bucketing.
//
// Mathematically: Combine((v_a, t_a), (v_b, t_b)) where t_b ≥ t_a is
//
//	(v_a * 2^(-(t_b - t_a)/halfLife) + v_b, t_b)
//
// Identity is (0, t_min). Associativity follows from the commutativity of decay-and-sum.
type Decayed struct {
	// Value is the current decayed sum at time T.
	Value float64
	// T is the reference timestamp (Unix nanoseconds).
	T int64
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
	// Identity short-circuits.
	if a.T == 0 && a.Value == 0 {
		return b
	}
	if b.T == 0 && b.Value == 0 {
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
	}
}

func (decayedMonoid) Kind() monoid.Kind { return monoid.KindCustom }

// DecayedAt builds a Decayed observation for use as a per-event delta. amount is the
// raw contribution at time t.
func DecayedAt(amount float64, t time.Time) Decayed {
	return Decayed{Value: amount, T: t.UnixNano()}
}

// DecayedNow is equivalent to DecayedAt(amount, time.Now()).
func DecayedNow(amount float64) Decayed {
	return DecayedAt(amount, time.Now())
}

// EvaluateAt returns the value of d evaluated at time t. If t is in the future
// relative to d.T, the value is decayed forward; if t is in the past, the value is
// "scaled up" (rarely useful, supplied for completeness). Use this from the query
// layer when "the value as of now" matters more than the stored reference time.
func EvaluateAt(d Decayed, halfLife time.Duration, t time.Time) float64 {
	if d.T == 0 {
		return d.Value
	}
	dtSec := float64(t.UnixNano()-d.T) / 1e9
	hl := halfLife.Seconds()
	if hl <= 0 {
		return d.Value
	}
	return d.Value * math.Exp2(-dtSec/hl)
}
