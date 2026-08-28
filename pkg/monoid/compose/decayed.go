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
//
// # Future-dated timestamps
//
// The reference frame Combine adopts is the newer of the two timestamps, so an
// observation stamped far ahead of real time freezes the key: 2^(-4y/24h) underflows
// to exactly zero, the accumulated mass is annihilated, and every real event that
// follows is itself the older operand and is annihilated in turn. The key sits at
// whatever the bogus observation carried.
//
// Combine cannot defend against this, and deliberately does not try — see ClampFuture
// for where the defense lives and why it cannot live here. EvaluateAt contains the
// blast radius on the read side: it will not scale a frozen value up.
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
// A non-positive halfLife means "no decay": Combine is a plain sum and EvaluateAt
// returns the stored value unchanged. The two used to disagree — Combine computed
// 2^(-dt/0), which is NaN at dt=0 and 0 at dt>0, so a zero half-life either poisoned
// the row with a NaN that survives Encode/Decode or silently dropped every older
// contribution, while EvaluateAt on the same row reported it undecayed. A zero
// half-life is reachable by accident: murmur.Trending(name, cfg.HalfLife) with an
// unset Duration field.
//
// To insert an event at processing time, lift it via DecayedAt(amount, time.Now()).
// The streaming runtime hands the resulting Decayed value through Combine, the same
// pattern used by HLL.Single and TopK.SingleN. If the timestamp comes from the EVENT
// rather than from the clock, run it through ClampFuture first.
func DecayedSum(halfLife time.Duration, opts ...Option) monoid.Monoid[Decayed] {
	return decayedMonoid{cfg: newDecayedConfig(halfLife, opts)}
}

// Option configures the decayed-sum monoids. The same options apply to
// DecayedSum and DecayedSumBytes; WithDecodeErrorHandler is only consulted by
// the bytes variant, which is the only one that parses a wire form.
type Option func(*decayedConfig)

// WithDecodeErrorHandler installs a callback invoked when DecayedSumBytes'
// Combine is handed bytes that are not a Decayed wire form.
//
// Combine returns no error — the contract is Combine(a, b) V — so the recovery
// is to keep the operand that decoded and discard the other. Doing that
// silently is the problem: the wire form is a fixed 17 bytes with no magic and
// no length prefix, so any longer blob (an HLL sketch, a Bloom filter, a row
// from a pipeline that changed monoids) used to decode to a Set=true value
// built out of whatever the first 17 bytes happened to be, and that garbage
// then merged into the row as if it were real.
//
// The handler must be cheap and non-blocking; it runs on the merge path.
func WithDecodeErrorHandler(fn func(error)) Option {
	return func(c *decayedConfig) { c.onDecodeErr = fn }
}

type decayedConfig struct {
	halfLife    float64 // seconds
	onDecodeErr func(error)
}

func newDecayedConfig(halfLife time.Duration, opts []Option) decayedConfig {
	c := decayedConfig{halfLife: halfLife.Seconds()}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func (c decayedConfig) reportDecodeError(err error) {
	if c.onDecodeErr != nil {
		c.onDecodeErr(err)
	}
}

type decayedMonoid struct {
	cfg decayedConfig
}

func (decayedMonoid) Identity() Decayed { return Decayed{} }

// Combine is a pure function of its two operands. It reads no clock and holds no
// state, which is not a stylistic preference:
//
//   - dynamodb.BytesStore.MergeUpdate recomputes Combine on every CAS retry. A
//     Combine that consulted the wall clock returned a different answer on the
//     second attempt than the first, so which value got written depended on how
//     many times the conditional write lost a race.
//   - Associativity and identity are fuzzed in CI (pkg/monoid/monoidlaws). A
//     clock inside Combine makes Combine(Combine(a,b),c) and Combine(a,Combine(b,c))
//     differ by however long the first evaluation took.
//
// So the skew bound cannot live here. See ClampFuture.
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

	if m.cfg.halfLife <= 0 {
		// No decay. EvaluateAt has always read a non-positive half-life this
		// way; Combine has to agree or the same row means two different things
		// depending on which side of the pipeline is looking at it.
		return Decayed{Value: older.Value + newer.Value, T: newer.T, Set: true}
	}
	dtSec := float64(newer.T-older.T) / 1e9
	factor := math.Exp2(-dtSec / m.cfg.halfLife)
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

// DefaultSkewBound is the skew allowance ClampFuture uses when a caller has no
// better number: two half-lives. It is scale-free, and it bounds the damage a
// skewed observation can do to a factor of four on the contributions that land
// while the clock catches up.
//
// A half-life past half the Duration range would double into overflow, so it is
// returned unmultiplied; nobody reaches that by accident.
func DefaultSkewBound(halfLife time.Duration) time.Duration {
	switch {
	case halfLife <= 0:
		return 0 // no decay, so there is no reference frame to protect
	case 2*halfLife > halfLife:
		return 2 * halfLife
	default:
		return halfLife
	}
}

// ClampFuture pulls a timestamp more than bound ahead of now back to now+bound.
// Timestamps in the past — however far — are returned untouched, as are all
// timestamps when bound is non-positive.
//
// Use it in a pipeline whose value extractor takes the timestamp from the EVENT
// rather than from the clock, which is the only way a future-dated observation
// can enter the aggregate:
//
//	Value(func(e Event) []byte {
//	    ts := compose.ClampFuture(e.OccurredAt, time.Now(), compose.DefaultSkewBound(halfLife))
//	    return compose.DecayedBytes(1, ts)
//	})
//
// murmur.Trending already stamps every event at its configured clock, so a
// pipeline built through that preset cannot produce a future frame and does not
// need this.
//
// # Why here and not in Combine
//
// The wall clock is the one piece of information that separates a bogus
// timestamp from a legitimate one, and it is exactly the piece Combine is not
// allowed to have: Combine must be pure, or CAS retries and the associativity
// fuzzer both stop being meaningful (see Combine).
//
// A pairwise bound — clamp the newer operand to the older operand's timestamp
// plus a limit, using no clock — is the obvious substitute and it does not
// work. From inside Combine, a state four years in the future beside a real
// event is indistinguishable from a real event four years after a key went
// quiet, so any pairwise clamp large enough to be safe for the first case pins
// legitimately-idle keys at a frame they can never leave: a key idle for 18
// hours at a one-minute half-life would come back holding a quarter of its
// old mass instead of essentially none.
//
// The lift is where an untrusted event timestamp becomes state, it runs once
// per observation rather than once per merge attempt, and it is the last point
// at which a clock reading is still honest. So that is where the bound goes.
func ClampFuture(t, now time.Time, bound time.Duration) time.Time {
	if bound <= 0 {
		return t
	}
	limit := now.Add(bound)
	if t.After(limit) {
		return limit
	}
	return t
}

// DecayedAt builds a Decayed observation for use as a per-event delta. amount is the
// raw contribution at time t. The returned value has Set=true so it round-trips
// through Combine(Identity, ...) correctly.
//
// t is honoured exactly. If it came off an event rather than off a clock, wrap it
// in ClampFuture — Combine will not second-guess it later.
func DecayedAt(amount float64, t time.Time) Decayed {
	return Decayed{Value: amount, T: t.UnixNano(), Set: true}
}

// DecayedNow is equivalent to DecayedAt(amount, time.Now()).
func DecayedNow(amount float64) Decayed {
	return DecayedAt(amount, time.Now())
}

// EvaluateAt returns the value of d evaluated at time t, decayed forward from
// d.T. Use this from the query layer when "the value as of now" matters more
// than the stored reference time.
//
// Evaluating at a t BEFORE d.T returns d.Value unchanged rather than scaling
// it up. Un-decaying is always an over-estimate — it re-inflates every
// contribution recorded between t and d.T as though it had been observed at t
// — and the over-estimate is unbounded: one event stamped a year ahead
// evaluates to 7.5e109 at halfLife=24h, and one stamped four years ahead to
// +Inf. A single such row outranks every honest score in the index forever,
// which is not a defensible reading of "the value as of now". This is also the
// read-side backstop for a row that was frozen by a future-dated observation
// before ClampFuture was in the pipeline: the key is stuck, but it is stuck at
// a finite value rather than at infinity.
//
// Returns 0 for an unset Decayed, and d.Value for a non-positive halfLife.
func EvaluateAt(d Decayed, halfLife time.Duration, t time.Time) float64 {
	if !d.Set {
		return 0
	}
	hl := halfLife.Seconds()
	if hl <= 0 {
		return d.Value
	}
	dtSec := float64(t.UnixNano()-d.T) / 1e9
	if dtSec <= 0 {
		return d.Value
	}
	return d.Value * math.Exp2(-dtSec/hl)
}
