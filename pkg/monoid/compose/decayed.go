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
// The reference frame is bounded: an operand timestamped further ahead of the wall
// clock than the skew bound (WithClockSkewBound, two half-lives by default) is pulled
// back to that limit before the merge, so no single observation can set a frame the
// rest of the stream will never reach. Observations in the past — however far — are
// untouched.
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
// pattern used by HLL.Single and TopK.SingleN.
func DecayedSum(halfLife time.Duration, opts ...Option) monoid.Monoid[Decayed] {
	return decayedMonoid{cfg: newDecayedConfig(halfLife, opts)}
}

// Option configures the decayed-sum monoids. The same options apply to
// DecayedSum and DecayedSumBytes; WithDecodeErrorHandler is only consulted by
// the bytes variant, which is the only one that parses a wire form.
type Option func(*decayedConfig)

// WithClockSkewBound sets how far ahead of the wall clock an observation's
// timestamp may sit before Combine treats it as clock skew and pulls it back.
// The default is two half-lives (see DecayedSum for why); a non-positive bound
// disables the clamp entirely, which is what you want for a pipeline that
// legitimately dates observations into the future.
func WithClockSkewBound(d time.Duration) Option {
	return func(c *decayedConfig) { c.skewBound = d }
}

// WithClock replaces the wall clock Combine measures skew against. Only useful
// for tests — production callers want time.Now, which is the default.
func WithClock(now func() time.Time) Option {
	return func(c *decayedConfig) {
		if now != nil {
			c.now = now
		}
	}
}

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
	skewBound   time.Duration
	now         func() time.Time
	onDecodeErr func(error)
}

func newDecayedConfig(halfLife time.Duration, opts []Option) decayedConfig {
	c := decayedConfig{
		halfLife: halfLife.Seconds(),
		now:      time.Now,
		// Default: two half-lives. Scale-free, and it bounds the damage a
		// skewed observation can do to a factor of four — see clampSkew.
		skewBound: defaultSkewBound(halfLife),
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

func defaultSkewBound(halfLife time.Duration) time.Duration {
	switch {
	case halfLife <= 0:
		return 0 // no decay, so there is no reference frame to protect
	case 2*halfLife > halfLife:
		return 2 * halfLife
	default:
		// A half-life past half the Duration range doubles into overflow. The
		// half-life itself is already a bound nobody reaches by accident.
		return halfLife
	}
}

// clampSkew pulls a timestamp back to now+skewBound.
//
// This is what stops one future-dated event from freezing a key forever. The
// frame Combine adopts is the newer of the two timestamps, so an event stamped
// four years out becomes a frame nothing can ever catch up to: 2^(-4y/24h)
// underflows to exactly zero, so the accumulated mass is annihilated, and then
// every real event that follows is itself the older operand and is annihilated
// in turn. The key sits at whatever the skewed event carried, and the read path
// scales that frozen value up by 2^(+gap/halfLife) — +Inf, or 7.5e109 for a
// one-year skew at halfLife=24h — pinning it to rank #1 for good.
//
// A pairwise horizon ("refuse to decay across more than N half-lives") cannot
// fix this: from inside Combine, a state four years in the future next to a
// real event is indistinguishable from a real event four years after a key
// went quiet, and clamping the second case pins legitimately-idle keys at a
// frame they can never leave. The wall clock is the one piece of information
// that separates them, so that is what we bound against.
//
// With the default bound of two half-lives the worst a skewed observation can
// cost is a factor of four on the contributions that land while the clock
// catches up, and it heals on its own once it does.
func clampSkew(t, limit int64) int64 {
	if t > limit {
		return limit
	}
	return t
}

// skewLimit is the newest timestamp Combine will accept as a reference frame.
// math.MaxInt64 disables the clamp.
func (c decayedConfig) skewLimit() int64 {
	if c.skewBound <= 0 {
		return math.MaxInt64
	}
	now := c.now().UnixNano()
	limit := now + int64(c.skewBound)
	if limit < now {
		// The nanosecond clock runs out in 2262; a bound that overflows it
		// isn't bounding anything.
		return math.MaxInt64
	}
	return limit
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

	limit := m.cfg.skewLimit()
	oldT := clampSkew(older.T, limit)
	newT := clampSkew(newer.T, limit)

	if m.cfg.halfLife <= 0 {
		// No decay. EvaluateAt has always read a non-positive half-life this
		// way; Combine has to agree or the same row means two different things
		// depending on which side of the pipeline is looking at it.
		return Decayed{Value: older.Value + newer.Value, T: newT, Set: true}
	}
	dtSec := float64(newT-oldT) / 1e9
	factor := math.Exp2(-dtSec / m.cfg.halfLife)
	return Decayed{
		Value: older.Value*factor + newer.Value,
		T:     newT,
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
// which is not a defensible reading of "the value as of now".
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
