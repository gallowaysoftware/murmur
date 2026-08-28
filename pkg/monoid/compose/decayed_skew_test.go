package compose_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
)

// fold replays a stream of observations through m the way a streaming worker
// would: one Combine per event, against the running state.
func fold(m monoid.Monoid[compose.Decayed], events []compose.Decayed) compose.Decayed {
	state := m.Identity()
	for _, e := range events {
		state = m.Combine(state, e)
	}
	return state
}

func TestDecayedSum_CombineIsPure(t *testing.T) {
	// Combine must be a function of its operands and nothing else.
	//
	// dynamodb.BytesStore.MergeUpdate recomputes Combine on every CAS retry, so
	// a Combine that read the wall clock produced a different answer on the
	// second attempt than on the first — which value got written then depended
	// on how many times the conditional write lost a race. The same impurity
	// makes the associativity fuzzer in pkg/monoid/monoidlaws meaningless,
	// because the two groupings are evaluated at different instants.
	//
	// A far-future operand is the case that exposed it: that is the one the
	// clock-based skew clamp used to rewrite.
	const halfLife = 24 * time.Hour
	t0 := time.Unix(1_700_000_000, 0)
	m := compose.DecayedSum(halfLife)

	a := compose.DecayedAt(100, t0)
	b := compose.DecayedAt(1, t0.Add(4*365*24*time.Hour))
	c := compose.DecayedAt(7, t0.Add(time.Second))

	want := m.Combine(a, b)
	for i := 0; i < 2_000; i++ {
		if got := m.Combine(a, b); got != want {
			t.Fatalf("Combine is not pure: call %d returned %+v, first call returned %+v", i, got, want)
		}
	}

	// The same property stated the way the law fuzzer sees it: both groupings
	// have to agree, and they have to keep agreeing when evaluated again later.
	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))
	if left.T != right.T {
		t.Errorf("associativity (frame): (a·b)·c has T=%d, a·(b·c) has T=%d", left.T, right.T)
	}
	if math.Abs(left.Value-right.Value) > 1e-9 {
		t.Errorf("associativity (value): (a·b)·c = %v, a·(b·c) = %v", left.Value, right.Value)
	}
	if again := m.Combine(m.Combine(a, b), c); again != left {
		t.Errorf("re-evaluating the same expression changed it: %+v then %+v", left, again)
	}
}

func TestDecayedSum_CombineAdoptsTheNewerFrameExactly(t *testing.T) {
	// Combine's contract is that the output frame is max(a.T, b.T) — exactly,
	// with no rewriting. A clock-based clamp inside Combine broke this for
	// future-dated operands, and it could not be fixed by clamping against the
	// OTHER operand instead: the "idle key" row below is the counterexample.
	// From inside Combine those two rows are the same shape, so the only sound
	// thing Combine can do is take the timestamps at face value. The bound
	// belongs at the lift (see TestClampFuture_*).
	t0 := time.Unix(1_700_000_000, 0)

	cases := []struct {
		name      string
		halfLife  time.Duration
		a, b      compose.Decayed
		wantT     time.Time
		wantValue float64
	}{
		{
			// A key that legitimately went quiet for 1,080 half-lives. Its old
			// mass must be gone. Any pairwise "don't decay across more than N
			// half-lives" bound would hand back a quarter of it instead.
			name:      "idle key decays to nothing",
			halfLife:  time.Minute,
			a:         compose.DecayedAt(1_000_000, t0.Add(-18*time.Hour)),
			b:         compose.DecayedAt(1, t0),
			wantT:     t0,
			wantValue: 1,
		},
		{
			name:      "same instant is a plain sum",
			halfLife:  24 * time.Hour,
			a:         compose.DecayedAt(10, t0),
			b:         compose.DecayedAt(5, t0),
			wantT:     t0,
			wantValue: 15,
		},
		{
			name:      "one half-life apart halves the older operand",
			halfLife:  24 * time.Hour,
			a:         compose.DecayedAt(8, t0),
			b:         compose.DecayedAt(1, t0.Add(24*time.Hour)),
			wantT:     t0.Add(24 * time.Hour),
			wantValue: 5,
		},
		{
			// The case a clock-based clamp used to rewrite. Combine takes it at
			// face value; nothing here is allowed to depend on what time it is
			// when the test runs.
			name:      "far-future operand is adopted verbatim",
			halfLife:  24 * time.Hour,
			a:         compose.DecayedAt(100, t0),
			b:         compose.DecayedAt(1, t0.Add(4*365*24*time.Hour)),
			wantT:     t0.Add(4 * 365 * 24 * time.Hour),
			wantValue: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := compose.DecayedSum(tc.halfLife)
			for _, got := range []compose.Decayed{m.Combine(tc.a, tc.b), m.Combine(tc.b, tc.a)} {
				if got.T != tc.wantT.UnixNano() {
					t.Errorf("frame: got %d, want max of the operands %d", got.T, tc.wantT.UnixNano())
				}
				if math.Abs(got.Value-tc.wantValue) > 1e-9 {
					t.Errorf("value: got %v, want %v", got.Value, tc.wantValue)
				}
			}
		})
	}
}

func TestClampFuture_BoundsOnlyTheFuture(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const bound = 48 * time.Hour

	cases := []struct {
		name string
		t    time.Time
		want time.Time
	}{
		{"long past is untouched", now.Add(-4 * 365 * 24 * time.Hour), now.Add(-4 * 365 * 24 * time.Hour)},
		{"now is untouched", now, now},
		{"inside the bound is untouched", now.Add(47 * time.Hour), now.Add(47 * time.Hour)},
		{"exactly at the bound is untouched", now.Add(bound), now.Add(bound)},
		{"just past the bound is pulled back", now.Add(bound + time.Nanosecond), now.Add(bound)},
		{"four years out is pulled back", now.Add(4 * 365 * 24 * time.Hour), now.Add(bound)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := compose.ClampFuture(tc.t, now, bound); !got.Equal(tc.want) {
				t.Errorf("ClampFuture: got %s, want %s", got, tc.want)
			}
		})
	}

	// A non-positive bound is the opt-out, for a pipeline that legitimately
	// dates observations forward.
	far := now.Add(4 * 365 * 24 * time.Hour)
	for _, b := range []time.Duration{0, -time.Hour} {
		if got := compose.ClampFuture(far, now, b); !got.Equal(far) {
			t.Errorf("bound=%s must disable the clamp: got %s, want %s", b, got, far)
		}
	}

	// The default bound is two half-lives, and it does not overflow for a
	// half-life past half the Duration range.
	if got, want := compose.DefaultSkewBound(24*time.Hour), 48*time.Hour; got != want {
		t.Errorf("DefaultSkewBound(24h): got %s, want %s", got, want)
	}
	if got := compose.DefaultSkewBound(time.Duration(math.MaxInt64)); got <= 0 {
		t.Errorf("DefaultSkewBound must not overflow into a non-positive bound: got %s", got)
	}
	if got := compose.DefaultSkewBound(0); got != 0 {
		t.Errorf("DefaultSkewBound(0): got %s, want 0 — no decay, no frame to protect", got)
	}
}

func TestDecayedSum_ClampFutureAtTheLiftKeepsTheKeyAlive(t *testing.T) {
	// One event with a bogus timestamp used to end the key's life. Combine
	// adopts the newer timestamp as the reference frame, so a four-year skew
	// made the frame unreachable: 2^(-4y/24h) underflows to exactly zero, the
	// hundred real events already folded in were annihilated, and every event
	// that followed was itself the older operand and was annihilated in turn.
	// The key sat at 1.0 forever while the read path evaluated it to +Inf.
	//
	// Clamping at the lift is what stops it, so this test lifts the way a
	// pipeline reading event-time timestamps has to.
	const halfLife = 24 * time.Hour
	t0 := time.Unix(1_700_000_000, 0)
	// A fixed "wall clock" for the ingest side. Every event below is either at
	// t0 or shortly after, so this stands in for a worker running live.
	now := t0

	raw := make([]time.Time, 0, 1101)
	for i := 0; i < 100; i++ {
		raw = append(raw, t0)
	}
	raw = append(raw, t0.Add(4*365*24*time.Hour)) // the skewed one
	for i := 0; i < 1000; i++ {
		raw = append(raw, t0.Add(time.Duration(i)*time.Second))
	}

	lift := func(bound time.Duration) []compose.Decayed {
		out := make([]compose.Decayed, 0, len(raw))
		for _, ts := range raw {
			out = append(out, compose.DecayedAt(1, compose.ClampFuture(ts, now, bound)))
		}
		return out
	}

	m := compose.DecayedSum(halfLife)

	t.Run("tight bound recovers nearly all the mass", func(t *testing.T) {
		got := fold(m, lift(time.Second))

		if math.IsInf(got.Value, 0) || math.IsNaN(got.Value) {
			t.Fatalf("value is not finite: %v", got.Value)
		}
		// 1,101 unit observations inside a 1,000-second window at a 24h
		// half-life decay by well under 1%.
		if got.Value < 1_000 || got.Value > 1_101 {
			t.Errorf("value %.3f does not track the 1,101 real events — one skewed timestamp still dominates", got.Value)
		}
		if want := t0.Add(1000 * time.Second).UnixNano(); got.T > want {
			t.Errorf("reference frame ran away: T=%d, want no later than %d", got.T, want)
		}
	})

	t.Run("default bound keeps the key alive", func(t *testing.T) {
		// Two half-lives, so the skewed event lands 48h out and the
		// contributions that arrive before the wall clock catches up are worth
		// a quarter each. That is a bounded, self-healing cost; freezing at 1.0
		// was neither.
		got := fold(m, lift(compose.DefaultSkewBound(halfLife)))

		if math.IsInf(got.Value, 0) || math.IsNaN(got.Value) {
			t.Fatalf("value is not finite: %v", got.Value)
		}
		if got.Value < 100 {
			t.Errorf("value %.3f: the later events never registered", got.Value)
		}
		if limit := t0.Add(2 * halfLife).UnixNano(); got.T > limit {
			t.Errorf("reference frame ran away: T=%d, want no later than now+2*halfLife=%d", got.T, limit)
		}
	})

	t.Run("unclamped is the failure this guards", func(t *testing.T) {
		// The unclamped fold is kept as an executable statement of what goes
		// wrong, so the two branches above are not just asserting numbers with
		// nothing to contrast against.
		got := fold(m, lift(0))
		if got.Value > 2 {
			t.Fatalf("fixture no longer reproduces the freeze: value %v", got.Value)
		}
		if got.T != t0.Add(4*365*24*time.Hour).UnixNano() {
			t.Fatalf("fixture no longer reproduces the runaway frame: T=%d", got.T)
		}
	})
}

func TestEvaluateAt_BeforeTheReferenceTimeDoesNotScaleUp(t *testing.T) {
	// The read-side half, and the backstop for a row that was already frozen
	// before ClampFuture was in the pipeline. Un-decaying is always an
	// over-estimate, and an unbounded one: a value stamped four years ahead
	// evaluated to +Inf, and a year ahead to 7.5e109 — either way it outranks
	// every honest score in the index, permanently.
	const halfLife = 24 * time.Hour
	t0 := time.Unix(1_700_000_000, 0)
	d := compose.DecayedAt(1, t0)

	for _, back := range []time.Duration{
		4 * 365 * 24 * time.Hour,
		365 * 24 * time.Hour,
		time.Hour,
	} {
		got := compose.EvaluateAt(d, halfLife, t0.Add(-back))
		if math.IsInf(got, 0) || math.IsNaN(got) {
			t.Errorf("EvaluateAt(d.T-%s): got %v, want a finite value", back, got)
			continue
		}
		if got != d.Value {
			t.Errorf("EvaluateAt(d.T-%s): got %v, want the stored %v — evaluating before the reference time must not inflate it", back, got, d.Value)
		}
	}

	// Forward in time is unchanged: still a real decay.
	if got := compose.EvaluateAt(d, halfLife, t0.Add(halfLife)); math.Abs(got-0.5) > 1e-9 {
		t.Errorf("EvaluateAt one half-life on: got %v, want 0.5", got)
	}
}

func TestDecayedSum_NonPositiveHalfLifeIsAPlainSum(t *testing.T) {
	// halfLife=0 is reachable by accident — murmur.Trending(name, cfg.HalfLife)
	// with an unset Duration field — and Combine and EvaluateAt used to
	// implement opposite semantics for it. Combine computed 2^(-dt/0): NaN at
	// dt=0, which round-trips through Encode/Decode and poisons the row for
	// good, and 0 at dt>0, which silently dropped every older contribution.
	// EvaluateAt meanwhile returned the value undecayed. Both now mean "no
	// decay".
	t0 := time.Unix(1_700_000_000, 0)

	for _, hl := range []time.Duration{0, -time.Hour} {
		m := compose.DecayedSum(hl)

		same := m.Combine(compose.DecayedAt(10, t0), compose.DecayedAt(5, t0))
		if math.IsNaN(same.Value) || math.IsInf(same.Value, 0) {
			t.Errorf("halfLife=%s, dt=0: got %v, want a finite 15", hl, same.Value)
		} else if same.Value != 15 {
			t.Errorf("halfLife=%s, dt=0: got %v, want the plain sum 15", hl, same.Value)
		}

		apart := m.Combine(compose.DecayedAt(10, t0), compose.DecayedAt(5, t0.Add(time.Second)))
		if apart.Value != 15 {
			t.Errorf("halfLife=%s, dt=1s: got %v, want 15 — the older contribution must not be dropped", hl, apart.Value)
		}

		grown := m.Combine(compose.DecayedAt(10, t0), compose.DecayedAt(5, t0.Add(time.Hour)))
		if grown.Value != 15 {
			t.Errorf("halfLife=%s, dt=1h: got %v, want 15 — a negative half-life must not make the value grow", hl, grown.Value)
		}

		// And the read path agrees with the merge path about the same row.
		if got := compose.EvaluateAt(apart, hl, t0.Add(time.Hour)); got != apart.Value {
			t.Errorf("halfLife=%s: EvaluateAt %v disagrees with the stored %v", hl, got, apart.Value)
		}
	}
}

func TestDecayedSumBytes_NaNDoesNotSurviveAZeroHalfLife(t *testing.T) {
	// The bytes path is where a NaN became permanent: 17 bytes of NaN encode
	// and decode cleanly, so once written the row stayed NaN through every
	// subsequent merge.
	m := compose.DecayedSumBytes(0)
	t0 := time.Unix(1_700_000_000, 0)

	state := m.Combine(compose.DecayedBytes(10, t0), compose.DecayedBytes(5, t0))
	for i := 0; i < 5; i++ {
		state = m.Combine(state, compose.DecayedBytes(1, t0))
	}
	got, err := compose.DecodeDecayed(state)
	if err != nil {
		t.Fatalf("DecodeDecayed: %v", err)
	}
	if math.IsNaN(got.Value) {
		t.Fatalf("row is NaN and will stay NaN for every future merge")
	}
	if got.Value != 20 {
		t.Errorf("value: got %v, want 20", got.Value)
	}
}
