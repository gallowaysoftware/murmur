package compose_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
)

// fold replays a stream of (amount, time) observations through m the way a
// streaming worker would: one Combine per event, against the running state.
func fold(m monoid.Monoid[compose.Decayed], events []compose.Decayed) compose.Decayed {
	state := m.Identity()
	for _, e := range events {
		state = m.Combine(state, e)
	}
	return state
}

func TestDecayedSum_FutureDatedEventDoesNotFreezeTheKey(t *testing.T) {
	// One event with a bogus timestamp used to end the key's life. Combine
	// adopts the newer timestamp as the reference frame, so a four-year skew
	// made the frame unreachable: 2^(-4y/24h) underflows to exactly zero, the
	// hundred real events already folded in were annihilated, and every event
	// that followed was itself the older operand and was annihilated in turn.
	// The key sat at 1.0 forever while the read path evaluated it to +Inf.
	const halfLife = 24 * time.Hour
	t0 := time.Unix(1_700_000_000, 0)

	events := make([]compose.Decayed, 0, 1101)
	for i := 0; i < 100; i++ {
		events = append(events, compose.DecayedAt(1, t0))
	}
	events = append(events, compose.DecayedAt(1, t0.Add(4*365*24*time.Hour)))
	for i := 0; i < 1000; i++ {
		events = append(events, compose.DecayedAt(1, t0.Add(time.Duration(i)*time.Second)))
	}

	t.Run("tight skew bound recovers nearly all the mass", func(t *testing.T) {
		m := compose.DecayedSum(halfLife,
			compose.WithClock(func() time.Time { return t0 }),
			compose.WithClockSkewBound(time.Second),
		)
		got := fold(m, events)

		if math.IsInf(got.Value, 0) || math.IsNaN(got.Value) {
			t.Fatalf("value is not finite: %v", got.Value)
		}
		// 1101 unit observations inside a 1000-second window at a 24h half-life
		// decay by well under 1%.
		if got.Value < 1_000 || got.Value > 1_101 {
			t.Errorf("value %.3f does not track the 1,101 real events — one skewed timestamp still dominates", got.Value)
		}
		if want := t0.Add(time.Second).UnixNano(); got.T > want {
			t.Errorf("reference frame ran away: T=%d, want no later than %d", got.T, want)
		}
	})

	t.Run("default skew bound keeps the key alive", func(t *testing.T) {
		// Default bound is two half-lives, so the frame lands 48h out and the
		// contributions that arrive before the wall clock catches up are worth
		// a quarter each. That is a bounded, self-healing cost; freezing at 1.0
		// was neither.
		m := compose.DecayedSum(halfLife, compose.WithClock(func() time.Time { return t0 }))
		got := fold(m, events)

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
}

func TestDecayedSum_SkewClampIsOptOut(t *testing.T) {
	// A pipeline that legitimately dates observations forward can turn the
	// clamp off, and then gets the raw reference-frame semantics.
	t0 := time.Unix(1_700_000_000, 0)
	future := t0.Add(90 * 24 * time.Hour)
	m := compose.DecayedSum(24*time.Hour,
		compose.WithClock(func() time.Time { return t0 }),
		compose.WithClockSkewBound(0),
	)
	got := m.Combine(compose.DecayedAt(1, t0), compose.DecayedAt(2, future))
	if got.T != future.UnixNano() {
		t.Errorf("with the clamp disabled the newer frame must be adopted: T=%d, want %d", got.T, future.UnixNano())
	}
}

func TestDecayedSum_IdleKeyStillDecaysNormally(t *testing.T) {
	// The counterpart the clamp must not break: a key that legitimately went
	// quiet for a long time. Its timestamps are in the PAST, so nothing is
	// clamped and the old mass decays away exactly as it should.
	const halfLife = time.Minute
	now := time.Unix(1_700_000_000, 0)
	m := compose.DecayedSum(halfLife, compose.WithClock(func() time.Time { return now }))

	stale := compose.DecayedAt(1_000_000, now.Add(-18*time.Hour))
	fresh := compose.DecayedAt(1, now)

	got := m.Combine(stale, fresh)
	if got.T != now.UnixNano() {
		t.Errorf("frame: got %d, want the fresh observation's %d", got.T, now.UnixNano())
	}
	if math.Abs(got.Value-1) > 1e-9 {
		t.Errorf("value %.6f: a million units from 1080 half-lives ago must be gone, leaving the fresh 1", got.Value)
	}
}

func TestEvaluateAt_BeforeTheReferenceTimeDoesNotScaleUp(t *testing.T) {
	// The read-side half. Un-decaying is always an over-estimate, and an
	// unbounded one: a value stamped four years ahead evaluated to +Inf, and a
	// year ahead to 7.5e109 — either way it outranks every honest score in the
	// index, permanently.
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
