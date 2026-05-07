package compose_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	cases := []compose.Decayed{
		{}, // identity
		{Value: 1.0, T: time.Now().UnixNano(), Set: true}, // typical
		{Value: -3.14, T: 0, Set: true},                   // negative + zero ts
		{Value: math.MaxFloat64 / 2, T: 1, Set: true},     // large value
	}
	for _, want := range cases {
		got := compose.DecodeDecayed(compose.EncodeDecayed(want))
		if got.Value != want.Value || got.T != want.T || got.Set != want.Set {
			t.Errorf("round-trip: got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeDecayed_ShortInput(t *testing.T) {
	if got := compose.DecodeDecayed(nil); got.Set {
		t.Errorf("decode(nil): expected Identity, got %+v", got)
	}
	if got := compose.DecodeDecayed([]byte{1, 2, 3}); got.Set {
		t.Errorf("decode(short): expected Identity, got %+v", got)
	}
}

func TestDecayedSumBytes_MatchesTypedMonoid(t *testing.T) {
	hl := 30 * time.Minute
	typed := compose.DecayedSum(hl)
	bytesM := compose.DecayedSumBytes(hl)

	t0 := time.Now()
	a := compose.DecayedAt(5, t0)
	b := compose.DecayedAt(7, t0.Add(15*time.Minute))
	c := compose.DecayedAt(3, t0.Add(40*time.Minute))

	wantValue := typed.Combine(typed.Combine(a, b), c).Value

	gotBytes := bytesM.Combine(bytesM.Combine(compose.EncodeDecayed(a), compose.EncodeDecayed(b)), compose.EncodeDecayed(c))
	gotValue := compose.DecodeDecayed(gotBytes).Value

	if math.Abs(wantValue-gotValue) > 1e-9 {
		t.Errorf("bytes path diverged from typed path: got %.6f, want %.6f", gotValue, wantValue)
	}
}

func TestDecayedSumBytes_Identity(t *testing.T) {
	m := compose.DecayedSumBytes(time.Hour)
	a := compose.EncodeDecayed(compose.DecayedAt(42, time.Now()))

	if got := compose.DecodeDecayed(m.Combine(m.Identity(), a)); got.Value != 42 {
		t.Errorf("Combine(Identity, a): got %v, want 42", got.Value)
	}
	if got := compose.DecodeDecayed(m.Combine(a, m.Identity())); got.Value != 42 {
		t.Errorf("Combine(a, Identity): got %v, want 42", got.Value)
	}
}

func TestDecayedBytes_Lift(t *testing.T) {
	t0 := time.Now()
	got := compose.DecodeDecayed(compose.DecayedBytes(99.5, t0))
	if !got.Set || got.Value != 99.5 || got.T != t0.UnixNano() {
		t.Errorf("DecayedBytes round-trip: %+v", got)
	}
}
