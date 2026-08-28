package compose_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
)

func mustDecode(t *testing.T, b []byte) compose.Decayed {
	t.Helper()
	d, err := compose.DecodeDecayed(b)
	if err != nil {
		t.Fatalf("DecodeDecayed: %v", err)
	}
	return d
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	cases := []compose.Decayed{
		{}, // identity
		{Value: 1.0, T: time.Now().UnixNano(), Set: true}, // typical
		{Value: -3.14, T: 0, Set: true},                   // negative + zero ts
		{Value: math.MaxFloat64 / 2, T: 1, Set: true},     // large value
	}
	for _, want := range cases {
		got := mustDecode(t, compose.EncodeDecayed(want))
		if got.Value != want.Value || got.T != want.T || got.Set != want.Set {
			t.Errorf("round-trip: got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeDecayed_EmptyIsIdentity(t *testing.T) {
	// An absent DDB item reads back as no bytes at all; that is the identity,
	// not a failure.
	got, err := compose.DecodeDecayed(nil)
	if err != nil {
		t.Fatalf("decode(nil): unexpected error %v", err)
	}
	if got.Set {
		t.Errorf("decode(nil): expected Identity, got %+v", got)
	}
}

func TestDecodeDecayed_WrongLengthIsAnError(t *testing.T) {
	// Any length but 0 and 17 is somebody else's bytes. Decoding the first 17
	// of them yields a Set=true observation made of noise, which then merges
	// into the row and never comes out.
	for _, n := range []int{1, 3, 16, 18, 200} {
		got, err := compose.DecodeDecayed(make([]byte, n))
		if err == nil {
			t.Errorf("decode(%d bytes): want error, got %+v", n, got)
			continue
		}
		if !strings.Contains(err.Error(), "decayed decode") {
			t.Errorf("decode(%d bytes): error should name the format: %v", n, err)
		}
		if got.Set {
			t.Errorf("decode(%d bytes): must not return a usable value alongside an error: %+v", n, got)
		}
	}
}

func TestDecayedSumBytes_ForeignBlobIsReported(t *testing.T) {
	// A 200-byte HLL sketch is exactly the shape of accident this guards: two
	// pipelines pointed at one DDB row, or a pipeline whose monoid changed.
	// Before the length check it decoded to a Set=true value built out of the
	// HLL's first 17 bytes and merged as if it were a real observation.
	hllBlob := make([]byte, 200)
	for i := range hllBlob {
		hllBlob[i] = byte(i*7 + 3)
	}

	var errs []error
	m := compose.DecayedSumBytes(time.Hour, compose.WithDecodeErrorHandler(func(err error) {
		errs = append(errs, err)
	}))
	good := compose.DecayedBytes(42, time.Now())

	got := m.Combine(hllBlob, good)

	if len(errs) != 1 {
		t.Fatalf("decode errors reported: got %d, want 1 — a dropped operand must not be silent", len(errs))
	}
	if !strings.Contains(errs[0].Error(), "left") {
		t.Errorf("error should say WHICH operand failed: %v", errs[0])
	}
	if string(got) != string(good) {
		t.Fatalf("Combine must return the operand that decoded")
	}
	d := mustDecode(t, got)
	if d.Value != 42 {
		t.Errorf("surviving operand corrupted: got %+v, want Value=42", d)
	}

	// And the mirror image, so the report names the right side.
	errs = nil
	if got := m.Combine(good, hllBlob); string(got) != string(good) {
		t.Error("Combine must return the operand that decoded")
	}
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "right") {
		t.Errorf("right-operand failure not reported as such: %v", errs)
	}
}

func TestDecayedSumBytes_ForeignBlobWithoutHandlerStillRecovers(t *testing.T) {
	// The hook is optional; without one the recovery must be the same, just
	// unreported.
	m := compose.DecayedSumBytes(time.Hour)
	good := compose.DecayedBytes(7, time.Now())
	if got := m.Combine(make([]byte, 200), good); string(got) != string(good) {
		t.Error("Combine must still return the decodable operand")
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
	gotValue := mustDecode(t, gotBytes).Value

	if math.Abs(wantValue-gotValue) > 1e-9 {
		t.Errorf("bytes path diverged from typed path: got %.6f, want %.6f", gotValue, wantValue)
	}
}

func TestDecayedSumBytes_Identity(t *testing.T) {
	m := compose.DecayedSumBytes(time.Hour)
	a := compose.EncodeDecayed(compose.DecayedAt(42, time.Now()))

	if got := mustDecode(t, m.Combine(m.Identity(), a)); got.Value != 42 {
		t.Errorf("Combine(Identity, a): got %v, want 42", got.Value)
	}
	if got := mustDecode(t, m.Combine(a, m.Identity())); got.Value != 42 {
		t.Errorf("Combine(a, Identity): got %v, want 42", got.Value)
	}
}

func TestDecayedBytes_Lift(t *testing.T) {
	t0 := time.Now()
	got := mustDecode(t, compose.DecayedBytes(99.5, t0))
	if !got.Set || got.Value != 99.5 || got.T != t0.UnixNano() {
		t.Errorf("DecayedBytes round-trip: %+v", got)
	}
}
