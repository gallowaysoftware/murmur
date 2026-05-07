package compose

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Decayed wire format: 17 bytes.
//
//	[ 8 bytes: float64 Value, little-endian IEEE-754 ]
//	[ 8 bytes: int64   T,     little-endian          ]
//	[ 1 byte:  uint8   Set    (0 or 1)               ]
//
// Designed to be small (DDB ItemSize matters) and self-describing enough
// that future schema additions can prepend a version byte without
// invalidating existing rows.
const decayedWireSize = 17

// EncodeDecayed marshals a Decayed observation to its 17-byte wire form.
// Identity (Set=false) encodes as zeros, which is intentional — DDB
// `attribute_not_exists` reads return that shape and DecodeDecayed maps it
// back to Identity.
func EncodeDecayed(d Decayed) []byte {
	b := make([]byte, decayedWireSize)
	binary.LittleEndian.PutUint64(b[0:8], math.Float64bits(d.Value))
	binary.LittleEndian.PutUint64(b[8:16], uint64(d.T))
	if d.Set {
		b[16] = 1
	}
	return b
}

// DecodeDecayed parses a 17-byte wire form back into a Decayed. Returns
// Identity (Set=false) on a short or empty input — both are treated as
// "no observation yet."
func DecodeDecayed(b []byte) Decayed {
	if len(b) < decayedWireSize {
		return Decayed{}
	}
	return Decayed{
		Value: math.Float64frombits(binary.LittleEndian.Uint64(b[0:8])),
		T:     int64(binary.LittleEndian.Uint64(b[8:16])),
		Set:   b[16] != 0,
	}
}

// DecayedSumBytes wraps DecayedSum to operate on []byte values, suitable
// for plugging into pkg/state/dynamodb.NewBytesStore. Each Combine call
// decodes both sides, runs the typed Combine, and re-encodes; the cost is
// a few dozen ns per merge — negligible compared to a DDB round-trip.
//
// The wire format is the same as EncodeDecayed / DecodeDecayed; queries
// can decode the bytes returned by GetWindow / GetRange and evaluate the
// score "as of now" via EvaluateAt(d, halfLife, time.Now()).
func DecayedSumBytes(halfLife time.Duration) monoid.Monoid[[]byte] {
	return decayedBytesMonoid{inner: decayedMonoid{halfLife: halfLife.Seconds()}}
}

type decayedBytesMonoid struct {
	inner decayedMonoid
}

func (decayedBytesMonoid) Identity() []byte {
	return EncodeDecayed(Decayed{}) // 17 zero bytes
}

func (m decayedBytesMonoid) Combine(a, b []byte) []byte {
	da := DecodeDecayed(a)
	db := DecodeDecayed(b)
	merged := m.inner.Combine(da, db)
	return EncodeDecayed(merged)
}

func (decayedBytesMonoid) Kind() monoid.Kind { return monoid.KindCustom }

// DecayedBytes lifts a single (amount, time) observation to the wire form
// expected by DecayedSumBytes pipelines. Pipelines call this from their
// value extractor:
//
//	Value(func(e Event) []byte {
//	    return compose.DecayedBytes(weight(e), e.At)
//	})
func DecayedBytes(amount float64, t time.Time) []byte {
	return EncodeDecayed(DecayedAt(amount, t))
}

// DecayedBytesNow is DecayedBytes(amount, time.Now()).
func DecayedBytesNow(amount float64) []byte {
	return DecayedBytes(amount, time.Now())
}
