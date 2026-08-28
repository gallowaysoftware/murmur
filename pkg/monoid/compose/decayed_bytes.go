package compose

import (
	"encoding/binary"
	"fmt"
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

// DecodeDecayed parses the 17-byte wire form back into a Decayed.
//
// An empty input is the absent key and decodes to Identity with no error —
// that is what a DDB read of a missing item yields. Any other length is a
// foreign blob and is an error: the format has no magic and no length prefix,
// so a 200-byte HLL sketch or a Bloom filter used to decode to a Set=true
// observation assembled from its first 17 bytes, and that fabricated value
// then merged into the row and stayed there.
func DecodeDecayed(b []byte) (Decayed, error) {
	switch {
	case len(b) == 0:
		return Decayed{}, nil
	case len(b) != decayedWireSize:
		return Decayed{}, fmt.Errorf("decayed decode: got %d bytes, want %d", len(b), decayedWireSize)
	}
	return Decayed{
		Value: math.Float64frombits(binary.LittleEndian.Uint64(b[0:8])),
		T:     int64(binary.LittleEndian.Uint64(b[8:16])),
		Set:   b[16] != 0,
	}, nil
}

// DecayedSumBytes wraps DecayedSum to operate on []byte values, suitable
// for plugging into pkg/state/dynamodb.NewBytesStore. Each Combine call
// decodes both sides, runs the typed Combine, and re-encodes; the cost is
// a few dozen ns per merge — negligible compared to a DDB round-trip.
//
// The wire format is the same as EncodeDecayed / DecodeDecayed; queries
// can decode the bytes returned by GetWindow / GetRange and evaluate the
// score "as of now" via EvaluateAt(d, halfLife, time.Now()).
//
// Pass WithDecodeErrorHandler to be told when an operand is not a Decayed at
// all; without it the mismatch is recovered from silently, the same deal as
// the sketch monoids.
func DecayedSumBytes(halfLife time.Duration, opts ...Option) monoid.Monoid[[]byte] {
	cfg := newDecayedConfig(halfLife, opts)
	return decayedBytesMonoid{cfg: cfg, inner: decayedMonoid{cfg: cfg}}
}

type decayedBytesMonoid struct {
	cfg   decayedConfig
	inner decayedMonoid
}

func (decayedBytesMonoid) Identity() []byte {
	return EncodeDecayed(Decayed{}) // 17 zero bytes
}

func (m decayedBytesMonoid) Combine(a, b []byte) []byte {
	da, errA := DecodeDecayed(a)
	db, errB := DecodeDecayed(b)
	if errA != nil {
		// Keep the operand that decoded; report the one that didn't.
		m.cfg.reportDecodeError(fmt.Errorf("decayed: decode left operand: %w", errA))
		return b
	}
	if errB != nil {
		m.cfg.reportDecodeError(fmt.Errorf("decayed: decode right operand: %w", errB))
		return a
	}
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
