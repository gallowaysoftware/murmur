// Package bloom is a Bloom filter implementation of monoid.Monoid[[]byte], suitable for
// approximate set-membership aggregations.
//
// Backed by github.com/bits-and-blooms/bloom/v3. Combine is bitwise-OR of the two
// underlying bit arrays. All sketches in a pipeline must share the same (m, k)
// parameters — a bitwise-OR of two differently-sized bit arrays is not defined, so
// Combine refuses to merge mismatched shapes: it returns the left operand and reports
// the mismatch through WithDecodeErrorHandler.
//
// Identity is the empty byte slice, deliberately not a marshaled empty filter. The
// marshaled form carries (m, k), so an Identity that had a shape was only an identity
// for filters of its own shape: a pipeline whose monoid was built with NewWithCapacity
// but whose value extractor called the default-sized Single merged every event into a
// shape mismatch, and the row stayed the empty filter forever.
//
// Default parameters: 1M bits, k=7 hash functions, ~1% false-positive rate at ~100K
// inserts. Use NewWithCapacity for custom sizing.
package bloom

import (
	"bytes"
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Default capacity and FPR. Yields ~10K-bit filter at p=0.01, k=7.
const (
	DefaultCapacity = 100_000
	DefaultFPR      = 0.01
)

// Bloom returns a Bloom-filter monoid with default parameters. To track a different
// expected cardinality / FPR, use NewWithCapacity.
func Bloom(opts ...Option) monoid.Monoid[[]byte] {
	return NewWithCapacity(DefaultCapacity, DefaultFPR, opts...)
}

// NewWithCapacity returns a Bloom-filter monoid sized for the expected number of
// distinct elements n and target false-positive rate p. The shape is fixed for the
// lifetime of the monoid; all sketches it merges must share these parameters.
func NewWithCapacity(n uint, p float64, opts ...Option) monoid.Monoid[[]byte] {
	probe := bloom.NewWithEstimates(n, p)
	mBits := probe.Cap()
	kHashes := probe.K()
	bm := bloomMonoid{n: n, p: p, m: mBits, k: kHashes}
	for _, o := range opts {
		o(&bm)
	}
	return bm
}

// Option configures the Bloom monoid.
type Option func(*bloomMonoid)

// WithDecodeErrorHandler installs a callback invoked when Combine cannot
// decode one of its operands, or when the two operands have incompatible
// (m, k) shapes.
//
// Combine returns no error — the contract is Combine(a, b) V — so the only
// recovery on a decode failure is to return the operand that DID decode,
// silently discarding the other. That recovery is deliberate; doing it
// silently is not. For Bloom the silent case is especially easy to hit: every
// sketch merged must share the (m, k) shape, so a single caller constructing
// the monoid with different capacity parameters produces filters that cannot
// be OR'd together — and without this hook, membership answers just quietly go
// wrong.
//
// The shape mismatch is the case this hook's doc always claimed to cover and
// never did: (m, k) is written into the marshaled form and read back out of
// it, so mismatched filters both decode fine and the merge was abandoned with
// no error at all.
//
// The handler must be cheap and non-blocking; it runs on the merge path.
func WithDecodeErrorHandler(fn func(error)) Option {
	return func(m *bloomMonoid) { m.onDecodeErr = fn }
}

type bloomMonoid struct {
	n           uint
	p           float64
	m           uint
	k           uint
	onDecodeErr func(error)
}

func (bm bloomMonoid) reportDecodeError(err error) {
	if bm.onDecodeErr != nil {
		bm.onDecodeErr(err)
	}
}

// Identity is the empty slice. Combine already short-circuits a zero-length
// operand, so this is a two-sided identity for a filter of ANY shape, which a
// marshaled empty filter was not: it carried (m, k), and merging it with a
// differently-shaped operand hit the mismatch path and returned the empty
// filter — permanently, since the stored row then stayed empty for every
// subsequent merge.
//
// It also keeps a window with no data from costing 120 KB of zero bits per
// bucket on the query path.
func (bloomMonoid) Identity() []byte { return nil }

func (bm bloomMonoid) Combine(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	}
	ba := bloom.New(bm.m, bm.k)
	if err := ba.UnmarshalBinary(a); err != nil {
		// Keep the operand that decoded; report the one that didn't.
		bm.reportDecodeError(fmt.Errorf("bloom: decode left operand (%d bytes): %w", len(a), err))
		return b
	}
	bb := bloom.New(bm.m, bm.k)
	if err := bb.UnmarshalBinary(b); err != nil {
		bm.reportDecodeError(fmt.Errorf("bloom: decode right operand (%d bytes): %w", len(b), err))
		return a
	}
	// Cap or K mismatch: return left operand. In practice this happens when sketches
	// were created with different parameters — a configuration bug we surface by
	// returning unmodified data rather than corrupting state.
	//
	// UnmarshalBinary reads (m, k) off the wire and overwrites whatever shape we
	// constructed the filter with, so both operands decode cleanly and this is the
	// only place the misconfiguration is visible. It used to return here in silence,
	// which is how a pipeline could drop every event for the lifetime of a row.
	if ba.Cap() != bb.Cap() || ba.K() != bb.K() {
		bm.reportDecodeError(fmt.Errorf(
			"bloom: shape mismatch, cannot merge: left (m=%d, k=%d), right (m=%d, k=%d) — monoid built for (m=%d, k=%d); keeping the left operand",
			ba.Cap(), ba.K(), bb.Cap(), bb.K(), bm.m, bm.k))
		return a
	}
	merged := ba.Copy()
	if err := merged.Merge(bb); err != nil {
		bm.reportDecodeError(fmt.Errorf("bloom: merge (m=%d, k=%d): %w", ba.Cap(), ba.K(), err))
		return a
	}
	out, _ := merged.MarshalBinary()
	return out
}

func (bloomMonoid) Kind() monoid.Kind { return monoid.KindBloom }

// Single returns the marshaled Bloom filter containing exactly element, sized with the
// default capacity. Use NewSingle for non-default sizing.
func Single(element []byte) []byte {
	return NewSingle(DefaultCapacity, DefaultFPR, element)
}

// NewSingle returns a marshaled Bloom filter sized for (n, p) and pre-populated with
// the given element. The pipeline's value extractor uses this to lift a per-event
// element into a one-element sketch suitable for monoidal merge.
func NewSingle(n uint, p float64, element []byte) []byte {
	bf := bloom.NewWithEstimates(n, p)
	bf.Add(element)
	b, _ := bf.MarshalBinary()
	return b
}

// Contains reports whether element is (probably) in the marshaled filter. Returns false
// on decode error.
func Contains(sketch, element []byte) bool {
	if len(sketch) == 0 {
		return false
	}
	bf := bloom.New(1, 1) // shape is read from the marshaled form
	if err := bf.UnmarshalBinary(sketch); err != nil {
		return false
	}
	return bf.Test(element)
}

// Equal reports whether two marshaled filters have identical bits and shape. Used in
// tests for determinism checks.
func Equal(a, b []byte) bool { return bytes.Equal(a, b) }

// Inspect returns the (capacity m, hash count k, approximate number of inserted
// elements) triple from a marshaled filter. Used by the admin server to render
// a human-readable view of an opaque sketch in the Query Console.
//
// "Approximate size" comes from the bits-and-blooms library's estimator —
// derived from the bit-fill ratio against (m, k); accurate within a few percent
// at typical fill levels, less reliable as the filter approaches saturation.
func Inspect(sketch []byte) (capacity uint64, hashes uint32, approxSize uint64, err error) {
	if len(sketch) == 0 {
		return 0, 0, 0, nil
	}
	bf := bloom.New(1, 1) // shape is read from the marshaled form
	if err := bf.UnmarshalBinary(sketch); err != nil {
		return 0, 0, 0, err
	}
	return uint64(bf.Cap()), uint32(bf.K()), uint64(bf.ApproximatedSize()), nil
}
