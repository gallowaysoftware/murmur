// Package bloom is a Bloom filter implementation of monoid.Monoid[[]byte], suitable for
// approximate set-membership aggregations.
//
// Backed by github.com/bits-and-blooms/bloom/v3. Combine is bitwise-OR of the two
// underlying bit arrays; Identity is an empty bit array of the same size. All sketches
// in a pipeline must share the same (m, k) parameters — Combine refuses to merge
// sketches with mismatched shapes (returns the left operand on mismatch).
//
// Default parameters: 1M bits, k=7 hash functions, ~1% false-positive rate at ~100K
// inserts. Use NewWithCapacity for custom sizing.
package bloom

import (
	"bytes"

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
func Bloom() monoid.Monoid[[]byte] {
	return NewWithCapacity(DefaultCapacity, DefaultFPR)
}

// NewWithCapacity returns a Bloom-filter monoid sized for the expected number of
// distinct elements n and target false-positive rate p. The shape is fixed for the
// lifetime of the monoid; all sketches it merges must share these parameters.
func NewWithCapacity(n uint, p float64) monoid.Monoid[[]byte] {
	probe := bloom.NewWithEstimates(n, p)
	mBits := probe.Cap()
	kHashes := probe.K()
	return bloomMonoid{n: n, p: p, m: mBits, k: kHashes}
}

type bloomMonoid struct {
	n uint
	p float64
	m uint
	k uint
}

func (bm bloomMonoid) Identity() []byte {
	bf := bloom.New(bm.m, bm.k)
	b, _ := bf.MarshalBinary()
	return b
}

func (bm bloomMonoid) Combine(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	}
	ba := bloom.New(bm.m, bm.k)
	if err := ba.UnmarshalBinary(a); err != nil {
		return b
	}
	bb := bloom.New(bm.m, bm.k)
	if err := bb.UnmarshalBinary(b); err != nil {
		return a
	}
	// Cap or K mismatch: return left operand. In practice this happens when sketches
	// were created with different parameters — a configuration bug we surface by
	// returning unmodified data rather than corrupting state.
	if ba.Cap() != bb.Cap() || ba.K() != bb.K() {
		return a
	}
	merged := ba.Copy()
	if err := merged.Merge(bb); err != nil {
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
