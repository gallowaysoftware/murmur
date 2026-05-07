// Package hll is a HyperLogLog implementation of monoid.Monoid[[]byte], suitable for
// approximate-cardinality aggregations.
//
// Backed by axiomhq/hyperloglog (HLL++, default precision 14, ~1.6% standard error).
// The marshaled form is the axiomhq encoding — bit-stable across versions of this
// library but NOT compatible with Valkey/Redis's PFADD/PFCOUNT/PFMERGE encoding. When
// Valkey is configured as a cache accelerator, conversion happens at the cache
// boundary (Phase 2). For DDB-stored sketches, all reads and writes use this
// encoding; cross-backend portability is preserved via the structural Kind dispatch.
package hll

import (
	"github.com/axiomhq/hyperloglog"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// HLL returns a structural HyperLogLog monoid. Combine merges sketches; Identity is the
// marshaled empty sketch.
//
// To feed an element into a pipeline using this monoid, the pipeline's value extractor
// should return Single(element) — a marshaled HLL containing exactly that one element.
// The streaming runtime then merges singletons into the running state via Combine.
func HLL() monoid.Monoid[[]byte] { return hllMonoid{} }

type hllMonoid struct{}

func (hllMonoid) Identity() []byte {
	b, _ := hyperloglog.New().MarshalBinary()
	return b
}

func (hllMonoid) Combine(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	}
	ha := hyperloglog.New()
	if err := ha.UnmarshalBinary(a); err != nil {
		return b
	}
	hb := hyperloglog.New()
	if err := hb.UnmarshalBinary(b); err != nil {
		return a
	}
	if err := ha.Merge(hb); err != nil {
		return a
	}
	out, _ := ha.MarshalBinary()
	return out
}

func (hllMonoid) Kind() monoid.Kind { return monoid.KindHLL }

// Single returns the marshaled HLL for a sketch containing exactly element. Use this
// from the pipeline's value extractor to lift a per-event element into a one-element
// sketch suitable for monoidal merge.
func Single(element []byte) []byte {
	h := hyperloglog.New()
	h.Insert(element)
	b, _ := h.MarshalBinary()
	return b
}

// Estimate returns the approximate cardinality of a marshaled HLL.
func Estimate(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, nil
	}
	h := hyperloglog.New()
	if err := h.UnmarshalBinary(b); err != nil {
		return 0, err
	}
	return h.Estimate(), nil
}
