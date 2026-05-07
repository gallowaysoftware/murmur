// Package projection provides bucket functions and transition detection
// for projecting Murmur counter state into search indices, dashboards,
// or any external system that benefits from a coarse, slow-moving view
// of a fast-moving counter.
//
// The canonical use case — see doc/search-integration.md — is the DDB
// Streams projector that watches Murmur's counter table and reindexes
// OpenSearch only on bucket transitions, cutting search-side write rate
// from "every event" to "every order-of-magnitude change."
//
// # Bucket function shapes
//
// LogBucket  — log10 mapping; right for popularity / view counts /
//
//	follower counts where most queries care about
//	order-of-magnitude.
//
// LinearBucket — fixed-width bands; right for ratings, reputation,
//
//	scores where small differences matter.
//
// ManualBucket — user-supplied breakpoints; right when business rules
//
//	define cutoffs ("verified at 10k followers").
//
// # Hysteresis
//
// Naïve bucket transitions thrash on documents oscillating around a
// boundary (e.g., a count that bounces between 999 and 1000 likes
// triggers a reindex on every flip). HysteresisBucket wraps a
// BucketFn with an upper-and-lower-band rule: only transition up
// when exceeding the boundary by `band` and only transition down when
// falling below by the same `band`. Bounds the projector's reindex
// rate at the cost of bucket-edge precision.
package projection

import (
	"math"
	"sort"
)

// BucketFn maps a counter value to its bucket ID. Typically a small
// non-negative integer. Required to be:
//
//   - Monotonic: v1 ≤ v2 ⟹ BucketFn(v1) ≤ BucketFn(v2). Search filters
//     and the projector both rely on this for correctness.
//   - Total: defined for every int64 in the workload's range, including
//     zero and negative values.
type BucketFn func(int64) int

// LogBucket maps a counter to its log10 bucket. Returns 0 for v < 1;
// floor(log10(v)) otherwise:
//
//	 0 → 0
//	 9 → 0
//	10 → 1
//	99 → 1
//	100 → 2
//	1k → 3
//	1M → 6
//	10M → 7
//
// A document going from 0 to 1M crosses 6 boundaries (10, 100, 1k, 10k,
// 100k, 1M); same shape as the bucketed-indexing pattern in
// doc/search-integration.md.
func LogBucket(v int64) int {
	if v < 1 {
		return 0
	}
	return int(math.Floor(math.Log10(float64(v))))
}

// LinearBucket returns a BucketFn that puts values into fixed-width
// bands of size `bandSize`. Useful for small-range scores (e.g., ratings
// 0–1000 with bandSize=100 → 10 buckets).
//
// Negative values map to negative bucket IDs (truncation toward zero
// follows Go's integer-divison semantics).
func LinearBucket(bandSize int64) BucketFn {
	if bandSize <= 0 {
		bandSize = 1
	}
	return func(v int64) int {
		return int(v / bandSize)
	}
}

// ManualBucket returns a BucketFn that maps values into bands defined by
// `breakpoints`, which must be sorted ascending. The bucket ID is the
// index of the first breakpoint > v (i.e., 0 for v < breakpoints[0],
// len(breakpoints) for v ≥ breakpoints[-1]).
//
// Example — verified-at-10k, influencer-at-100k, celebrity-at-1M:
//
//	bf := ManualBucket([]int64{10_000, 100_000, 1_000_000})
//	bf(0)        → 0  (regular)
//	bf(50_000)   → 1  (verified)
//	bf(500_000)  → 2  (influencer)
//	bf(5_000_000) → 3 (celebrity)
//
// Panics if breakpoints is unsorted — caller bug, not a runtime concern.
func ManualBucket(breakpoints []int64) BucketFn {
	if !sort.SliceIsSorted(breakpoints, func(i, j int) bool {
		return breakpoints[i] < breakpoints[j]
	}) {
		panic("projection.ManualBucket: breakpoints must be sorted ascending")
	}
	bps := make([]int64, len(breakpoints))
	copy(bps, breakpoints)
	return func(v int64) int {
		return sort.Search(len(bps), func(i int) bool { return bps[i] > v })
	}
}

// HysteresisBucket wraps an inner BucketFn with an upper-and-lower-band
// transition rule that suppresses bucket flips for values oscillating
// around a boundary.
//
// Without hysteresis, a counter alternating between 999 and 1000 (the
// LogBucket boundary at log10(1000) = 3) flips between bucket 2 and
// bucket 3 on every transition, triggering N reindexes for N flips.
//
// With hysteresis (band=10%):
//
//   - To transition UP from bucket B to B+1, the value must exceed the
//     B→B+1 boundary by `band` × (boundary value).
//   - To transition DOWN from bucket B+1 to B, the value must fall
//     below the boundary by the same `band`.
//   - Otherwise the bucket is "sticky" — it stays at the previous value.
//
// This makes the bucket function STATEFUL: the bucket ID at time t
// depends on the bucket ID at time t-1, not just the current value.
// Use the Detector type below to track this state in a projector.
//
// Cost: bounds the per-document reindex rate. A document oscillating
// within `band` of a boundary triggers AT MOST ONE transition on the
// way up and ONE on the way down, regardless of how many oscillations
// occur. The trade-off is bucket-edge precision: a value just above
// the boundary may be reported as the lower bucket if the document
// recently came up from below, and vice versa.
type HysteresisBucket struct {
	Inner BucketFn
	// Band is the fractional hysteresis width — 0.10 means a 10% margin
	// on either side of each boundary. Typical values: 0.05–0.20. Set
	// 0 to disable hysteresis (matches Inner exactly).
	Band float64
}

// Apply returns the new bucket given the previous bucket and current
// value. For first observation (no previous), pass prev = -1.
func (h HysteresisBucket) Apply(prev int, v int64) int {
	if h.Band <= 0 || prev < 0 {
		return h.Inner(v)
	}
	naive := h.Inner(v)
	switch {
	case naive == prev:
		return prev
	case naive > prev:
		// Transitioning up: the value must exceed the prev-boundary by Band.
		// Approximate the boundary as the smallest v where Inner(v) > prev,
		// found by binary search in a bounded range. For LogBucket this is
		// 10^(prev+1); for ManualBucket it's the (prev+1)-th breakpoint.
		// We don't have direct access to the boundary, so we apply the band
		// as a fraction of `v` itself — equivalent for monotone bucket
		// functions where the boundary scales linearly with the bucket ID.
		// For LogBucket specifically, this is approximate but conservative
		// (slightly stickier than ideal).
		threshold := int64(float64(v) / (1 + h.Band))
		if h.Inner(threshold) > prev {
			return naive
		}
		return prev
	default:
		// Transitioning down.
		threshold := int64(float64(v) * (1 + h.Band))
		if h.Inner(threshold) < prev {
			return naive
		}
		return prev
	}
}

// Transition is the projector's per-record decision: did the bucket
// change, and what is the new bucket?
type Transition struct {
	OldBucket int
	NewBucket int
	Changed   bool
}

// Detect returns the Transition for a (prev, new) value pair under the
// given BucketFn. For the hysteresis case, use HysteresisBucket.Apply
// directly instead — Detect doesn't carry hysteresis state.
//
// This is the building block for DDB-Streams projectors (or any change-
// data-capture consumer) that decode OldImage + NewImage and decide
// whether to emit a downstream update.
func Detect(bf BucketFn, oldV, newV int64) Transition {
	oldB := bf(oldV)
	newB := bf(newV)
	return Transition{
		OldBucket: oldB,
		NewBucket: newB,
		Changed:   oldB != newB,
	}
}
