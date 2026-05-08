package streaming

import (
	"fmt"
	"time"
)

// valueDebouncer drops records whose (entityKey, value) pair was already
// seen within `window`. Implemented on top of the same bounded-map
// infrastructure as keyDebouncer, with a composite key that includes a
// fingerprint of the value.
//
// **Different from keyDebouncer.**
//
//   - keyDebouncer drops by ENTITY KEY alone — assumes any record on
//     the same key carries the same logical value (the cache-fill
//     pattern: "this hot key got hit again, ignore the redundant
//     cache-fill").
//   - valueDebouncer drops only when the VALUE also matches — safe
//     even when the same key emits different values over time. A
//     value change on the same key always passes through.
//
// **Use case.** Services that re-emit absolute-value records on every
// downstream change notification, but most events carry the SAME value
// as the previous emission. Mongo CDC's change-stream is the canonical
// example: it fires for every document touch, including no-op updates
// where no real field changed.
type valueDebouncer struct {
	inner *keyDebouncer
}

func newValueDebouncer(window time.Duration, maxKeys int) *valueDebouncer {
	return &valueDebouncer{inner: newKeyDebouncer(window, maxKeys)}
}

// shouldDrop returns true if (entityKey, valueFingerprint) was seen
// within the window. The fingerprint is a string the caller has
// computed from the value (see fingerprintValue). The composite key
// uses NUL as a separator since keys generally don't contain it.
func (d *valueDebouncer) shouldDrop(entityKey, valueFingerprint string) bool {
	if entityKey == "" {
		return false
	}
	return d.inner.shouldDrop(entityKey + "\x00" + valueFingerprint)
}

// fingerprintValue returns a stable string representation of v suitable
// for value-debounce comparison. fmt.Sprintf("%v", v) handles all V
// (basic types, structs, []byte) and produces equal output for equal
// values — that's the only contract value-debounce needs.
//
// Cost: one allocation per record. For most pipelines this is well
// under the per-record store-write or Combine cost. For pipelines where
// Sprintf is the hot-path bottleneck, callers can build a faster
// monoid-specific path; today's API takes the simple route.
func fingerprintValue[V any](v V) string {
	// Special-case []byte to avoid Sprintf's "[1 2 3]"-style formatting,
	// which is correct but produces large strings for big sketches.
	if b, ok := any(v).([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

// WithValueDebounce drops records when the same (entityKey, value) pair
// was already processed within `window`. Backed by an in-process
// bounded cache (2× maxKeys); on overflow, oldest entries past the
// window are evicted.
//
// **Use case.** A service emits absolute-value records on every change
// notification, but most events carry the SAME value as the previous
// emission (e.g. Mongo CDC fires for every document touch including
// no-op updates). WithValueDebounce drops those unchanged emissions
// before they hit the processor + store.
//
// **Compared to WithKeyDebounce.** WithKeyDebounce drops every record
// after the first for the same key in the window — works only if every
// event for a given key carries the same logical value.
// WithValueDebounce drops only when the VALUE also matches; a value
// change on the same key always passes through.
//
// **Safety.** Like WithKeyDebounce, debouncing changes semantics for
// non-idempotent monoids:
//
//   - Safe with: Max, Min, Set, Last, Monotonic — when an event
//     re-emits the same absolute value, that's a semantic no-op.
//   - Safe with: HLL, Bloom — idempotent under duplicate inputs.
//   - **NOT SAFE with: Sum, Count, TopK** — duplicate inputs accumulate;
//     dropping under-counts.
//
// Pair with `Int64MaxStore` + `core.Monotonic[int64]` for the
// SetCountIfGreater pattern: the monoid logic accepts only rising
// values; this debounce strips repeated identical emissions BEFORE
// they reach the store, saving DDB writes.
//
// **Composes with WithKeyDebounce / WithBatchWindow / WithConcurrency.**
// All four can be combined. Order of application: WithKeyDebounce
// first (drops same-key duplicates regardless of value), then
// WithValueDebounce (drops same-(key,value) within window), then
// per-EventID dedup, then batch window.
func WithValueDebounce(window time.Duration, maxKeys int) RunOption {
	return func(c *runConfig) {
		if window > 0 {
			c.valueDebounce = window
			c.valueDebounceMax = maxKeys
		}
	}
}
