package streaming

import (
	"sync"
	"time"
)

// keyDebouncer drops records whose first-emitted key was already seen
// within `window`. Backed by a small per-key timestamp map plus a
// janitor that evicts entries older than 2× window so the map stays
// bounded.
//
// The "producer-side debounce" use case from the count-core integration
// review: a service emits an event per cache miss; if N pods all miss
// the same hot key at top-of-hour, you get N events. The debouncer
// drops N-1 of them at the source pump, before the processor sees them
// — saves processor CPU AND store writes.
//
// **Safety boundary.** Debouncing changes semantics for non-idempotent
// monoids:
//
//   - Sum / Count: dropping records UNDER-COUNTS. WRONG for these.
//   - Max / Min / Set / Last: dropping a duplicate-key record is
//     either a no-op (if values agree) or makes the result depend on
//     which arrived first — usually fine for cache-fill semantics
//     where every event for the same key carries the same absolute
//     value.
//   - HLL / Bloom: idempotent under duplicate inputs (the same byte
//     sequence inserted twice is the same sketch). Safe.
//   - TopK: similar to Sum — duplicate inputs DO accumulate into the
//     count; debouncing under-counts. WRONG.
//
// The package doc and the `WithKeyDebounce` option documentation are
// upfront about this. There's no runtime check; the caller picks based
// on their monoid and event semantics.
type keyDebouncer struct {
	window  time.Duration
	maxKeys int

	mu   sync.Mutex
	seen map[string]time.Time

	now func() time.Time // injectable for tests
}

func newKeyDebouncer(window time.Duration, maxKeys int) *keyDebouncer {
	if maxKeys <= 0 {
		maxKeys = 100_000
	}
	return &keyDebouncer{
		window:  window,
		maxKeys: maxKeys,
		seen:    make(map[string]time.Time, 256),
		now:     time.Now,
	}
}

// shouldDrop reports whether a record with the given first-emitted key
// is a duplicate within the window and should be dropped. When it
// returns false, the key is recorded as seen at the current time.
//
// Map size is capped at 2× maxKeys; on overflow the oldest entries are
// evicted in batches. This keeps memory bounded under high-cardinality
// keyspaces; for keyspaces well under maxKeys, eviction never fires
// and the debouncer behaves as expected.
func (d *keyDebouncer) shouldDrop(key string) bool {
	if key == "" {
		return false
	}
	now := d.now()
	d.mu.Lock()
	defer d.mu.Unlock()

	if last, ok := d.seen[key]; ok {
		if now.Sub(last) < d.window {
			return true
		}
	}
	d.seen[key] = now

	// Bound the map: evict roughly half when we hit 2x maxKeys. The
	// eviction is a coarse "drop entries older than window" pass —
	// good enough for the typical case where keyspace cardinality
	// hovers around maxKeys.
	if len(d.seen) > 2*d.maxKeys {
		threshold := now.Add(-d.window)
		for k, t := range d.seen {
			if t.Before(threshold) {
				delete(d.seen, k)
			}
		}
		// If we're STILL over (high churn within the window), delete
		// half the entries arbitrarily to force progress. Loses some
		// state — the next emit for those keys won't be debounced —
		// but bounded memory is the priority.
		if len(d.seen) > 2*d.maxKeys {
			i := 0
			for k := range d.seen {
				delete(d.seen, k)
				i++
				if i >= len(d.seen)/2 {
					break
				}
			}
		}
	}
	return false
}

// stats returns the current map size for tests / monitoring.
func (d *keyDebouncer) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

// WithKeyDebounce drops records whose first-emitted key was already
// seen within `window`. The dropped record's Ack still fires (so the
// source advances) but no processor work is done — same shape as the
// dedup-skip path, but keyed by entity rather than EventID.
//
// **Use cases.** This is the producer-side debounce primitive count-core
// asked for in their integration review. The canonical pattern: a
// service emits a cache-fill event per cache miss; bursts of misses on
// the same hot key at top-of-hour produce N redundant events, all
// carrying the same value. With WithKeyDebounce(5*time.Second, 10000),
// the first event lands, the next N-1 within 5s are dropped. The
// underlying source advances (Ack fires); the store is touched once.
//
// **Critical safety constraint.** Debouncing CHANGES SEMANTICS for
// non-idempotent monoids:
//
//   - Safe with: Max, Min, Set, Last (cache-fill / absolute-value
//     patterns where N events for the same key carry the same value).
//   - Safe with: HLL, Bloom (idempotent under duplicate inputs).
//   - **NOT SAFE with: Sum, Count, TopK** (dropping deltas UNDER-COUNTS).
//
// There's no runtime check on the monoid kind — the option is shipped
// with the assumption the caller knows what they're doing. Pair with
// `Int64MaxStore` (this package) for the count-core SetCountIfGreater
// pattern.
//
// **Differences from WithDedup.** WithDedup skips records whose
// EventID has already been claimed (per-event idempotency, typically
// backed by DDB with TTL). WithKeyDebounce skips records whose KEY
// has been seen within an in-process time window — much cheaper (no
// DDB roundtrip, no TTL writes), much narrower (only safe for
// idempotent / absolute-value monoids).
//
// **Differences from WithBatchWindow.** WithBatchWindow accumulates
// per-key deltas into a single store write per flush — it KEEPS all
// the records' contributions, just merged. WithKeyDebounce DROPS all
// but the first record per key per window. The two compose: a
// pipeline can use both, where debounce eliminates duplicate-cache-
// miss noise and the batch window then collapses any remaining writes
// per key.
func WithKeyDebounce(window time.Duration, maxKeys int) RunOption {
	return func(c *runConfig) {
		if window > 0 {
			c.keyDebounce = window
			c.keyDebounceMax = maxKeys
		}
	}
}
