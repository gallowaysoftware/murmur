// Package state defines the StateStore abstraction Murmur pipelines write aggregations
// to. Concrete implementations live in subpackages (state/dynamodb, state/valkey).
//
// Murmur's invariant: DynamoDB is always the source of truth. Valkey, when configured,
// is a read-cache and sketch-accelerator — never trusted as ground truth. If Valkey is
// lost, every pipeline can rebuild its accelerator state from DynamoDB.
package state

import (
	"context"
	"time"
)

// Key identifies a stored aggregation value. Entity is the user-supplied aggregation key
// (e.g. a page ID, customer ID). Bucket, when nonzero, is the time-bucket ID for windowed
// aggregations; bucket 0 means "no window / all-time."
type Key struct {
	Entity string
	Bucket int64
}

// Store is the StateStore contract. Implementations must be safe for concurrent use.
//
// MergeUpdate atomically applies the monoid Combine of the current value with delta and
// writes the result. Implementations may use native primitives (DynamoDB UpdateItem ADD,
// Valkey PFADD) when the monoid Kind permits, falling back to read-modify-write under a
// conditional write for non-native cases.
type Store[V any] interface {
	// Get reads the current value at k. Returns zero value of V and ok=false if missing.
	Get(ctx context.Context, k Key) (val V, ok bool, err error)

	// GetMany batches reads. Returns one entry per requested key; missing keys are
	// represented by the zero value of V at that index with ok=false.
	GetMany(ctx context.Context, ks []Key) (vals []V, ok []bool, err error)

	// MergeUpdate combines delta into the existing value at k via the monoid associated
	// with the pipeline. ttl, if nonzero, sets/extends the TTL on the underlying record
	// (used for windowed aggregations to expire old buckets).
	MergeUpdate(ctx context.Context, k Key, delta V, ttl time.Duration) error

	// Close releases any underlying resources (connection pools, batchers).
	Close() error
}

// Cache is the read-side accelerator. It mirrors a subset of Store data and may serve
// reads with lower latency. Pipelines configured with a cache write through to both
// Store and Cache; on Cache miss, they fall back to Store.
//
// A Cache is *never* a source of truth. Implementations should treat their data as
// repopulatable from the underlying Store at any time.
type Cache[V any] interface {
	Store[V]
	// Repopulate rebuilds the cache from the authoritative Store, e.g. after a Valkey
	// node restart. Implementations may stream from Store and insert in batches.
	Repopulate(ctx context.Context, src Store[V], keys []Key) error
}

// Deduper is the at-least-once dedup contract. The streaming runtime calls
// MarkSeen with each Source.Record's EventID before applying the monoid
// Combine; on a duplicate (worker crashed mid-write, source replays the
// record) MarkSeen returns firstSeen=false and the runtime skips the merge.
//
// Implementations must be:
//   - Atomic. Two concurrent calls with the same EventID must produce exactly
//     one firstSeen=true and one firstSeen=false — never two of either.
//   - Bounded. EventIDs older than a configured retention should fall out so
//     the dedup table doesn't grow forever. Typical TTL is hours to days
//     depending on how long messages can sit unacked in the source's
//     retention window.
type Deduper interface {
	// MarkSeen atomically attempts to claim eventID. Returns firstSeen=true
	// if the caller is the first to mark this ID; firstSeen=false if it was
	// already claimed. Returns an error only on transient backend failures
	// — duplicates are NOT errors.
	MarkSeen(ctx context.Context, eventID string) (firstSeen bool, err error)

	// Close releases any underlying resources.
	Close() error
}
