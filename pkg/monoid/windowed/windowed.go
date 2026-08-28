// Package windowed expresses time-bucketed aggregations on top of any structural monoid.
// State is keyed by (entity, bucket_id) where bucket_id is computed from event time and
// the configured Granularity. Queries assemble sliding windows by Combining the N most
// recent buckets through the wrapped monoid. DDB TTL handles eviction past Retention.
package windowed

import (
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Config describes the bucket layout for a windowed aggregation.
//
// The timestamp a record is bucketed by is source.Record.EventTime — every
// runtime passes it to BucketID, falling back to the wall clock only when a
// record carries no event time. There is no per-field extractor here: sources
// own event-time, not the window config.
type Config struct {
	// Granularity is the size of each tumbling bucket (e.g. 24h for daily, 1h for hourly).
	// Smaller granularity means finer query precision and higher state cost.
	Granularity time.Duration

	// Retention is how long buckets are kept before TTL eviction. Sliding-window queries
	// can ask for any range up to Retention.
	Retention time.Duration

	// MaxBuckets caps how many buckets a single read may span. Leave it zero to take
	// the default from Retention (see MaxBucketSpan).
	//
	// Without a cap, a caller who passes an open-ended range gets no error — just an
	// enormous key list. GetRange(entity, Unix(0,0), now) against Minute granularity
	// builds 29,797,201 keys (~715MB of state.Key) before the first store call, and
	// all but the last Retention/Granularity of them address buckets that TTL evicted
	// and can never hold data again.
	MaxBuckets int
}

// MaxBucketSpan reports the largest number of buckets one read may span. It is
// MaxBuckets when set, otherwise ceil(Retention / Granularity) — reading further back
// than Retention can only return TTL-evicted buckets. Zero means unbounded, which
// happens only when neither MaxBuckets nor Retention is configured.
func (c Config) MaxBucketSpan() int64 {
	if c.MaxBuckets > 0 {
		return int64(c.MaxBuckets)
	}
	if c.Granularity <= 0 || c.Retention <= 0 {
		return 0
	}
	n := int64(c.Retention / c.Granularity)
	if c.Retention%c.Granularity != 0 {
		n++
	}
	return n
}

// RetentionBuckets reports how many whole buckets fit inside Retention — the longest
// window a read can ask for and still be answered entirely from live buckets. Zero
// means Retention is unset (or shorter than a single bucket), in which case callers
// should not enforce a retention bound.
func (c Config) RetentionBuckets() int64 {
	if c.Granularity <= 0 || c.Retention <= 0 {
		return 0
	}
	return int64(c.Retention / c.Granularity)
}

// Daily returns a Config with 24h granularity and the given retention. The most common
// configuration for "last N days of X"-style counters.
func Daily(retention time.Duration) Config {
	return Config{Granularity: 24 * time.Hour, Retention: retention}
}

// Hourly returns a Config with 1h granularity and the given retention. Suitable for
// "last N hours" or "last 7 days at hourly resolution" queries.
func Hourly(retention time.Duration) Config {
	return Config{Granularity: time.Hour, Retention: retention}
}

// Minute returns a Config with 1-minute granularity and the given retention. Useful
// for high-resolution short-window aggregations (last 5 minutes, last hour at
// per-minute resolution). At this granularity, "last 7 days" reads 10080 buckets per
// query — consider hierarchical roll-ups for queries spanning more than ~24h.
func Minute(retention time.Duration) Config {
	return Config{Granularity: time.Minute, Retention: retention}
}

// BucketID assigns the given time to a bucket according to Granularity. Buckets are
// tumbling and aligned to the Unix epoch.
func (c Config) BucketID(t time.Time) int64 {
	if c.Granularity <= 0 {
		return 0
	}
	return t.UnixNano() / int64(c.Granularity)
}

// BucketRange returns the inclusive range of bucket IDs that cover [start, end].
func (c Config) BucketRange(start, end time.Time) (lo, hi int64) {
	return c.BucketID(start), c.BucketID(end)
}

// LastN returns the bucket-ID range covering the most recent d duration ending at now.
// The number of buckets returned is ceil(d / Granularity); the upper bound is the bucket
// containing now and the range extends backward that many buckets. So for daily
// granularity, "last 7 days" returns 7 buckets (today plus 6 prior), not 8.
func (c Config) LastN(now time.Time, d time.Duration) (lo, hi int64) {
	if c.Granularity <= 0 {
		return 0, 0
	}
	hi = c.BucketID(now)
	n := int64((d + c.Granularity - 1) / c.Granularity) // ceil
	if n < 1 {
		n = 1
	}
	lo = hi - n + 1
	return lo, hi
}

// Wrapped pairs a monoid with a windowing config. The Pipeline DSL uses this to drive
// state-store keying and query-handler generation; the underlying Combine is unchanged.
type Wrapped[V any] struct {
	Inner  monoid.Monoid[V]
	Window Config
}

// MergeBuckets folds bucket values via the inner monoid in stable order. Used by the
// query layer to produce sliding-window results from per-bucket reads.
func MergeBuckets[V any](m monoid.Monoid[V], values []V) V {
	out := m.Identity()
	for _, v := range values {
		out = m.Combine(out, v)
	}
	return out
}
