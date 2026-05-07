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
type Config struct {
	// Granularity is the size of each tumbling bucket (e.g. 24h for daily, 1h for hourly).
	// Smaller granularity means finer query precision and higher state cost.
	Granularity time.Duration

	// Retention is how long buckets are kept before TTL eviction. Sliding-window queries
	// can ask for any range up to Retention.
	Retention time.Duration

	// EventTimeField, if non-empty, names the field on the source record from which to
	// extract event-time. If empty, processing-time is used. Backends honor this when
	// computing BucketID.
	EventTimeField string
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
