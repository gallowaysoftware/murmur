package windowed_test

import (
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
)

// TestConfig_MaxBucketSpanCoversExactRetention pins the arithmetic against the shape
// it exists to admit. BucketRange is inclusive at both ends, so an absolute range of
// exactly Retention touches Retention/Granularity + 1 buckets; a cap derived as
// ceil(Retention/Granularity) was one short of that and turned the exact-retention
// read into an InvalidArgument.
func TestConfig_MaxBucketSpanCoversExactRetention(t *testing.T) {
	cases := []struct {
		name string
		cfg  windowed.Config
		want int64
	}{
		{"daily, 30d retention", windowed.Daily(30 * 24 * time.Hour), 31},
		{"hourly, 7d retention", windowed.Hourly(7 * 24 * time.Hour), 169},
		{"minute, 1h retention", windowed.Minute(time.Hour), 61},
		// Retention that is not a whole number of buckets rounds up, then still
		// gets the inclusive-endpoint bucket.
		{"daily, 90h retention", windowed.Daily(90 * time.Hour), 5},
		// An explicit MaxBuckets is a count the operator wrote down; it is used
		// verbatim, no +1.
		{"explicit MaxBuckets wins", windowed.Config{
			Granularity: 24 * time.Hour, Retention: 365 * 24 * time.Hour, MaxBuckets: 7,
		}, 7},
		// No Granularity means everything lands in bucket 0, so there is nothing
		// to fan out over and nothing to cap.
		{"no granularity is unbounded", windowed.Config{Retention: 30 * 24 * time.Hour}, 0},
		// The reason DefaultMaxBucketSpan exists: this Config used to report 0.
		{"granularity without retention", windowed.Config{Granularity: time.Minute},
			windowed.DefaultMaxBucketSpan},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cfg.MaxBucketSpan(); got != tc.want {
				t.Errorf("MaxBucketSpan() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestConfig_BucketRangeOverRetentionIsWithinCap is the property the constant in
// MaxBucketSpan has to satisfy: whatever the alignment of the range, a read covering
// exactly Retention must fit inside the cap, and a read one bucket longer must not.
func TestConfig_BucketRangeOverRetentionIsWithinCap(t *testing.T) {
	for _, cfg := range []windowed.Config{
		windowed.Daily(30 * 24 * time.Hour),
		windowed.Hourly(7 * 24 * time.Hour),
		windowed.Minute(time.Hour),
	} {
		// Offsets deliberately unaligned to the bucket grid — the natural query
		// starts at whatever instant the caller happens to hold.
		for _, offset := range []time.Duration{0, 1, 37 * time.Second, 12*time.Hour + 3*time.Minute} {
			start := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC).Add(offset)

			lo, hi := cfg.BucketRange(start, start.Add(cfg.Retention))
			if touched := hi - lo + 1; touched > cfg.MaxBucketSpan() {
				t.Errorf("%s granularity, offset %s: a range of exactly the %s retention touches %d buckets, over the %d cap",
					cfg.Granularity, offset, cfg.Retention, touched, cfg.MaxBucketSpan())
			}

			lo, hi = cfg.BucketRange(start, start.Add(cfg.Retention+cfg.Granularity))
			if touched := hi - lo + 1; touched <= cfg.MaxBucketSpan() {
				t.Errorf("%s granularity, offset %s: a range one bucket past the %s retention touches %d buckets, still inside the %d cap",
					cfg.Granularity, offset, cfg.Retention, touched, cfg.MaxBucketSpan())
			}
		}
	}
}
