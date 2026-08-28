// Read-path request validation. Every helper in this package used to answer a
// degenerate request with the monoid Identity and no error: a swapped range, a
// negative duration, an unset proto3 bound, or a window longer than Retention all
// produced a confident zero that the gRPC layer then labelled Present:true. An
// operator cannot tell that apart from "the counter really is zero", so the
// checks below turn every one of them into a caller-visible rejection.

package query

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
)

// ErrInvalidQuery marks a read rejected because of the request itself rather than
// anything wrong with the store. Test for it with errors.Is; pkg/query/grpc maps it
// onto connect.CodeInvalidArgument so callers stop reading query bugs as DDB outages.
var ErrInvalidQuery = errors.New("query: invalid request")

// minQueryTime and maxQueryTime bound the instants whose UnixNano — and therefore
// windowed.Config.BucketID — is representable in int64. Outside them BucketID wraps
// silently: end_unix=253402300799 (year 9999) and the zero time.Time that
// pkg/query/typed sends as Unix -62135596800 both land on arbitrary, often negative,
// bucket IDs and merge a slice of state nobody asked for.
var (
	minQueryTime = time.Unix(0, math.MinInt64).UTC()
	maxQueryTime = time.Unix(0, math.MaxInt64).UTC()
)

// invalidQuery builds an ErrInvalidQuery-matching error carrying a message specific
// enough for an operator to fix the callsite from the log line alone.
func invalidQuery(format string, args ...any) error {
	return &invalidQueryError{msg: "query: " + fmt.Sprintf(format, args...)}
}

type invalidQueryError struct{ msg string }

func (e *invalidQueryError) Error() string { return e.msg }

// Is reports a match against ErrInvalidQuery so callers can branch on the class
// without depending on the concrete type.
func (e *invalidQueryError) Is(target error) bool { return target == ErrInvalidQuery }

// windowBuckets validates a trailing-duration request and returns the inclusive
// bucket range it covers. Shared by GetWindow / GetWindowMany / LambdaQuery /
// WarmupWindowed so all of them reject the same shapes.
func windowBuckets(w windowed.Config, d time.Duration, now time.Time) (lo, hi int64, err error) {
	if d <= 0 {
		return 0, 0, invalidQuery("duration must be positive, got %s", d)
	}
	if now.Before(minQueryTime) || now.After(maxQueryTime) {
		return 0, 0, invalidQuery("now %s is outside the representable bucket range [%s, %s]",
			now.UTC().Format(time.RFC3339), minQueryTime.Format(time.RFC3339), maxQueryTime.Format(time.RFC3339))
	}
	if err := checkRetention(w, d); err != nil {
		return 0, 0, err
	}
	lo, hi = w.LastN(now, d)
	if err := checkSpan(w, lo, hi); err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

// rangeBuckets validates an absolute [start, end] request and returns the inclusive
// bucket range it covers.
func rangeBuckets(w windowed.Config, start, end time.Time) (lo, hi int64, err error) {
	if err := checkRepresentable("start", start); err != nil {
		return 0, 0, err
	}
	if err := checkRepresentable("end", end); err != nil {
		return 0, 0, err
	}
	if end.Before(start) {
		return 0, 0, invalidQuery("end %s precedes start %s",
			end.UTC().Format(time.RFC3339), start.UTC().Format(time.RFC3339))
	}
	lo, hi = w.BucketRange(start, end)
	if err := checkSpan(w, lo, hi); err != nil {
		return 0, 0, err
	}
	return lo, hi, nil
}

func checkRepresentable(name string, t time.Time) error {
	if t.Before(minQueryTime) || t.After(maxQueryTime) {
		return invalidQuery("%s %s is outside the representable bucket range [%s, %s]",
			name, t.UTC().Format(time.RFC3339),
			minQueryTime.Format(time.RFC3339), maxQueryTime.Format(time.RFC3339))
	}
	return nil
}

// checkRetention rejects a window longer than the buckets still exist for. Retention
// was advisory on the read path: asking for 90 days against a 7-day Retention read 83
// TTL-evicted buckets, folded them in as Identity, and returned the result labelled as
// a full 90-day window. A short window that happens to be missing buckets is normal
// and stays silent — this only catches the case where the bucket range itself reaches
// past what TTL keeps.
func checkRetention(w windowed.Config, d time.Duration) error {
	limit := w.RetentionBuckets()
	if limit <= 0 {
		return nil
	}
	n := int64(d / w.Granularity)
	if d%w.Granularity != 0 {
		n++
	}
	if n > limit {
		return invalidQuery("duration %s spans %d buckets of %s, beyond the %s retention (%d buckets)",
			d, n, w.Granularity, w.Retention, limit)
	}
	return nil
}

// checkSpan enforces windowed.Config.MaxBucketSpan on an already-computed bucket
// range. This is the last line before the key slice is materialized, so it runs on
// every path that fans a read out over buckets.
func checkSpan(w windowed.Config, lo, hi int64) error {
	limit := w.MaxBucketSpan()
	if limit <= 0 {
		return nil
	}
	span := hi - lo
	// A negative span here means hi-lo overflowed int64, which a nanosecond-scale
	// Granularity makes reachable from two representable instants.
	if span < 0 || span >= limit {
		return invalidQuery("range spans more than %d buckets (max_buckets); narrow the range or raise MaxBuckets", limit)
	}
	return nil
}
