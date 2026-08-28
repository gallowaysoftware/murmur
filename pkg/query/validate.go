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
	// An absolute range answers to Retention just as a trailing window does. Only
	// checkSpan ran here before, and its limit is MaxBuckets whenever MaxBuckets is
	// set — so a Config with MaxBuckets raised above Retention read straight past TTL:
	// GetRange over a year against a 7-day Retention fanned out over 366 buckets, found
	// 359 of them evicted, folded the holes in as Identity, and returned one week's
	// total labelled as a year. MaxBuckets is a cap, never a licence to outrun TTL.
	//
	// time.Time.Sub saturates instead of wrapping, so a range between the two extreme
	// representable instants arrives here as the maximum Duration and is rejected
	// rather than overflowing into a plausible-looking short one.
	if err := checkRetention(w, end.Sub(start)); err != nil {
		return 0, 0, err
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

// checkRetention rejects a read wider than the buckets still exist for. Retention was
// advisory on the read path: asking for 90 days against a 7-day Retention read 83
// TTL-evicted buckets, folded them in as Identity, and returned the result labelled as
// a full 90-day window. A short window that happens to be missing buckets is normal
// and stays silent — this only catches the case where the requested span itself reaches
// past what TTL keeps.
//
// It bounds the WIDTH of a read, not its age. For a trailing window those are the same
// thing, because the window is anchored at now. For an absolute range they are not:
// GetRange(now-90d, now-83d) against a 7-day Retention is seven days wide and passes
// here even though every bucket in it is long evicted. Age is deliberately left
// unchecked. rangeBuckets has no `now` to measure against, and threading one in would
// break LambdaQuery.GetRange, whose View store holds history written by a bootstrap or
// replay job and outlives the streaming table's TTL by design — reading a year-old
// range out of a batch view is the feature, not the bug.
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
//
// [lo, hi] is inclusive at both ends, so it touches hi-lo+1 buckets. That count is
// what MaxBucketSpan caps, and the comparison is written as span >= limit rather than
// span+1 > limit so that a span of MaxInt64 cannot overflow the addition.
func checkSpan(w windowed.Config, lo, hi int64) error {
	limit := w.MaxBucketSpan()
	if limit <= 0 {
		return nil
	}
	span := hi - lo
	// A negative span means hi-lo overflowed int64, which a nanosecond-scale
	// Granularity makes reachable from two representable instants; MaxInt64 is the
	// one non-negative span whose bucket count would overflow in turn.
	if span < 0 || span == math.MaxInt64 {
		return invalidQuery("range spans more buckets than int64 can count; narrow the range or coarsen Granularity")
	}
	if span >= limit {
		return invalidQuery("range touches %d buckets of %s, more than the %d-bucket cap; narrow the range or raise MaxBuckets",
			span+1, w.Granularity, limit)
	}
	return nil
}
