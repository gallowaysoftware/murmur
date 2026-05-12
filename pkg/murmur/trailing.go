// Trailing-window builder sugar. Most counter consumers want a small
// set of "last N days" / "last N hours" trailing windows, not arbitrary
// absolute ranges. `(*CounterBuilder).Trailing(durations...)` is the
// preset for that shape: pass the durations the pipeline will be
// queried for and the builder both
//
//   - records the requested trailing windows on the pipeline so
//     downstream tooling (query servers, terraform modules, dashboards)
//     can discover them, and
//   - sizes Daily retention from max(durations) + 1 day of slack so
//     the buckets needed to answer the largest query are guaranteed
//     to still be in the store. The +1 day slack covers UTC midnight
//     boundary effects (a query "now" near midnight may straddle two
//     daily buckets; the slack ensures the oldest bucket isn't TTL'd
//     out from under us).
//
// Trailing is currently only implemented for CounterBuilder (the
// dominant counter case). UniqueCount / TopN / Trending can grow
// matching helpers later once we see real demand.

package murmur

import (
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
)

// trailingSlack is added to max(durations) when sizing Daily retention.
// One full bucket of slack covers UTC-midnight boundary effects: a
// trailing-7d query issued shortly after midnight reads 8 daily
// buckets (today plus the previous 7), so the oldest bucket must still
// be present in the store.
const trailingSlack = 24 * time.Hour

// Trailing records the trailing-window durations the pipeline will be
// queried for and configures Daily windowing with retention sized to
// the largest requested window plus one bucket of slack.
//
// Use it instead of Daily when the consumer side is known to query
// fixed trailing windows:
//
//	murmur.Counter[Like]("post_likes").
//	    KeyBy(func(e Like) string { return e.PostID }).
//	    Trailing(24*time.Hour, 7*24*time.Hour, 30*24*time.Hour).
//	    StoreIn(store).
//	    Build()
//
// The pipeline above keeps 31 days of daily buckets and is ready to
// answer trailing-1d / trailing-7d / trailing-30d queries.
//
// Passing zero or negative durations is silently dropped; passing no
// durations at all is a no-op (the builder stays un-windowed).
//
// Trailing supersedes any prior Daily / Hourly call on the same
// builder. Multiple Trailing calls re-set the trailing-window set
// from scratch rather than accumulating across calls.
func (b *CounterBuilder[T]) Trailing(durations ...time.Duration) *CounterBuilder[T] {
	cleaned := make([]time.Duration, 0, len(durations))
	var maxDur time.Duration
	for _, d := range durations {
		if d <= 0 {
			continue
		}
		cleaned = append(cleaned, d)
		if d > maxDur {
			maxDur = d
		}
	}
	if maxDur == 0 {
		// Nothing to configure — preserve any previously-set window.
		return b
	}
	w := windowed.Daily(maxDur + trailingSlack)
	b.window = &w
	b.trailing = cleaned
	return b
}

// TrailingWindows returns the trailing-window durations recorded on
// the builder via Trailing, in the order they were supplied (with
// zero / negative values filtered out). Returns nil if Trailing was
// never called.
//
// Used by downstream tooling that wants to discover the canonical
// query windows for a pipeline — e.g., a query server that wants to
// pre-register trailing-7d / trailing-30d as named convenience
// endpoints, or a dashboard that wants to render exactly the
// trailing windows the pipeline was designed for.
func (b *CounterBuilder[T]) TrailingWindows() []time.Duration {
	if len(b.trailing) == 0 {
		return nil
	}
	out := make([]time.Duration, len(b.trailing))
	copy(out, b.trailing)
	return out
}
