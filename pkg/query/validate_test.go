package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// keyProbeStore records the key slices it was asked for, so a test can assert
// that a rejected request never reached the store at all.
type keyProbeStore struct {
	fakeStore
	requested [][]state.Key
}

func (s *keyProbeStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.requested = append(s.requested, ks)
	return s.fakeStore.GetMany(ctx, ks)
}

func (s *keyProbeStore) keysRequested() int {
	n := 0
	for _, ks := range s.requested {
		n += len(ks)
	}
	return n
}

func TestGetRange_RejectsDegenerateBounds(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		start, end time.Time
	}{
		{"swapped", now, now.Add(-24 * time.Hour)},
		// pkg/query/typed sends a zero time.Time as Unix -62135596800, whose
		// UnixNano wraps and lands on an arbitrary negative bucket.
		{"zero start time", time.Time{}, now},
		// The proto3 "max timestamp" an operator reaches for when they mean
		// "forever"; UnixNano wraps here too.
		{"year 9999 end", now.Add(-24 * time.Hour), time.Unix(253402300799, 0).UTC()},
		{"both ends unrepresentable", time.Time{}, time.Unix(253402300799, 0).UTC()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &keyProbeStore{fakeStore: fakeStore{}}
			got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A", tc.start, tc.end)
			if err == nil {
				t.Fatalf("GetRange(%s, %s): got (%d, nil), want an error",
					tc.start.Format(time.RFC3339), tc.end.Format(time.RFC3339), got)
			}
			if !errors.Is(err, query.ErrInvalidQuery) {
				t.Errorf("error %v does not match ErrInvalidQuery", err)
			}
			if n := store.keysRequested(); n != 0 {
				t.Errorf("rejected range still read %d keys from the store", n)
			}
		})
	}
}

func TestGetRange_AcceptsOrdinaryBounds(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := fakeStore{
		state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 5,
	}
	got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-48*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if got != 5 {
		t.Errorf("GetRange: got %d, want 5", got)
	}
}

func TestGetWindow_RejectsNonPositiveDuration(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	for _, d := range []time.Duration{0, -time.Second, -30 * 24 * time.Hour} {
		store := &keyProbeStore{fakeStore: fakeStore{}}
		got, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", d, now)
		if err == nil {
			t.Fatalf("GetWindow(duration=%s): got (%d, nil), want an error", d, got)
		}
		if !errors.Is(err, query.ErrInvalidQuery) {
			t.Errorf("GetWindow(duration=%s): error %v does not match ErrInvalidQuery", d, err)
		}
		if n := store.keysRequested(); n != 0 {
			t.Errorf("GetWindow(duration=%s): rejected window still read %d keys", d, n)
		}
	}
}

func TestGetWindow_RejectsDurationBeyondRetention(t *testing.T) {
	// Retention keeps 7 daily buckets. A 30-day window used to read 30 buckets,
	// find 23 of them TTL-evicted, fold the gaps in as Identity, and hand back
	// the 7-day total labelled as a 30-day answer.
	w := windowed.Daily(7 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	for i := 0; i < 7; i++ {
		store.fakeStore[state.Key{Entity: "page-A", Bucket: w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))}] = 1
	}

	if _, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 30*24*time.Hour, now); err == nil {
		t.Fatal("GetWindow(30d) against a 7d retention: got nil error, want a rejection")
	} else if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}

	// The longest window retention can actually answer stays allowed.
	got, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetWindow(7d): %v", err)
	}
	if got != 7 {
		t.Errorf("GetWindow(7d): got %d, want 7", got)
	}
}

// spanCappedConfig is a Config whose binding read limit is MaxBucketSpan rather than
// Retention: a day of minute buckets kept, but no more than 60 of them read at once.
//
// The distinction is what makes the two tests below exercise checkSpan at all. A range
// wider than Retention is refused a step earlier now, so a config where Retention is
// the tighter of the two bounds can never reach the span check.
func spanCappedConfig() windowed.Config {
	w := windowed.Minute(24 * time.Hour)
	w.MaxBuckets = 60
	return w
}

func TestGetRange_RejectsBucketSpanBeyondCap(t *testing.T) {
	// Two hours at minute granularity is 121 buckets, well inside the day of
	// retention but twice the 60 the operator allowed per read. Uncapped, the same
	// shape at the shipped Minute(24h) preset reaches 29,797,201 keys for a range
	// starting at the epoch.
	w := spanCappedConfig()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}

	_, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-2*time.Hour), now)
	if err == nil {
		t.Fatal("GetRange over 121 minute buckets with a 60-bucket cap: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	// Naming the count and the cap pins that MaxBucketSpan is what refused this, not
	// the retention check one step earlier.
	if !strings.Contains(err.Error(), "121 buckets") || !strings.Contains(err.Error(), "60-bucket cap") {
		t.Errorf("error %q does not report the bucket count against the cap", err)
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected range still fanned out over %d keys", n)
	}

	// A range inside the cap is still served.
	if _, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-30*time.Minute), now); err != nil {
		t.Errorf("GetRange over 31 minute buckets: %v", err)
	}
}

func TestGetRangeMany_RejectsBucketSpanPerEntity(t *testing.T) {
	// The Many path multiplies the bucket span by the entity count, so the cap
	// matters most here: 121 buckets × 3 entities = 363 keys in one request.
	w := spanCappedConfig()
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	entities := []string{"a", "b", "c"}

	vals, err := query.GetRangeMany(context.Background(), store, core.Sum[int64](), w, entities,
		now.Add(-2*time.Hour), now)
	if err == nil {
		t.Fatal("GetRangeMany over 121 minute buckets with a 60-bucket cap: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if !strings.Contains(err.Error(), "60-bucket cap") {
		t.Errorf("error %q does not report the bucket cap", err)
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected range still fanned out over %d keys", n)
	}
	// Callers that ignore the error must still get an indexable slice.
	if len(vals) != len(entities) {
		t.Errorf("rejected GetRangeMany returned %d values, want %d", len(vals), len(entities))
	}
}

func TestGetWindowMany_RejectsDurationBeyondRetention(t *testing.T) {
	w := windowed.Daily(7 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	entities := []string{"a", "b"}

	vals, err := query.GetWindowMany(context.Background(), store, core.Sum[int64](), w, entities, 90*24*time.Hour, now)
	if err == nil {
		t.Fatal("GetWindowMany(90d) against a 7d retention: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if len(vals) != len(entities) {
		t.Errorf("rejected GetWindowMany returned %d values, want %d", len(vals), len(entities))
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected window still read %d keys", n)
	}
}

// TestGetRange_AcceptsExactlyRetention pins the boundary the fan-out cap has to get
// right. Buckets are tumbling and BucketRange is inclusive at both ends, so an absolute
// range of exactly Retention touches Retention/Granularity + 1 buckets. A cap derived as
// ceil(Retention/Granularity) was one short of that, so "give me the whole window I am
// allowed to ask for" — the most natural range a caller writes — came back
// InvalidArgument.
func TestGetRange_AcceptsExactlyRetention(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	for i := 0; i <= 30; i++ {
		store.fakeStore[state.Key{Entity: "page-A", Bucket: w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))}] = 1
	}

	got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-30*24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange over exactly the %s retention window: %v", w.Retention, err)
	}
	if got != 31 {
		t.Errorf("GetRange over exactly the retention window: got %d, want 31", got)
	}
	if n := store.keysRequested(); n != 31 {
		t.Errorf("GetRange over exactly the retention window read %d keys, want 31", n)
	}

	// One bucket further back is genuinely past what TTL keeps, and is refused.
	store = &keyProbeStore{fakeStore: fakeStore{}}
	if _, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-31*24*time.Hour), now); err == nil {
		t.Fatal("GetRange one bucket past the retention window: got nil error, want a rejection")
	} else if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected range still read %d keys", n)
	}
}

// TestGetRangeMany_AcceptsExactlyRetention is the fan-out counterpart: GetRangeMany
// shares rangeBuckets, so the same off-by-one rejected the exact-retention query for
// every entity in the batch at once.
func TestGetRangeMany_AcceptsExactlyRetention(t *testing.T) {
	w := windowed.Hourly(7 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	entities := []string{"a", "b"}
	store := &keyProbeStore{fakeStore: fakeStore{}}
	for _, e := range entities {
		store.fakeStore[state.Key{Entity: e, Bucket: w.BucketID(now)}] = 3
	}

	vals, err := query.GetRangeMany(context.Background(), store, core.Sum[int64](), w, entities,
		now.Add(-7*24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRangeMany over exactly the %s retention window: %v", w.Retention, err)
	}
	for i, v := range vals {
		if v != 3 {
			t.Errorf("GetRangeMany[%d] = %d, want 3", i, v)
		}
	}
	// 169 buckets per entity: 168 hours of retention plus the inclusive endpoint.
	if n := store.keysRequested(); n != 169*len(entities) {
		t.Errorf("GetRangeMany read %d keys, want %d", n, 169*len(entities))
	}

	if _, err := query.GetRangeMany(context.Background(), store, core.Sum[int64](), w, entities,
		now.Add(-7*24*time.Hour-time.Hour), now); err == nil {
		t.Error("GetRangeMany one bucket past the retention window: got nil error, want a rejection")
	}
}

// TestGetRange_CapsFanOutWithoutRetention covers the Config the cap used to miss
// entirely. MaxBucketSpan only reported a limit when Retention or MaxBuckets was set,
// so a hand-built Config{Granularity: time.Minute} got no cap at all and GetRange from
// the epoch still built ~30 million keys before the first store call.
func TestGetRange_CapsFanOutWithoutRetention(t *testing.T) {
	w := windowed.Config{Granularity: time.Minute} // no Retention, no MaxBuckets
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}

	_, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		time.Unix(0, 0).UTC(), now)
	if err == nil {
		t.Fatal("GetRange(epoch, now) at minute granularity with no Retention: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected range still fanned out over %d keys", n)
	}

	// A read inside the default cap is still served: the cap is a backstop against
	// unbounded, not a policy on how far back an unretained pipeline may look.
	store = &keyProbeStore{fakeStore: fakeStore{
		state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 9,
	}}
	got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange over 1441 minute buckets, inside the %d-bucket default: %v",
			windowed.DefaultMaxBucketSpan, err)
	}
	if got != 9 {
		t.Errorf("GetRange: got %d, want 9", got)
	}
}

// TestGetRange_MaxBucketsCannotOutrunRetention pins that MaxBuckets is a cap and never
// a licence. Only checkSpan ran on the absolute-range path, and its limit is MaxBuckets
// whenever MaxBuckets is set — so raising MaxBuckets above Retention let GetRange read
// straight past TTL and report the holes as Identity.
func TestGetRange_MaxBucketsCannotOutrunRetention(t *testing.T) {
	w := windowed.Daily(7 * 24 * time.Hour)
	w.MaxBuckets = 400
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	for i := 0; i < 7; i++ {
		store.fakeStore[state.Key{Entity: "page-A", Bucket: w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))}] = 1
	}

	// A year against a 7-day retention: 359 of the 366 buckets are long evicted, and
	// the answer used to come back as 7 labelled as a year's worth.
	got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-365*24*time.Hour), now)
	if err == nil {
		t.Fatalf("GetRange(365d) with Retention=7d, MaxBuckets=400: got (%d, nil), want a rejection", got)
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if n := store.keysRequested(); n != 0 {
		t.Errorf("rejected range still fanned out over %d keys", n)
	}

	// What retention can actually answer is unaffected.
	if _, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-7*24*time.Hour), now); err != nil {
		t.Errorf("GetRange over exactly the retention window with MaxBuckets=400: %v", err)
	}
}

// TestLambdaQueryGetRange_MaxBucketsCannotOutrunRetention covers the third caller of
// rangeBuckets. LambdaQuery fans the same key list out over TWO stores, so an
// unretained range costs double.
func TestLambdaQueryGetRange_MaxBucketsCannotOutrunRetention(t *testing.T) {
	w := windowed.Daily(7 * 24 * time.Hour)
	w.MaxBuckets = 400
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	view := &keyProbeStore{fakeStore: fakeStore{}}
	delta := &keyProbeStore{fakeStore: fakeStore{}}
	q := query.LambdaQuery[int64]{View: view, Delta: delta, Monoid: core.Sum[int64]()}

	if _, err := q.GetRange(context.Background(), w, "page-A", now.Add(-365*24*time.Hour), now); err == nil {
		t.Fatal("LambdaQuery.GetRange(365d) with Retention=7d, MaxBuckets=400: got nil error, want a rejection")
	} else if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
	}
	if n := view.keysRequested() + delta.keysRequested(); n != 0 {
		t.Errorf("rejected range still fanned out over %d keys across the two stores", n)
	}

	if _, err := q.GetRange(context.Background(), w, "page-A", now.Add(-7*24*time.Hour), now); err != nil {
		t.Errorf("LambdaQuery.GetRange over exactly the retention window: %v", err)
	}
}

func TestConfigMaxBuckets_OverridesRetentionDefault(t *testing.T) {
	w := windowed.Daily(365 * 24 * time.Hour)
	w.MaxBuckets = 7
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}

	if _, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 30*24*time.Hour, now); err == nil {
		t.Fatal("GetWindow(30d) with MaxBuckets=7: got nil error, want a rejection")
	}
	if _, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 7*24*time.Hour, now); err != nil {
		t.Errorf("GetWindow(7d) with MaxBuckets=7: %v", err)
	}
}
