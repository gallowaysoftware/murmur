package query_test

import (
	"context"
	"errors"
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

func TestGetRange_RejectsBucketSpanBeyondCap(t *testing.T) {
	// Minute granularity with an hour of retention: 60 live buckets. A range
	// reaching two days back used to build 2880 keys, of which at most 60 could
	// ever hold data. At the shipped Minute(24h) preset the same shape reaches
	// 29,797,201 keys for a range starting at the epoch.
	w := windowed.Minute(time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}

	_, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A",
		now.Add(-48*time.Hour), now)
	if err == nil {
		t.Fatal("GetRange over 2880 minute buckets with a 60-bucket cap: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
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
	// matters most here: 2880 buckets × 3 entities = 8640 keys in one request.
	w := windowed.Minute(time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &keyProbeStore{fakeStore: fakeStore{}}
	entities := []string{"a", "b", "c"}

	vals, err := query.GetRangeMany(context.Background(), store, core.Sum[int64](), w, entities,
		now.Add(-48*time.Hour), now)
	if err == nil {
		t.Fatal("GetRangeMany over 2880 minute buckets with a 60-bucket cap: got nil error, want a rejection")
	}
	if !errors.Is(err, query.ErrInvalidQuery) {
		t.Errorf("error %v does not match ErrInvalidQuery", err)
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
