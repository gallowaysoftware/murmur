package dynamodb_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/state"
	"github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// concatMonoid is the simplest byte monoid that grows without bound: Combine
// appends. Real sketches grow with the length of the keys fed into them; this
// makes the size the test controls directly.
type concatMonoid struct{}

func (concatMonoid) Identity() []byte { return nil }

func (concatMonoid) Combine(a, b []byte) []byte {
	out := make([]byte, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

func (concatMonoid) Kind() monoid.Kind { return monoid.KindCustom }

func TestBytesStore_MergeUpdate_RejectsOversizedItemBeforeWriting(t *testing.T) {
	// DynamoDB caps an item at 400KB. Over it, PutItem fails non-retryably on
	// every attempt while Get keeps serving the last value that fit — a key
	// that has silently stopped updating but still reads as Present:true. The
	// store has to name that failure, and has to name it without spending the
	// round trip.
	const limit = 400 * 1024
	cases := []struct {
		name   string
		entity string
		delta  []byte
	}{
		{
			name:   "oversized value",
			entity: "global",
			delta:  make([]byte, limit+1),
		},
		{
			// The realistic trigger is key length, not K: a TopK that lifts an
			// unbounded raw wire field into its sketch keys overflows the row
			// on a couple of entries.
			name:   "oversized entity key",
			entity: strings.Repeat("e", limit-1024),
			delta:  make([]byte, 2048),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// GetItem falls through to the default empty-200 answer, which
			// reads as "no such row" — so the merge is Identity ⊕ delta.
			ft := &fakeTransport{}
			store := dynamodb.NewBytesStore(newFakeClient(t, ft, 1), "t", concatMonoid{})

			err := store.MergeUpdate(context.Background(),
				state.Key{Entity: tc.entity, Bucket: 0}, tc.delta, 0)
			if !errors.Is(err, dynamodb.ErrItemTooLarge) {
				t.Fatalf("MergeUpdate: got %v, want ErrItemTooLarge", err)
			}
			var tooBig *dynamodb.ItemTooLargeError
			if !errors.As(err, &tooBig) {
				t.Fatalf("MergeUpdate error is not *ItemTooLargeError: %#v", err)
			}
			if tooBig.Size <= limit {
				t.Errorf("reported size: got %d, want > %d", tooBig.Size, limit)
			}
			if tooBig.Key.Entity != tc.entity {
				t.Errorf("reported entity: got %d bytes, want %d bytes",
					len(tooBig.Key.Entity), len(tc.entity))
			}
			// Pre-flight means pre-flight: the oversized write must never
			// reach DynamoDB, and must not burn the CAS retry budget.
			if got := ft.putItemCalls.Load(); got != 0 {
				t.Errorf("PutItem calls: got %d, want 0 (the guard must run before the write)", got)
			}
			if got := ft.getItemCalls.Load(); got != 1 {
				t.Errorf("GetItem calls: got %d, want 1 (an unwritable item must not retry)", got)
			}
		})
	}
}

func TestBytesStore_MergeUpdate_CASConflictExhaustsInjectedRetries(t *testing.T) {
	// Every PutItem loses the CAS race. The store should spend exactly its
	// configured budget of read-combine-write cycles, count each loss, and
	// surface a typed ErrMaxRetriesExceeded rather than an opaque DDB string.
	cases := []struct {
		name        string
		opts        []dynamodb.BytesStoreOption
		wantRetries int64
	}{
		{name: "default budget", wantRetries: 8},
		{name: "lowered budget", opts: []dynamodb.BytesStoreOption{dynamodb.WithCASRetries(3)}, wantRetries: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var backoffCalls int
			ft := &fakeTransport{
				handleOp: func(target string, _ int, _ []byte) (*http.Response, error) {
					switch target {
					case "DynamoDB_20120810.GetItem":
						// An existing row, so the CAS path takes the
						// "#ver = :v" branch a peer can invalidate.
						return okResponse(`{"Item":{"pk":{"S":"hot"},"sk":{"N":"0"},` +
							`"v":{"B":"AAEC"},"ver":{"N":"7"}}}`), nil
					case "DynamoDB_20120810.PutItem":
						return ddbErrorResponse("ConditionalCheckFailedException"), nil
					}
					return okResponse(`{}`), nil
				},
			}
			rec := metrics.NewInMemory()
			opts := append([]dynamodb.BytesStoreOption{
				dynamodb.WithCASMetrics(rec, "hotkey_pipeline"),
				// Stand in for the ~17s of jittered backoff the default
				// schedule would spend. A short context deadline would not
				// work here: it surfaces ctx.Err() instead of the retry
				// outcome under test.
				dynamodb.WithCASBackoff(func(context.Context, int) error {
					backoffCalls++
					return nil
				}),
			}, tc.opts...)
			store := dynamodb.NewBytesStore(newFakeClient(t, ft, 1), "t", concatMonoid{}, opts...)

			err := store.MergeUpdate(context.Background(),
				state.Key{Entity: "hot", Bucket: 0}, []byte("x"), 0)
			if !errors.Is(err, dynamodb.ErrMaxRetriesExceeded) {
				t.Fatalf("MergeUpdate: got %v, want ErrMaxRetriesExceeded", err)
			}
			if got := ft.getItemCalls.Load(); got != tc.wantRetries {
				t.Errorf("GetItem calls: got %d, want %d", got, tc.wantRetries)
			}
			if got := ft.putItemCalls.Load(); got != tc.wantRetries {
				t.Errorf("PutItem calls: got %d, want %d", got, tc.wantRetries)
			}
			if got := int64(backoffCalls); got != tc.wantRetries-1 {
				t.Errorf("backoff waits: got %d, want %d (no wait before the first attempt)",
					got, tc.wantRetries-1)
			}
			// Without this counter, a key that burns its whole budget and
			// dead-letters is indistinguishable in the metrics from one whose
			// writes were simply failing.
			if got := rec.SnapshotOne("hotkey_pipeline:cas_conflict").EventsProcessed; got != uint64(tc.wantRetries) {
				t.Errorf("hotkey_pipeline:cas_conflict events: got %d, want %d", got, tc.wantRetries)
			}
		})
	}
}
