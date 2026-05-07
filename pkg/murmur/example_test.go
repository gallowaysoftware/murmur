package murmur_test

// Examples in this file render on pkg.go.dev as the canonical usage
// reference. Keep them runnable (no fake imports), short (one focused use
// case each), and free of pkg/state/dynamodb to avoid pulling integration
// dependencies into the doc set.

import (
	"context"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/murmur"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// memStore is a tiny in-memory state.Store[int64] used only by the examples
// below so they are self-contained on pkg.go.dev. Production code uses
// pkg/state/dynamodb.Int64SumStore.
type memStore struct{ m map[state.Key]int64 }

func newMemStore() *memStore { return &memStore{m: map[state.Key]int64{}} }
func (s *memStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *memStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *memStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.m[k] += d
	return nil
}
func (s *memStore) Close() error { return nil }

// nopSource yields one record per Read call, then closes. Stand-in for a real
// kafka.Source / kinesis.Source so the example doesn't need a broker running.
type nopEvent struct{ K string }
type nopSource struct{ events []nopEvent }

func (s *nopSource) Read(_ context.Context, out chan<- source.Record[nopEvent]) error {
	for i, e := range s.events {
		out <- source.Record[nopEvent]{
			EventID: "evt-" + e.K + string(rune('0'+i)),
			Value:   e,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*nopSource) Name() string { return "nop" }
func (*nopSource) Close() error { return nil }

// ExampleCounter shows the canonical Murmur counter pipeline — one event
// contributes 1 to a per-key Sum, daily windowing, write-through to a state
// store, run via streaming.Run.
func ExampleCounter() {
	store := newMemStore()

	pipe := murmur.Counter[nopEvent]("page_views").
		From(&nopSource{events: []nopEvent{{"a"}, {"a"}, {"b"}}}).
		KeyBy(func(e nopEvent) string { return e.K }).
		Daily(7 * 24 * time.Hour).
		StoreIn(store).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = streaming.Run(ctx, pipe)

	// store.m now holds {a: 2, b: 1} keyed by today's bucket. Production
	// code reads through query.GetWindow / GetRange rather than the store
	// directly.
}

// ExampleUniqueCount shows the HLL preset — one element per event lifted into
// a per-key cardinality sketch. The store value type is []byte so a custom
// store is needed; production code uses pkg/state/dynamodb.BytesStore.
func ExampleUniqueCount() {
	// Real code substitutes a state.Store[[]byte] backed by DDB BytesStore.
	var store state.Store[[]byte]

	_ = murmur.UniqueCount[nopEvent]("unique_visitors", func(e nopEvent) []byte {
		return []byte(e.K) // the visitor identifier
	}).
		KeyBy(func(e nopEvent) string { return "global" }).
		Hourly(24 * time.Hour).
		StoreIn(store).
		Build()
}

// ExampleTopN shows the Misra-Gries TopK preset — useful for "top N most
// active X over a window."
func ExampleTopN() {
	var store state.Store[[]byte]

	_ = murmur.TopN[nopEvent]("recently_interacted", 10, func(e nopEvent) string {
		return e.K // the entity id
	}).
		KeyBy(func(e nopEvent) string { return "global" }).
		Daily(7 * 24 * time.Hour).
		StoreIn(store).
		Build()
}

// ExampleRunStreamingWorker shows the boilerplate-eating worker entry point.
// The recorder + dedup options are forwarded to streaming.Run so a typical
// production main is ~15 lines.
func ExampleRunStreamingWorker() {
	store := newMemStore()
	src := &nopSource{events: []nopEvent{{"a"}}}

	pipe := murmur.Counter[nopEvent]("page_views").
		From(src).KeyBy(func(e nopEvent) string { return e.K }).
		StoreIn(store).Build()

	rec := metrics.NewInMemory()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = murmur.RunStreamingWorker(ctx, pipe,
		streaming.WithMetrics(rec),
		streaming.WithMaxAttempts(5),
	)
	// Real code would also pass streaming.WithDedup(deduper) and
	// streaming.WithDeadLetter(fn) for production-grade at-least-once.
}
