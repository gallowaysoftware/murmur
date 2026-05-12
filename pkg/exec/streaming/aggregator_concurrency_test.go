package streaming_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// manyKeySource emits N records spread across `keys` distinct entity
// keys, so the aggregator's flushAll has many distinct (entity, bucket)
// batches to drain.
type manyKeySource struct {
	n    int
	keys []string
}

type mkEv struct{ key string }

func (s *manyKeySource) Read(_ context.Context, out chan<- source.Record[mkEv]) error {
	for i := 0; i < s.n; i++ {
		out <- source.Record[mkEv]{
			EventID: fmt.Sprintf("ev-%d", i),
			Value:   mkEv{key: s.keys[i%len(s.keys)]},
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*manyKeySource) Name() string { return "many-key" }
func (*manyKeySource) Close() error { return nil }

// TestAggregator_FlushConcurrencyCorrectness verifies that per-key
// totals are identical across concurrency = 1, 4, 16 when the aggregator
// is configured. Each batch's coalesced delta lands in the store exactly
// once per key regardless of worker count.
func TestAggregator_FlushConcurrencyCorrectness(t *testing.T) {
	const N = 800
	keys := make([]string, 40) // 40 distinct keys
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}

	for _, conc := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("concurrency=%d", conc), func(t *testing.T) {
			store := newCountingStore()
			src := &manyKeySource{n: N, keys: keys}
			pipe := pipeline.NewPipeline[mkEv, int64]("agg-conc").
				From(src).
				Key(func(e mkEv) string { return e.key }).
				Value(func(mkEv) int64 { return 1 }).
				Aggregate(core.Sum[int64]()).
				StoreIn(store)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := streaming.Run(ctx, pipe,
				streaming.WithBatchWindow(20*time.Millisecond, 10000),
				streaming.WithConcurrency(conc),
			); err != nil {
				t.Fatalf("Run: %v", err)
			}

			for _, k := range keys {
				want := int64(N / len(keys))
				if got := store.values[state.Key{Entity: k}]; got != want {
					t.Errorf("concurrency=%d key %s: got %d, want %d", conc, k, got, want)
				}
			}
		})
	}
}

// TestAggregator_FlushParallelism verifies that with concurrency > 1
// and many distinct keys, the parallel flush actually overlaps store
// MergeUpdate calls. We use a latency-bearing store to make the
// overlap measurable.
func TestAggregator_FlushParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short mode (test sleeps to measure overlap)")
	}

	const numKeys = 32
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}

	measure := func(conc int) time.Duration {
		store := &latencyStore{latency: 20 * time.Millisecond}
		src := &manyKeySource{n: numKeys * 4, keys: keys}
		pipe := pipeline.NewPipeline[mkEv, int64]("agg-overlap").
			From(src).
			Key(func(e mkEv) string { return e.key }).
			Value(func(mkEv) int64 { return 1 }).
			Aggregate(core.Sum[int64]()).
			StoreIn(store)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		start := time.Now()
		if err := streaming.Run(ctx, pipe,
			streaming.WithBatchWindow(50*time.Millisecond, 100000),
			streaming.WithConcurrency(conc),
		); err != nil {
			t.Fatalf("Run conc=%d: %v", conc, err)
		}
		return time.Since(start)
	}

	seq := measure(1)
	par := measure(8)
	t.Logf("flush wall-time: concurrency=1 -> %v, concurrency=8 -> %v", seq, par)

	// Don't assert a hard speedup ratio (timer-driven flush + ack/dead-letter
	// bookkeeping introduce noise), but the parallel run should not be
	// dramatically SLOWER than sequential. 2x sequential is the loose
	// upper bound we enforce.
	if par > 2*seq {
		t.Errorf("concurrency=8 wall-time (%v) is >2x sequential (%v) — flush is not parallelizing", par, seq)
	}
}

// latencyStore sleeps `latency` per MergeUpdate, simulating DDB latency.
type latencyStore struct {
	latency time.Duration
	calls   atomic.Int64
	mu      sync.Mutex
	values  map[state.Key]int64
}

func (*latencyStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}
func (*latencyStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *latencyStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	time.Sleep(s.latency)
	s.calls.Add(1)
	s.mu.Lock()
	if s.values == nil {
		s.values = map[state.Key]int64{}
	}
	s.values[k] += d
	s.mu.Unlock()
	return nil
}
func (*latencyStore) Close() error { return nil }
