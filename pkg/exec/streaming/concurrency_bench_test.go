package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// benchSource emits N events spread across `keys` distinct keys, then
// closes. Used by all concurrency benchmarks below.
type benchEv struct {
	key string
}

type benchSourceFor struct {
	n    int
	keys []string
}

func (s *benchSourceFor) Read(_ context.Context, out chan<- source.Record[benchEv]) error {
	for i := 0; i < s.n; i++ {
		k := s.keys[i%len(s.keys)]
		out <- source.Record[benchEv]{
			EventID: fmt.Sprintf("ev-%d", i),
			Value:   benchEv{key: k},
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*benchSourceFor) Name() string { return "bench" }
func (*benchSourceFor) Close() error { return nil }

// shardedAtomicStore is a benchmark Store[int64] using sync/atomic on
// per-key int64 slots. Same-key records always route to the same
// worker via WithConcurrency's key-hash routing, so the stores never
// actually contend cross-worker for the same slot — but using atomic
// keeps the benchmark honest if routing ever drifts.
type shardedAtomicStore struct {
	values []atomicInt64Slot
	keys   map[string]int
}

type atomicInt64Slot struct{ v int64 }

func newShardedAtomicStore(keys []string) *shardedAtomicStore {
	s := &shardedAtomicStore{
		values: make([]atomicInt64Slot, len(keys)),
		keys:   make(map[string]int, len(keys)),
	}
	for i, k := range keys {
		s.keys[k] = i
	}
	return s
}
func (s *shardedAtomicStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	if i, ok := s.keys[k.Entity]; ok {
		return s.values[i].v, true, nil
	}
	return 0, false, nil
}
func (s *shardedAtomicStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *shardedAtomicStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	if i, ok := s.keys[k.Entity]; ok {
		s.values[i].v += d
	}
	return nil
}
func (*shardedAtomicStore) Close() error { return nil }

// runBench sets up the pipeline and runs streaming.Run. The benchmark
// measures wall time per-event from start of Read to end of Run; the
// store does close to nothing so the dominant cost is the runtime
// itself.
func runBench(b *testing.B, concurrency int) {
	const N = 100_000
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	store := newShardedAtomicStore(keys)
	src := &benchSourceFor{n: N, keys: keys}
	pipe := pipeline.NewPipeline[benchEv, int64]("bench").
		From(src).
		Key(func(e benchEv) string { return e.key }).
		Value(func(benchEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.ResetTimer()
	b.ReportAllocs()
	if concurrency > 1 {
		_ = streaming.Run(ctx, pipe, streaming.WithConcurrency(concurrency))
	} else {
		_ = streaming.Run(ctx, pipe)
	}
	b.StopTimer()
	// Report events / second from the wall time the benchmark consumed.
	// Each iteration of `for i:=0; i<b.N; i++` is one full N-record run,
	// so events processed = b.N * N.
	if b.Elapsed() > 0 {
		eventsPerSec := float64(b.N*N) / b.Elapsed().Seconds()
		b.ReportMetric(eventsPerSec, "events/sec")
	}
}

// Benchmarks at increasing concurrency. The defaults are reported
// for context; production runs against real DDB will be I/O-bound
// rather than CPU-bound so the relative scaling is what matters.

func BenchmarkConcurrency_1(b *testing.B)  { runBench(b, 1) }
func BenchmarkConcurrency_2(b *testing.B)  { runBench(b, 2) }
func BenchmarkConcurrency_4(b *testing.B)  { runBench(b, 4) }
func BenchmarkConcurrency_8(b *testing.B)  { runBench(b, 8) }
func BenchmarkConcurrency_16(b *testing.B) { runBench(b, 16) }

// slowStore simulates an I/O-bound store (DDB) by sleeping `latency`
// per MergeUpdate. Lets the benchmark show the actual speedup that
// concurrency delivers under realistic store latency.
type slowStore struct {
	latency time.Duration
	keys    map[string]int
	values  []atomicInt64Slot
}

func newSlowStore(keys []string, latency time.Duration) *slowStore {
	s := &slowStore{
		latency: latency,
		keys:    make(map[string]int, len(keys)),
		values:  make([]atomicInt64Slot, len(keys)),
	}
	for i, k := range keys {
		s.keys[k] = i
	}
	return s
}
func (s *slowStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}
func (s *slowStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *slowStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	time.Sleep(s.latency)
	if i, ok := s.keys[k.Entity]; ok {
		s.values[i].v += d
	}
	return nil
}
func (*slowStore) Close() error { return nil }

// runBenchSlow simulates a workload where each store call takes
// `latency` (typical DDB UpdateItem at ~5ms, batched-throttled
// closer to 10–20ms). Concurrency should lift effective throughput
// linearly with N until the source-pump or dispatch becomes the
// bottleneck.
func runBenchSlow(b *testing.B, concurrency int, latency time.Duration) {
	const N = 200 // small N because each call sleeps
	keys := make([]string, 32)
	for i := range keys {
		keys[i] = fmt.Sprintf("k-%d", i)
	}
	store := newSlowStore(keys, latency)
	src := &benchSourceFor{n: N, keys: keys}
	pipe := pipeline.NewPipeline[benchEv, int64]("slow-bench").
		From(src).
		Key(func(e benchEv) string { return e.key }).
		Value(func(benchEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	b.ResetTimer()
	if concurrency > 1 {
		_ = streaming.Run(ctx, pipe, streaming.WithConcurrency(concurrency))
	} else {
		_ = streaming.Run(ctx, pipe)
	}
	b.StopTimer()
	if b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N*N)/b.Elapsed().Seconds(), "events/sec")
	}
}

// SlowStore benchmarks: 5ms latency per MergeUpdate (representative of
// DDB UpdateItem at moderate load). At N=200 records sequential, that's
// ~1 second; with 8 workers, ~125ms.
func BenchmarkSlowStore_Concurrency_1(b *testing.B)  { runBenchSlow(b, 1, 5*time.Millisecond) }
func BenchmarkSlowStore_Concurrency_4(b *testing.B)  { runBenchSlow(b, 4, 5*time.Millisecond) }
func BenchmarkSlowStore_Concurrency_8(b *testing.B)  { runBenchSlow(b, 8, 5*time.Millisecond) }
func BenchmarkSlowStore_Concurrency_16(b *testing.B) { runBenchSlow(b, 16, 5*time.Millisecond) }
