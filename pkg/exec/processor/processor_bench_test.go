package processor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Benchmarks for the processor's core merge path. Runs against an
// in-memory store so the measurements isolate processor overhead from
// store latency. Anchors the design-doc claim that the processor is
// ~sub-microsecond on the hot path.
//
// Run with:
//   go test -bench=. -benchmem ./pkg/exec/processor/...

// benchStore is a minimal Store[int64] that's pure CPU — no allocation,
// no locking visible to the benchmark loop.
type benchStore struct {
	mu sync.Mutex
	v  int64
}

func (s *benchStore) Get(_ context.Context, _ state.Key) (int64, bool, error) {
	return s.v, true, nil
}
func (s *benchStore) GetMany(_ context.Context, _ []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *benchStore) MergeUpdate(_ context.Context, _ state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	s.v += d
	s.mu.Unlock()
	return nil
}
func (*benchStore) Close() error { return nil }

func keyFnInt(_ int) string     { return "k" }
func valueFnInt(_ int) int64    { return 1 }
func keysFn4Int(_ int) []string { return []string{"k", "country", "global", "post"} }

// BenchmarkMergeOne_NoDedup measures the canonical hot-path: one record,
// one key, no Deduper. The bare retry/dedup/metrics-noop overhead.
func BenchmarkMergeOne_NoDedup(b *testing.B) {
	cfg := processor.Defaults() // Noop recorder
	store := &benchStore{}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = processor.MergeOne(context.Background(), &cfg, "bench",
			"ev-1", now, i, keyFnInt, valueFnInt, store, nil, nil)
	}
}

// BenchmarkMergeMany_FourKeys measures the hierarchical-rollup hot path.
// 4 keys per record (post/country/global/post-x-country) is the typical
// social-counting fan-out shape from doc/search-integration.md.
func BenchmarkMergeMany_FourKeys(b *testing.B) {
	cfg := processor.Defaults()
	store := &benchStore{}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = processor.MergeMany(context.Background(), &cfg, "bench",
			"ev-1", now, keysFn4Int(i), valueFnInt(i), store, nil, nil)
	}
}

// BenchmarkMergeOne_WithDedup measures the dedup-claim cost.
type benchDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (d *benchDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[id] {
		return false, nil
	}
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	d.seen[id] = true
	return true, nil
}
func (*benchDeduper) Close() error { return nil }

func BenchmarkMergeOne_WithDedup(b *testing.B) {
	cfg := processor.Defaults()
	cfg.Dedup = &benchDeduper{seen: make(map[string]bool, b.N)}
	store := &benchStore{}
	now := time.Now()

	// Use a unique EventID per iteration — exercises the actual map insert,
	// not the same-id-skips-merge fastpath.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eventID := "ev-" + intToStr(i)
		_ = processor.MergeOne(context.Background(), &cfg, "bench",
			eventID, now, i, keyFnInt, valueFnInt, store, nil, nil)
	}
}

// BenchmarkMergeOne_WithRecorder measures the cost of the InMemory
// metrics path on the hot path. Real production deployments use a
// CloudWatch / Prometheus exporter behind the InMemory; this isolates
// the in-process portion.
func BenchmarkMergeOne_WithRecorder(b *testing.B) {
	cfg := processor.Defaults()
	cfg.Recorder = metrics.NewInMemory()
	store := &benchStore{}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = processor.MergeOne(context.Background(), &cfg, "bench",
			"ev-1", now, i, keyFnInt, valueFnInt, store, nil, nil)
	}
}

// intToStr is the strconv.Itoa-equivalent without pulling strconv into
// the bench (to keep the alloc count honest).
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
