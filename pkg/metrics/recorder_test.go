package metrics_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
)

func TestInMemory_BasicCounters(t *testing.T) {
	m := metrics.NewInMemory()
	m.RecordEvent("page_views")
	m.RecordEvent("page_views")
	m.RecordEvent("page_views")
	m.RecordEvent("orders")
	m.RecordError("page_views", errors.New("kaboom"))

	all := m.Snapshot()
	if len(all) != 2 {
		t.Fatalf("want 2 pipelines, got %d", len(all))
	}
	for _, s := range all {
		switch s.Pipeline {
		case "page_views":
			if s.EventsProcessed != 3 {
				t.Errorf("page_views events: got %d, want 3", s.EventsProcessed)
			}
			if s.Errors != 1 {
				t.Errorf("page_views errors: got %d, want 1", s.Errors)
			}
			if s.LastError != "kaboom" {
				t.Errorf("page_views last error: got %q, want kaboom", s.LastError)
			}
		case "orders":
			if s.EventsProcessed != 1 || s.Errors != 0 {
				t.Errorf("orders unexpected: %+v", s)
			}
		}
	}
}

func TestInMemory_LatencyHistogram(t *testing.T) {
	m := metrics.NewInMemory()
	for i := 1; i <= 100; i++ {
		m.RecordLatency("p", "store_merge", time.Duration(i)*time.Millisecond)
	}
	s := m.SnapshotOne("p")
	lat, ok := s.Latencies["store_merge"]
	if !ok {
		t.Fatalf("latencies missing store_merge")
	}
	if lat.N != 100 {
		t.Fatalf("N: got %d, want 100", lat.N)
	}
	// p50 ≈ 50ms, p95 ≈ 95ms, p99 ≈ 99ms, max = 100ms.
	if lat.P50 < 49 || lat.P50 > 51 {
		t.Errorf("p50: got %f, want ~50", lat.P50)
	}
	if lat.P95 < 94 || lat.P95 > 96 {
		t.Errorf("p95: got %f, want ~95", lat.P95)
	}
	if lat.Max != 100 {
		t.Errorf("max: got %f, want 100", lat.Max)
	}
}

func TestInMemory_ConcurrentSafety(t *testing.T) {
	m := metrics.NewInMemory()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				m.RecordEvent("p")
				m.RecordLatency("p", "x", time.Microsecond)
			}
		}()
	}
	wg.Wait()
	s := m.SnapshotOne("p")
	if s.EventsProcessed != 8000 {
		t.Errorf("EventsProcessed: got %d, want 8000", s.EventsProcessed)
	}
}

func TestInMemory_RecordBatch(t *testing.T) {
	m := metrics.NewInMemory()
	// Three batches of 10/20/30 records under the streaming mode and one
	// of 7 under the bootstrap mode.
	m.RecordBatch("p", metrics.ModeStreaming, 10, 100*time.Millisecond)
	m.RecordBatch("p", metrics.ModeStreaming, 20, 200*time.Millisecond)
	m.RecordBatch("p", metrics.ModeStreaming, 30, 300*time.Millisecond)
	m.RecordBatch("p", metrics.ModeBootstrap, 7, 50*time.Millisecond)

	// Per-mode event counts live under a synthetic pipeline name; this is
	// how a dashboard separates streaming vs bootstrap throughput while
	// preserving the unmodified pipeline name for per-op latencies.
	stream := m.SnapshotOne("p:batch:streaming")
	if stream.EventsProcessed != 60 {
		t.Errorf("streaming events: got %d, want 60", stream.EventsProcessed)
	}
	boot := m.SnapshotOne("p:batch:bootstrap")
	if boot.EventsProcessed != 7 {
		t.Errorf("bootstrap events: got %d, want 7", boot.EventsProcessed)
	}

	// Batch latency lives on the unmodified pipeline name under the synthetic
	// "batch_<mode>" op; dashboards can plot p50/p95/p99 by mode.
	p := m.SnapshotOne("p")
	streamLat, ok := p.Latencies["batch_streaming"]
	if !ok {
		t.Fatalf("expected batch_streaming latency op")
	}
	if streamLat.N != 3 {
		t.Errorf("streaming latency samples: got %d, want 3", streamLat.N)
	}
	bootLat, ok := p.Latencies["batch_bootstrap"]
	if !ok {
		t.Fatalf("expected batch_bootstrap latency op")
	}
	if bootLat.N != 1 {
		t.Errorf("bootstrap latency samples: got %d, want 1", bootLat.N)
	}
}

func TestInMemory_RecordBatch_ZeroCountSkipsEventEmit(t *testing.T) {
	// A 0-count batch with non-zero duration should NOT inflate the event
	// counter (idle ticks must not pollute throughput dashboards) but the
	// duration sample should still be discarded too — zero events in zero
	// time is a noise observation.
	m := metrics.NewInMemory()
	m.RecordBatch("p", metrics.ModeStreaming, 0, 50*time.Millisecond)
	if got := m.SnapshotOne("p:batch:streaming").EventsProcessed; got != 0 {
		t.Errorf("events on zero-count batch: got %d, want 0", got)
	}
}

func TestNoop_RecordBatch_ZeroCost(t *testing.T) {
	// The Noop recorder MUST satisfy Recorder including the new
	// RecordBatch method and remain a true no-op. Compile-time check
	// (interface satisfaction) + a runtime smoke test.
	var r metrics.Recorder = metrics.Noop{}
	r.RecordBatch("p", "streaming", 1_000_000, time.Hour)
}
