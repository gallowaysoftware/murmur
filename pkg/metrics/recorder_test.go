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
