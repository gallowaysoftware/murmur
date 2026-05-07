// Package metrics is Murmur's observability surface. The Recorder interface is what
// runtimes call to record per-pipeline events, errors, and latencies; the recorder
// implementation decides what to do with them (drop them, push to Prometheus, etc).
//
// Implementations:
//
//   - Noop: discards everything. The default when no recorder is configured.
//   - InMemory: keeps per-pipeline running stats and bounded latency histograms in
//     RAM. Powers the admin REST API and the web UI's "live metrics" cards. Suitable
//     for single-process workers; for multi-replica deployments aggregate via your
//     observability stack of choice.
package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Recorder is the abstraction runtimes call to publish observability events. Methods
// must be safe for concurrent use; implementations should aim for nanosecond-cost on
// the hot path so wrapping a Counter pipeline doesn't reshape its throughput.
type Recorder interface {
	// RecordEvent is called once per record successfully processed.
	RecordEvent(pipeline string)

	// RecordError is called when processing a record fails. The runtime decides
	// whether to retry, drop, or surface the error.
	RecordError(pipeline string, err error)

	// RecordLatency records the duration of a named operation. Typical names:
	// "store_merge", "cache_merge", "ack". Histograms expose p50/p95/p99.
	RecordLatency(pipeline string, op string, d time.Duration)
}

// Noop discards all metrics. Useful as a default and in tests.
type Noop struct{}

func (Noop) RecordEvent(string)                          {}
func (Noop) RecordError(string, error)                   {}
func (Noop) RecordLatency(string, string, time.Duration) {}

// PipelineStats is an immutable snapshot of metrics for a single pipeline.
type PipelineStats struct {
	Pipeline        string
	EventsProcessed uint64
	Errors          uint64
	LastEventAt     time.Time
	LastErrorAt     time.Time
	LastError       string
	// Latencies maps op name (e.g. "store_merge") to p50/p95/p99 over the last N samples.
	Latencies map[string]LatencyStats
}

// LatencyStats is a histogram summary in milliseconds.
type LatencyStats struct {
	N   int
	P50 float64
	P95 float64
	P99 float64
	Max float64
}

// InMemory is a process-local Recorder. Concurrent-safe; bounded memory: each op's
// latency window is capped at MaxLatencySamples.
type InMemory struct {
	mu     sync.RWMutex
	stats  map[string]*pipelineMetrics
	maxLat int
}

type pipelineMetrics struct {
	events      atomic.Uint64
	errors      atomic.Uint64
	lastEventAt atomic.Int64 // Unix nanoseconds
	lastErrorAt atomic.Int64
	lastError   atomic.Value // string, may be empty

	latMu sync.Mutex
	lats  map[string]*ringBuffer
}

const defaultMaxLatencySamples = 4096

// NewInMemory returns an InMemory recorder.
func NewInMemory() *InMemory {
	return &InMemory{
		stats:  make(map[string]*pipelineMetrics),
		maxLat: defaultMaxLatencySamples,
	}
}

func (m *InMemory) get(name string) *pipelineMetrics {
	m.mu.RLock()
	pm, ok := m.stats[name]
	m.mu.RUnlock()
	if ok {
		return pm
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pm, ok := m.stats[name]; ok {
		return pm
	}
	pm = &pipelineMetrics{lats: make(map[string]*ringBuffer)}
	m.stats[name] = pm
	return pm
}

// RecordEvent increments the events-processed counter for the given pipeline.
func (m *InMemory) RecordEvent(pipeline string) {
	pm := m.get(pipeline)
	pm.events.Add(1)
	pm.lastEventAt.Store(time.Now().UnixNano())
}

// RecordError increments the error counter and stores the most recent error string.
func (m *InMemory) RecordError(pipeline string, err error) {
	pm := m.get(pipeline)
	pm.errors.Add(1)
	pm.lastErrorAt.Store(time.Now().UnixNano())
	if err != nil {
		pm.lastError.Store(err.Error())
	}
}

// RecordLatency stores a duration sample in the ring buffer for (pipeline, op).
func (m *InMemory) RecordLatency(pipeline string, op string, d time.Duration) {
	pm := m.get(pipeline)
	pm.latMu.Lock()
	rb, ok := pm.lats[op]
	if !ok {
		rb = newRingBuffer(m.maxLat)
		pm.lats[op] = rb
	}
	rb.push(float64(d.Microseconds()) / 1000.0) // milliseconds
	pm.latMu.Unlock()
}

// Snapshot returns a point-in-time view of all pipeline metrics. Suitable for the
// admin REST API; copies are taken so callers can't mutate internal state.
func (m *InMemory) Snapshot() []PipelineStats {
	m.mu.RLock()
	names := make([]string, 0, len(m.stats))
	for n := range m.stats {
		names = append(names, n)
	}
	m.mu.RUnlock()
	sort.Strings(names)

	out := make([]PipelineStats, 0, len(names))
	for _, name := range names {
		out = append(out, m.snapshotOne(name))
	}
	return out
}

// SnapshotOne returns the snapshot for a single pipeline.
func (m *InMemory) SnapshotOne(name string) PipelineStats {
	return m.snapshotOne(name)
}

func (m *InMemory) snapshotOne(name string) PipelineStats {
	pm := m.get(name)
	s := PipelineStats{
		Pipeline:        name,
		EventsProcessed: pm.events.Load(),
		Errors:          pm.errors.Load(),
		Latencies:       make(map[string]LatencyStats),
	}
	if t := pm.lastEventAt.Load(); t > 0 {
		s.LastEventAt = time.Unix(0, t)
	}
	if t := pm.lastErrorAt.Load(); t > 0 {
		s.LastErrorAt = time.Unix(0, t)
	}
	if v := pm.lastError.Load(); v != nil {
		s.LastError, _ = v.(string)
	}
	pm.latMu.Lock()
	for op, rb := range pm.lats {
		s.Latencies[op] = rb.stats()
	}
	pm.latMu.Unlock()
	return s
}

// --- ringBuffer for bounded latency samples ---

type ringBuffer struct {
	buf  []float64
	pos  int
	full bool
}

func newRingBuffer(cap int) *ringBuffer {
	return &ringBuffer{buf: make([]float64, cap)}
}

func (r *ringBuffer) push(v float64) {
	r.buf[r.pos] = v
	r.pos = (r.pos + 1) % len(r.buf)
	if r.pos == 0 {
		r.full = true
	}
}

func (r *ringBuffer) snapshot() []float64 {
	if r.full {
		out := make([]float64, len(r.buf))
		copy(out, r.buf)
		return out
	}
	out := make([]float64, r.pos)
	copy(out, r.buf[:r.pos])
	return out
}

func (r *ringBuffer) stats() LatencyStats {
	s := r.snapshot()
	n := len(s)
	if n == 0 {
		return LatencyStats{}
	}
	sort.Float64s(s)
	pct := func(p float64) float64 {
		if n == 0 {
			return 0
		}
		idx := int(float64(n-1) * p)
		return s[idx]
	}
	return LatencyStats{
		N:   n,
		P50: pct(0.50),
		P95: pct(0.95),
		P99: pct(0.99),
		Max: s[n-1],
	}
}
