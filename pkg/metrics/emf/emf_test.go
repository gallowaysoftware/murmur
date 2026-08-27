package emf_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics/emf"
)

// syncBuf is an io.Writer safe for concurrent use by the flush goroutine and
// the test.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// docs parses the newline-delimited EMF documents written so far.
func docs(t *testing.T, s *syncBuf) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("emitted line is not valid JSON: %v\nline: %s", err, line)
		}
		out = append(out, m)
	}
	return out
}

func newRec(out *syncBuf) *emf.Recorder {
	// A long interval so only Close() triggers the flush, keeping tests
	// deterministic.
	return emf.New(emf.Config{Out: out, FlushInterval: time.Hour})
}

func TestRecorder_EmitsValidEMFEnvelope(t *testing.T) {
	out := &syncBuf{}
	r := newRec(out)
	r.RecordEvent("orders")
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d := docs(t, out)
	if len(d) != 1 {
		t.Fatalf("documents: got %d, want 1", len(d))
	}
	doc := d[0]

	aws, ok := doc["_aws"].(map[string]any)
	if !ok {
		t.Fatal("missing _aws block — CloudWatch will not extract any metric")
	}
	if _, ok := aws["Timestamp"]; !ok {
		t.Error("_aws.Timestamp missing")
	}
	cw, ok := aws["CloudWatchMetrics"].([]any)
	if !ok || len(cw) != 1 {
		t.Fatalf("CloudWatchMetrics: %#v", aws["CloudWatchMetrics"])
	}
	entry := cw[0].(map[string]any)
	if entry["Namespace"] != emf.DefaultNamespace {
		t.Errorf("namespace: got %v, want %v", entry["Namespace"], emf.DefaultNamespace)
	}
	if doc["Pipeline"] != "orders" {
		t.Errorf("Pipeline dimension: got %v", doc["Pipeline"])
	}
	if doc["EventsProcessed"] != float64(1) {
		t.Errorf("EventsProcessed: got %v, want 1", doc["EventsProcessed"])
	}
	// Every declared metric must have a matching top-level member, or
	// CloudWatch silently drops it.
	for _, m := range entry["Metrics"].([]any) {
		name := m.(map[string]any)["Name"].(string)
		if _, ok := doc[name]; !ok {
			t.Errorf("metric %q declared but has no value member", name)
		}
	}
}

func TestRecorder_SubEventsBecomeTheirOwnMetric(t *testing.T) {
	// This is what makes dedup_skip / dedup_release observable at all. Emitting
	// "orders:dedup_skip" verbatim as a Pipeline dimension would fragment the
	// dashboard into one dimension value per sub-event.
	out := &syncBuf{}
	r := newRec(out)
	r.RecordEvent("orders")
	r.RecordEvent("orders:dedup_skip")
	r.RecordEvent("orders:dedup_skip")
	r.RecordEvent("orders:dedup_release")
	_ = r.Close()

	d := docs(t, out)
	if len(d) != 1 {
		t.Fatalf("documents: got %d, want 1 (all sub-events belong to one pipeline)", len(d))
	}
	doc := d[0]
	if doc["Pipeline"] != "orders" {
		t.Errorf("Pipeline: got %v, want orders", doc["Pipeline"])
	}
	if doc["DedupSkip"] != float64(2) {
		t.Errorf("DedupSkip: got %v, want 2", doc["DedupSkip"])
	}
	if doc["DedupRelease"] != float64(1) {
		t.Errorf("DedupRelease: got %v, want 1", doc["DedupRelease"])
	}
	if doc["EventsProcessed"] != float64(1) {
		t.Errorf("EventsProcessed: got %v, want 1 (sub-events must not inflate it)", doc["EventsProcessed"])
	}
}

func TestRecorder_LatencyIsAStatisticSet(t *testing.T) {
	out := &syncBuf{}
	r := newRec(out)
	r.RecordLatency("orders", "store_merge", 10*time.Millisecond)
	r.RecordLatency("orders", "store_merge", 30*time.Millisecond)
	_ = r.Close()

	doc := docs(t, out)[0]
	st, ok := doc["StoreMergeLatency"].(map[string]any)
	if !ok {
		t.Fatalf("StoreMergeLatency is not a statistic set: %#v", doc["StoreMergeLatency"])
	}
	if st["Count"] != float64(2) {
		t.Errorf("Count: got %v, want 2", st["Count"])
	}
	if st["Sum"] != float64(40) {
		t.Errorf("Sum: got %v ms, want 40", st["Sum"])
	}
	if st["Min"] != float64(10) || st["Max"] != float64(30) {
		t.Errorf("Min/Max: got %v/%v, want 10/30", st["Min"], st["Max"])
	}
}

func TestRecorder_ErrorsCountTowardThePipelineTotal(t *testing.T) {
	// A sub-scoped error must still move the pipeline's Errors metric, or an
	// alarm on Errors misses it entirely.
	out := &syncBuf{}
	r := newRec(out)
	r.RecordError("orders", errors.New("boom"))
	r.RecordError("orders:dedup_release_failed", errors.New("ddb down"))
	_ = r.Close()

	doc := docs(t, out)[0]
	if doc["Errors"] != float64(2) {
		t.Errorf("Errors: got %v, want 2", doc["Errors"])
	}
	if doc["DedupReleaseFailedErrors"] != float64(1) {
		t.Errorf("DedupReleaseFailedErrors: got %v, want 1", doc["DedupReleaseFailedErrors"])
	}
}

func TestRecorder_FlushResetsAggregates(t *testing.T) {
	// Counters are deltas per flush. If they accumulated, CloudWatch Sum over
	// a window would double-count every prior interval.
	out := &syncBuf{}
	r := newRec(out)
	r.RecordEvent("orders")
	r.Flush()
	r.RecordEvent("orders")
	_ = r.Close()

	d := docs(t, out)
	if len(d) != 2 {
		t.Fatalf("documents: got %d, want 2", len(d))
	}
	for i, doc := range d {
		if doc["EventsProcessed"] != float64(1) {
			t.Errorf("doc %d EventsProcessed: got %v, want 1", i, doc["EventsProcessed"])
		}
	}
}

func TestRecorder_EmptyFlushWritesNothing(t *testing.T) {
	// An idle pipeline must not emit — otherwise a quiet worker still bills
	// CloudWatch Logs ingestion every interval for the whole soak.
	out := &syncBuf{}
	r := newRec(out)
	r.Flush()
	_ = r.Close()
	if s := out.String(); s != "" {
		t.Errorf("idle recorder wrote %q, want nothing", s)
	}
}

func TestRecorder_ExtraDimensionsAreDeclaredAndValued(t *testing.T) {
	out := &syncBuf{}
	r := emf.New(emf.Config{Out: out, FlushInterval: time.Hour, Dimensions: map[string]string{"Env": "soak"}})
	r.RecordEvent("orders")
	_ = r.Close()

	doc := docs(t, out)[0]
	if doc["Env"] != "soak" {
		t.Errorf("Env value: got %v", doc["Env"])
	}
	dims := doc["_aws"].(map[string]any)["CloudWatchMetrics"].([]any)[0].(map[string]any)["Dimensions"].([]any)[0].([]any)
	var found bool
	for _, d := range dims {
		if d == "Env" {
			found = true
		}
	}
	if !found {
		t.Errorf("Env not declared as a dimension: %v — CloudWatch would ignore it", dims)
	}
}

func TestRecorder_BatchRecordsCountAndLatency(t *testing.T) {
	out := &syncBuf{}
	r := newRec(out)
	r.RecordBatch("orders", "streaming", 100, 50*time.Millisecond)
	r.RecordBatch("orders", "streaming", 40, 10*time.Millisecond)
	_ = r.Close()

	doc := docs(t, out)[0]
	if doc["BatchesProcessed"] != float64(2) {
		t.Errorf("BatchesProcessed: got %v, want 2", doc["BatchesProcessed"])
	}
	if doc["BatchRecords"] != float64(140) {
		t.Errorf("BatchRecords: got %v, want 140", doc["BatchRecords"])
	}
	if _, ok := doc["BatchStreamingLatency"].(map[string]any); !ok {
		t.Errorf("BatchStreamingLatency missing: %#v", doc["BatchStreamingLatency"])
	}
}

func TestRecorder_ConcurrentUseIsSafe(t *testing.T) {
	out := &syncBuf{}
	r := newRec(out)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.RecordEvent("orders")
				r.RecordLatency("orders", "store_merge", time.Millisecond)
				r.RecordEvent("orders:dedup_skip")
			}
		}()
	}
	wg.Wait()
	_ = r.Close()

	doc := docs(t, out)[0]
	if doc["EventsProcessed"] != float64(3200) {
		t.Errorf("EventsProcessed: got %v, want 3200 (lost updates under concurrency)", doc["EventsProcessed"])
	}
	if doc["DedupSkip"] != float64(3200) {
		t.Errorf("DedupSkip: got %v, want 3200", doc["DedupSkip"])
	}
}

func TestRecorder_CloseIsIdempotent(t *testing.T) {
	r := emf.New(emf.Config{Out: &syncBuf{}, FlushInterval: time.Hour})
	if err := r.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second close must not panic or error: %v", err)
	}
}
