package kinesis_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is an in-memory state.Store[int64] for handler tests.
type fakeStore struct {
	mu sync.Mutex
	m  map[state.Key]int64
}

func newFakeStore() *fakeStore { return &fakeStore{m: map[state.Key]int64{}} }

func (s *fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (s *fakeStore) Close() error { return nil }

// flakyStore returns errFlaky on the first failuresEach calls per key, then
// succeeds. Mirrors the streaming runtime's flakyStore so retry semantics
// stay aligned.
type flakyStore struct {
	mu           sync.Mutex
	m            map[state.Key]int64
	attempts     map[state.Key]int
	failuresEach int
}

var errFlaky = errors.New("flaky store: transient")

func newFlakyStore(failuresEach int) *flakyStore {
	return &flakyStore{m: map[state.Key]int64{}, attempts: map[state.Key]int{}, failuresEach: failuresEach}
}
func (s *flakyStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *flakyStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *flakyStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[k]++
	if s.attempts[k] <= s.failuresEach {
		return errFlaky
	}
	s.m[k] += d
	return nil
}
func (*flakyStore) Close() error { return nil }

// memDeduper is a process-local Deduper for tests.
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newMemDeduper() *memDeduper { return &memDeduper{seen: map[string]bool{}} }
func (d *memDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}
func (*memDeduper) Close() error { return nil }

type page struct {
	PageID string `json:"page_id"`
}

func newPipe(store state.Store[int64]) *pipeline.Pipeline[page, int64] {
	return pipeline.NewPipeline[page, int64]("page_views").
		Key(func(p page) string { return p.PageID }).
		Value(func(page) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func mustEvent(t *testing.T, p page, seq, arn string) events.KinesisEventRecord {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return events.KinesisEventRecord{
		EventSourceArn: arn,
		Kinesis: events.KinesisRecord{
			Data:           b,
			SequenceNumber: seq,
			PartitionKey:   p.PageID,
		},
	}
}

func TestHandler_HappyPath(t *testing.T) {
	store := newFakeStore()
	rec := metrics.NewInMemory()
	h, err := kinesis.NewHandler(newPipe(store), kinesis.JSONDecoder[page](),
		kinesis.WithMetrics(rec),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		mustEvent(t, page{PageID: "a"}, "seq-1", "arn"),
		mustEvent(t, page{PageID: "a"}, "seq-2", "arn"),
		mustEvent(t, page{PageID: "b"}, "seq-3", "arn"),
	}}

	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0; got %+v", got, resp.BatchItemFailures)
	}
	if got, want := store.m[state.Key{Entity: "a"}], int64(2); got != want {
		t.Errorf("page a: got %d, want %d", got, want)
	}
	if got, want := store.m[state.Key{Entity: "b"}], int64(1); got != want {
		t.Errorf("page b: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("page_views").EventsProcessed != 3 {
		t.Errorf("events processed: got %d, want 3", rec.SnapshotOne("page_views").EventsProcessed)
	}
}

func TestHandler_RetriesAndRecovers(t *testing.T) {
	// Two failures per key, MaxAttempts=3 → every record lands by the third try.
	store := newFlakyStore(2)
	rec := metrics.NewInMemory()
	h, err := kinesis.NewHandler(newPipe(store), kinesis.JSONDecoder[page](),
		kinesis.WithMetrics(rec),
		kinesis.WithMaxAttempts(3),
		kinesis.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		mustEvent(t, page{PageID: "a"}, "seq-1", "arn"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0", got)
	}
	if store.m[state.Key{Entity: "a"}] != 1 {
		t.Errorf("page a: got %d, want 1", store.m[state.Key{Entity: "a"}])
	}
}

func TestHandler_ReportsExhaustedRetries(t *testing.T) {
	// Three failures per key, MaxAttempts=2 → record is reported as a
	// BatchItemFailure with its sequence number.
	store := newFlakyStore(10)
	h, err := kinesis.NewHandler(newPipe(store), kinesis.JSONDecoder[page](),
		kinesis.WithMaxAttempts(2),
		kinesis.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		mustEvent(t, page{PageID: "a"}, "seq-doomed", "arn:doomed"),
		mustEvent(t, page{PageID: "b"}, "seq-also-doomed", "arn:doomed"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 2 {
		t.Fatalf("BatchItemFailures = %d, want 2 (got %+v)", got, resp.BatchItemFailures)
	}
	wantSeqs := map[string]bool{"seq-doomed": true, "seq-also-doomed": true}
	for _, f := range resp.BatchItemFailures {
		if !wantSeqs[f.ItemIdentifier] {
			t.Errorf("unexpected failure ItemIdentifier %q", f.ItemIdentifier)
		}
	}
}

func TestHandler_DedupSkipsSecondInvocation(t *testing.T) {
	store := newFakeStore()
	dedup := newMemDeduper()
	rec := metrics.NewInMemory()
	h, err := kinesis.NewHandler(newPipe(store), kinesis.JSONDecoder[page](),
		kinesis.WithMetrics(rec),
		kinesis.WithDedup(dedup),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		mustEvent(t, page{PageID: "a"}, "seq-dup", "arn:dup"),
	}}
	for i := 0; i < 3; i++ {
		resp, err := h(context.Background(), evt)
		if err != nil {
			t.Fatalf("invocation %d: %v", i, err)
		}
		if got := len(resp.BatchItemFailures); got != 0 {
			t.Fatalf("invocation %d: failures = %d, want 0", i, got)
		}
	}
	// Three Lambda invocations all delivering the same record → only one merge.
	if got, want := store.m[state.Key{Entity: "a"}], int64(1); got != want {
		t.Errorf("after 3 dedup-protected invocations: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("page_views:dedup_skip").EventsProcessed != 2 {
		t.Errorf("dedup_skip events: got %d, want 2",
			rec.SnapshotOne("page_views:dedup_skip").EventsProcessed)
	}
}

func TestHandler_DecodeErrorCallback(t *testing.T) {
	store := newFakeStore()
	var got []string
	h, err := kinesis.NewHandler(newPipe(store), kinesis.JSONDecoder[page](),
		kinesis.WithDecodeErrorCallback(func(_ []byte, seq, _ string, _ error) {
			got = append(got, seq)
		}),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		{Kinesis: events.KinesisRecord{Data: []byte("not json"), SequenceNumber: "bad-seq"}},
		mustEvent(t, page{PageID: "a"}, "good-seq", "arn"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("decode error reported as BatchItemFailure (would loop forever); got %d", got)
	}
	if len(got) != 1 || got[0] != "bad-seq" {
		t.Errorf("decode callback received %v, want [bad-seq]", got)
	}
	if store.m[state.Key{Entity: "a"}] != 1 {
		t.Errorf("good record dropped after decode failure: got %d, want 1",
			store.m[state.Key{Entity: "a"}])
	}
}

func TestHandler_NilPipeline(t *testing.T) {
	if _, err := kinesis.NewHandler[page, int64](nil, kinesis.JSONDecoder[page]()); err == nil {
		t.Fatal("NewHandler(nil): expected error")
	}
}

func TestHandler_NilDecode(t *testing.T) {
	store := newFakeStore()
	if _, err := kinesis.NewHandler[page, int64](newPipe(store), nil); err == nil {
		t.Fatal("NewHandler(decode=nil): expected error")
	}
}

func TestHandler_PipelineNotBuilt(t *testing.T) {
	// Pipeline with no Key → Build fails → NewHandler returns the error.
	p := pipeline.NewPipeline[page, int64]("missing_key").
		Value(func(page) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFakeStore())
	if _, err := kinesis.NewHandler(p, kinesis.JSONDecoder[page]()); err == nil {
		t.Fatal("expected build error from incomplete pipeline")
	}
}
