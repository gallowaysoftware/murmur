package sqs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/sqs"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

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
func (s *fakeStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (s *fakeStore) Close() error { return nil }

type flakyStore struct {
	mu       sync.Mutex
	m        map[state.Key]int64
	attempts int
	failures int
}

var errFlaky = errors.New("flaky")

func newFlakyStore(n int) *flakyStore {
	return &flakyStore{m: map[state.Key]int64{}, failures: n}
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
	s.attempts++
	if s.attempts <= s.failures {
		return errFlaky
	}
	s.m[k] += d
	return nil
}
func (*flakyStore) Close() error { return nil }

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

type click struct {
	UserID string `json:"user_id"`
}

func newPipe(store state.Store[int64]) *pipeline.Pipeline[click, int64] {
	return pipeline.NewPipeline[click, int64]("clicks").
		Key(func(c click) string { return c.UserID }).
		Value(func(click) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func mustMessage(t *testing.T, c click, msgID, arn string) events.SQSMessage {
	t.Helper()
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return events.SQSMessage{
		MessageId:      msgID,
		Body:           string(body),
		EventSourceARN: arn,
	}
}

func TestHandler_HappyPath(t *testing.T) {
	store := newFakeStore()
	rec := metrics.NewInMemory()
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithMetrics[click](rec),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.SQSEvent{Records: []events.SQSMessage{
		mustMessage(t, click{UserID: "alice"}, "msg-1", "arn:queue"),
		mustMessage(t, click{UserID: "alice"}, "msg-2", "arn:queue"),
		mustMessage(t, click{UserID: "bob"}, "msg-3", "arn:queue"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0", got)
	}
	if got, want := store.m[state.Key{Entity: "alice"}], int64(2); got != want {
		t.Errorf("alice: got %d, want %d", got, want)
	}
	if got, want := store.m[state.Key{Entity: "bob"}], int64(1); got != want {
		t.Errorf("bob: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("clicks").EventsProcessed != 3 {
		t.Errorf("processed events: got %d, want 3", rec.SnapshotOne("clicks").EventsProcessed)
	}
}

func TestHandler_RetriesAndRecovers(t *testing.T) {
	store := newFlakyStore(2)
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithMaxAttempts[click](3),
		sqs.WithRetryBackoff[click](time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.SQSEvent{Records: []events.SQSMessage{
		mustMessage(t, click{UserID: "u-1"}, "msg-1", "arn"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures: got %d, want 0", got)
	}
	if store.m[state.Key{Entity: "u-1"}] != 1 {
		t.Errorf("u-1: got %d, want 1", store.m[state.Key{Entity: "u-1"}])
	}
}

func TestHandler_ReportsExhaustedRetries(t *testing.T) {
	store := newFlakyStore(99)
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithMaxAttempts[click](2),
		sqs.WithRetryBackoff[click](time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.SQSEvent{Records: []events.SQSMessage{
		mustMessage(t, click{UserID: "u-1"}, "doomed-msg", "arn"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 1 {
		t.Fatalf("BatchItemFailures: got %d, want 1", got)
	}
	if got := resp.BatchItemFailures[0].ItemIdentifier; got != "doomed-msg" {
		t.Errorf("ItemIdentifier: got %q, want %q", got, "doomed-msg")
	}
}

func TestHandler_DedupSkipsRedelivery(t *testing.T) {
	store := newFakeStore()
	dedup := newMemDeduper()
	rec := metrics.NewInMemory()
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithMetrics[click](rec),
		sqs.WithDedup[click](dedup),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.SQSEvent{Records: []events.SQSMessage{
		mustMessage(t, click{UserID: "u-1"}, "msg-dup", "arn"),
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
	if got, want := store.m[state.Key{Entity: "u-1"}], int64(1); got != want {
		t.Errorf("after 3 dedup-protected invocations: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("clicks:dedup_skip").EventsProcessed != 2 {
		t.Errorf("dedup_skip: got %d, want 2",
			rec.SnapshotOne("clicks:dedup_skip").EventsProcessed)
	}
}

func TestHandler_CustomEventIDExtractor(t *testing.T) {
	// Use the click's UserID as the dedup key — same user clicking twice
	// from different SQS messages folds idempotently.
	store := newFakeStore()
	dedup := newMemDeduper()
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithDedup[click](dedup),
		sqs.WithEventID[click](func(_ *events.SQSMessage, c click) string {
			return c.UserID
		}),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	evt := events.SQSEvent{Records: []events.SQSMessage{
		mustMessage(t, click{UserID: "alice"}, "msg-1", "arn"),
		mustMessage(t, click{UserID: "alice"}, "msg-2", "arn"), // same user → dedup
		mustMessage(t, click{UserID: "bob"}, "msg-3", "arn"),
	}}
	if _, err := h(context.Background(), evt); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got, want := store.m[state.Key{Entity: "alice"}], int64(1); got != want {
		t.Errorf("alice: got %d, want %d (custom EventID dedup'd 2nd msg)", got, want)
	}
	if got, want := store.m[state.Key{Entity: "bob"}], int64(1); got != want {
		t.Errorf("bob: got %d, want %d", got, want)
	}
}

func TestHandler_DecodeErrorCallback(t *testing.T) {
	store := newFakeStore()
	var seen []string
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithDecodeErrorCallback[click](func(_ string, msgID string, _ error) {
			seen = append(seen, msgID)
		}),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "bad", Body: "not json", EventSourceARN: "arn"},
		mustMessage(t, click{UserID: "u-1"}, "good", "arn"),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("decode error reported as BatchItemFailure (would loop forever); got %d", got)
	}
	if len(seen) != 1 || seen[0] != "bad" {
		t.Errorf("decode callback received %v, want [bad]", seen)
	}
	if store.m[state.Key{Entity: "u-1"}] != 1 {
		t.Errorf("good record dropped after decode failure: got %d, want 1",
			store.m[state.Key{Entity: "u-1"}])
	}
}

func TestHandler_NilPipeline(t *testing.T) {
	if _, err := sqs.NewHandler[click, int64](nil, sqs.JSONDecoder[click]()); err == nil {
		t.Fatal("NewHandler(nil): expected error")
	}
}

func TestHandler_NilDecode(t *testing.T) {
	if _, err := sqs.NewHandler[click, int64](newPipe(newFakeStore()), nil); err == nil {
		t.Fatal("NewHandler(decode=nil): expected error")
	}
}

func TestHandler_PipelineNotBuilt(t *testing.T) {
	p := pipeline.NewPipeline[click, int64]("missing_key").
		Value(func(click) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFakeStore())
	if _, err := sqs.NewHandler(p, sqs.JSONDecoder[click]()); err == nil {
		t.Fatal("expected build error from incomplete pipeline")
	}
}

func TestHandler_SentTimestampUsedForWindowing(t *testing.T) {
	// When the pipeline is windowed, the SQS SentTimestamp attribute drives
	// bucket assignment — a delayed delivery from yesterday must still land
	// in yesterday's bucket, not today's.
	store := newFakeStore()
	w := windowed.Daily(30 * 24 * time.Hour)
	pipe := pipeline.NewPipeline[click, int64]("clicks").
		Key(func(c click) string { return c.UserID }).
		Value(func(click) int64 { return 1 }).
		Aggregate(core.Sum[int64](), w).
		StoreIn(store)

	// Fix "now" so the bucket math is deterministic.
	fixedNow := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	yesterday := fixedNow.Add(-24 * time.Hour)

	h, err := sqs.NewHandler(pipe, sqs.JSONDecoder[click](),
		sqs.WithClock[click](func() time.Time { return fixedNow }),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	body, _ := json.Marshal(click{UserID: "u-1"})
	evt := events.SQSEvent{Records: []events.SQSMessage{
		{
			MessageId:      "msg-yesterday",
			Body:           string(body),
			EventSourceARN: "arn",
			Attributes: map[string]string{
				"SentTimestamp": strconv.FormatInt(yesterday.UnixMilli(), 10),
			},
		},
	}}
	if _, err := h(context.Background(), evt); err != nil {
		t.Fatalf("handler: %v", err)
	}
	yesterdayBucket := w.BucketID(yesterday)
	todayBucket := w.BucketID(fixedNow)
	if got := store.m[state.Key{Entity: "u-1", Bucket: yesterdayBucket}]; got != 1 {
		t.Errorf("yesterday bucket: got %d, want 1", got)
	}
	if got := store.m[state.Key{Entity: "u-1", Bucket: todayBucket}]; got != 0 {
		t.Errorf("today bucket: got %d, want 0 (message landed in yesterday's bucket)", got)
	}
}

func TestHandler_DefaultEventIDIncludesARN(t *testing.T) {
	// Two SQS messages with the same MessageId on different queues should
	// dedup independently — the EventID includes the ARN, so they're
	// distinct keys.
	store := newFakeStore()
	dedup := newMemDeduper()
	h, err := sqs.NewHandler(newPipe(store), sqs.JSONDecoder[click](),
		sqs.WithDedup[click](dedup),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	// Same MessageId on two queues:
	mid := "shared-id-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	evt := events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: mid, Body: `{"user_id":"alice"}`, EventSourceARN: "arn:queue1"},
		{MessageId: mid, Body: `{"user_id":"alice"}`, EventSourceARN: "arn:queue2"},
	}}
	if _, err := h(context.Background(), evt); err != nil {
		t.Fatalf("handler: %v", err)
	}
	// Both messages should have landed (different ARNs → different EventIDs).
	if got, want := store.m[state.Key{Entity: "alice"}], int64(2); got != want {
		t.Errorf("alice: got %d, want %d (ARN-scoped EventIDs should be distinct)", got, want)
	}
}
