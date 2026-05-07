package dynamodbstreams_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/dynamodbstreams"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is an in-memory state.Store[int64].
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

// flakyStore returns errFlaky N times per key, then succeeds.
type flakyStore struct {
	mu           sync.Mutex
	m            map[state.Key]int64
	attempts     map[state.Key]int
	failuresEach int
}

var errFlaky = errors.New("flaky")

func newFlakyStore(n int) *flakyStore {
	return &flakyStore{m: map[state.Key]int64{}, attempts: map[state.Key]int{}, failuresEach: n}
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

// memDeduper.
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

// order is the test pipeline's input shape — projected by the Decoder from
// a DDB Streams change record.
type order struct {
	customerID string
	amount     int64
}

func newPipe(store state.Store[int64]) *pipeline.Pipeline[order, int64] {
	return pipeline.NewPipeline[order, int64]("orders").
		Key(func(o order) string { return o.customerID }).
		Value(func(o order) int64 { return o.amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

// decodeOrder reads NewImage's customer_id (S) + amount (N) into order.
// Mirrors the canonical "DDB-table-as-source-of-truth → counter pipeline"
// shape.
func decodeOrder(rec *events.DynamoDBEventRecord) (order, error) {
	if rec.EventName == "REMOVE" {
		return order{}, dynamodbstreams.ErrSkipRecord
	}
	cid, ok := rec.Change.NewImage["customer_id"]
	if !ok {
		return order{}, errors.New("missing customer_id")
	}
	amt, ok := rec.Change.NewImage["amount"]
	if !ok {
		return order{}, errors.New("missing amount")
	}
	// NumberAttribute panics if not a number; use IntegerValue helper.
	n, err := amt.Integer()
	if err != nil {
		return order{}, err
	}
	return order{customerID: cid.String(), amount: n}, nil
}

func mustChange(t *testing.T, eventID, eventName, customerID string, amount int64) events.DynamoDBEventRecord {
	t.Helper()
	rec := events.DynamoDBEventRecord{
		EventID:   eventID,
		EventName: eventName,
		Change:    events.DynamoDBStreamRecord{},
	}
	if eventName != "REMOVE" {
		rec.Change.NewImage = map[string]events.DynamoDBAttributeValue{
			"customer_id": events.NewStringAttribute(customerID),
			"amount":      events.NewNumberAttribute(itoa(amount)),
		}
	}
	return rec
}

// itoa is strconv.Itoa for int64 without pulling in strconv in test code.
func itoa(n int64) string {
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

func TestHandler_HappyPath(t *testing.T) {
	store := newFakeStore()
	rec := metrics.NewInMemory()
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithMetrics(rec),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		mustChange(t, "ev-1", "INSERT", "cust-A", 100),
		mustChange(t, "ev-2", "INSERT", "cust-A", 50),
		mustChange(t, "ev-3", "MODIFY", "cust-B", 200),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0; got %+v", got, resp.BatchItemFailures)
	}
	if store.m[state.Key{Entity: "cust-A"}] != 150 {
		t.Errorf("cust-A: got %d, want 150", store.m[state.Key{Entity: "cust-A"}])
	}
	if store.m[state.Key{Entity: "cust-B"}] != 200 {
		t.Errorf("cust-B: got %d, want 200", store.m[state.Key{Entity: "cust-B"}])
	}
	if rec.SnapshotOne("orders").EventsProcessed != 3 {
		t.Errorf("events: got %d, want 3", rec.SnapshotOne("orders").EventsProcessed)
	}
}

func TestHandler_SkipRecordSentinel(t *testing.T) {
	store := newFakeStore()
	rec := metrics.NewInMemory()
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithMetrics(rec),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	// Mix INSERT (counted) + REMOVE (skipped) — the REMOVE must NOT decrement
	// or otherwise affect state, and must NOT be reported as a failure.
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		mustChange(t, "ev-1", "INSERT", "cust-A", 100),
		mustChange(t, "ev-2", "REMOVE", "cust-A", 0),
		mustChange(t, "ev-3", "INSERT", "cust-A", 50),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0", got)
	}
	if got, want := store.m[state.Key{Entity: "cust-A"}], int64(150); got != want {
		t.Errorf("cust-A: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("orders:skip").EventsProcessed != 1 {
		t.Errorf("skip events: got %d, want 1",
			rec.SnapshotOne("orders:skip").EventsProcessed)
	}
}

func TestHandler_RetriesAndRecovers(t *testing.T) {
	store := newFlakyStore(2)
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithMaxAttempts(3),
		dynamodbstreams.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		mustChange(t, "ev-1", "INSERT", "cust-A", 1),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0", got)
	}
	if store.m[state.Key{Entity: "cust-A"}] != 1 {
		t.Errorf("cust-A: got %d, want 1", store.m[state.Key{Entity: "cust-A"}])
	}
}

func TestHandler_ReportsExhaustedRetries(t *testing.T) {
	store := newFlakyStore(10)
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithMaxAttempts(2),
		dynamodbstreams.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		mustChange(t, "ev-doom", "INSERT", "cust-A", 1),
	}}
	resp, err := h(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 1 {
		t.Fatalf("BatchItemFailures = %d, want 1; got %+v", got, resp.BatchItemFailures)
	}
	if resp.BatchItemFailures[0].ItemIdentifier != "ev-doom" {
		t.Errorf("ItemIdentifier: got %q, want %q",
			resp.BatchItemFailures[0].ItemIdentifier, "ev-doom")
	}
}

func TestHandler_DedupSkipsSecondInvocation(t *testing.T) {
	store := newFakeStore()
	dedup := newMemDeduper()
	rec := metrics.NewInMemory()
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithMetrics(rec),
		dynamodbstreams.WithDedup(dedup),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		mustChange(t, "ev-dup", "INSERT", "cust-A", 100),
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
	if got, want := store.m[state.Key{Entity: "cust-A"}], int64(100); got != want {
		t.Errorf("after 3 dedup-protected invocations: got %d, want %d", got, want)
	}
	if rec.SnapshotOne("orders:dedup_skip").EventsProcessed != 2 {
		t.Errorf("dedup_skip events: got %d, want 2",
			rec.SnapshotOne("orders:dedup_skip").EventsProcessed)
	}
}

func TestHandler_DecodeErrorCallback(t *testing.T) {
	store := newFakeStore()
	var seen []string
	h, err := dynamodbstreams.NewHandler(newPipe(store), decodeOrder,
		dynamodbstreams.WithDecodeErrorCallback(func(rec *events.DynamoDBEventRecord, _ error) {
			seen = append(seen, rec.EventID)
		}),
	)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		// missing customer_id → decoder returns an error (not ErrSkipRecord)
		{EventID: "bad", EventName: "INSERT", Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"amount": events.NewNumberAttribute("100"),
			},
		}},
		mustChange(t, "good", "INSERT", "cust-A", 50),
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
	if store.m[state.Key{Entity: "cust-A"}] != 50 {
		t.Errorf("good record dropped after decode failure: got %d, want 50",
			store.m[state.Key{Entity: "cust-A"}])
	}
}

func TestHandler_NilPipeline(t *testing.T) {
	if _, err := dynamodbstreams.NewHandler[order, int64](nil, decodeOrder); err == nil {
		t.Fatal("NewHandler(nil): expected error")
	}
}

func TestHandler_NilDecode(t *testing.T) {
	if _, err := dynamodbstreams.NewHandler[order, int64](newPipe(newFakeStore()), nil); err == nil {
		t.Fatal("NewHandler(decode=nil): expected error")
	}
}

func TestHandler_PipelineNotBuilt(t *testing.T) {
	p := pipeline.NewPipeline[order, int64]("missing_key").
		Value(func(order) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFakeStore())
	if _, err := dynamodbstreams.NewHandler(p, decodeOrder); err == nil {
		t.Fatal("expected build error from incomplete pipeline")
	}
}
