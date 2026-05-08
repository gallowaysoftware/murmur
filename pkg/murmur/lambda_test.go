package murmur_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/exec/lambda/sqs"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/murmur"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

type lambdaEvent struct {
	K string `json:"k"`
}

type lambdaStore struct {
	mu sync.Mutex
	m  map[state.Key]int64
}

func newLambdaStore() *lambdaStore { return &lambdaStore{m: map[state.Key]int64{}} }
func (s *lambdaStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (*lambdaStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *lambdaStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (*lambdaStore) Close() error { return nil }

func newLambdaPipe(store state.Store[int64]) *pipeline.Pipeline[lambdaEvent, int64] {
	return pipeline.NewPipeline[lambdaEvent, int64]("lambda_test").
		Key(func(e lambdaEvent) string { return e.K }).
		Value(func(lambdaEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestKinesisHandler_BuildsAndProcesses(t *testing.T) {
	store := newLambdaStore()
	rec := metrics.NewInMemory()
	pipe := newLambdaPipe(store)
	handler, err := murmur.KinesisHandler(pipe, kinesis.JSONDecoder[lambdaEvent](),
		murmur.LambdaConfig{Recorder: rec})
	if err != nil {
		t.Fatalf("KinesisHandler: %v", err)
	}
	body, _ := json.Marshal(lambdaEvent{K: "a"})
	evt := events.KinesisEvent{Records: []events.KinesisEventRecord{
		{
			EventSourceArn: "arn",
			Kinesis: events.KinesisRecord{
				Data:           body,
				SequenceNumber: "seq-1",
				PartitionKey:   "a",
			},
		},
	}}
	resp, err := handler(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures: got %d, want 0", len(resp.BatchItemFailures))
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("a: got %d, want 1", got)
	}
	// Recorder wired via the standard option set — events fired.
	if rec.SnapshotOne("lambda_test").EventsProcessed != 1 {
		t.Errorf("events: got %d, want 1", rec.SnapshotOne("lambda_test").EventsProcessed)
	}
}

func TestDynamoDBStreamsHandler_BuildsAndProcesses(t *testing.T) {
	store := newLambdaStore()
	pipe := newLambdaPipe(store)
	handler, err := murmur.DynamoDBStreamsHandler(pipe,
		func(rec *events.DynamoDBEventRecord) (lambdaEvent, error) {
			pk := rec.Change.Keys["pk"]
			return lambdaEvent{K: pk.String()}, nil
		},
		murmur.LambdaConfig{},
	)
	if err != nil {
		t.Fatalf("DynamoDBStreamsHandler: %v", err)
	}
	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		{
			EventID:   "ev-1",
			EventName: "INSERT",
			Change: events.DynamoDBStreamRecord{
				Keys: map[string]events.DynamoDBAttributeValue{
					"pk": events.NewStringAttribute("a"),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{
					"v": events.NewNumberAttribute("1"),
				},
			},
		},
	}}
	resp, err := handler(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures: got %d, want 0", len(resp.BatchItemFailures))
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("a: got %d, want 1", got)
	}
}

func TestSQSHandler_BuildsAndProcesses(t *testing.T) {
	store := newLambdaStore()
	pipe := newLambdaPipe(store)
	handler, err := murmur.SQSHandler[lambdaEvent](pipe, sqs.JSONDecoder[lambdaEvent](),
		murmur.LambdaConfig{},
	)
	if err != nil {
		t.Fatalf("SQSHandler: %v", err)
	}
	body, _ := json.Marshal(lambdaEvent{K: "a"})
	evt := events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "m1", Body: string(body), EventSourceARN: "arn"},
	}}
	resp, err := handler(context.Background(), evt)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if len(resp.BatchItemFailures) != 0 {
		t.Errorf("BatchItemFailures: got %d, want 0", len(resp.BatchItemFailures))
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("a: got %d, want 1", got)
	}
}

func TestMustHandler_PanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from MustHandler on error input")
		}
	}()
	type fakeHandler = func()
	_ = murmur.MustHandler[fakeHandler](nil, errFakeInit)
}

var errFakeInit = func() error {
	return &fakeError{msg: "fake init failure"}
}()

type fakeError struct{ msg string }

func (e *fakeError) Error() string { return e.msg }
