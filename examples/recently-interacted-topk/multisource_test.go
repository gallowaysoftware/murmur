package recentlyinteracted_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	mkinesis "github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// This test mirrors the recently-interacted-topk example pipeline against an
// in-memory state store driven by BOTH the Lambda Kinesis handler and the
// streaming runtime simultaneously. The point is to prove that the
// multi-source claim — one pipeline definition, two ingest paths, merged
// state — actually holds end-to-end.
//
// We import the example's package indirectly: rather than depend on the
// Build() function (which spins up a real DDB client), we re-state the same
// pipeline definition here with a synthetic Store. This keeps the test
// hermetic; the example's pipeline.go still owns the canonical definition.

type interaction struct {
	EntityID string `json:"entity_id"`
	UserID   string `json:"user_id"`
	Source   string `json:"source,omitempty"`
}

const k uint32 = 10

// memBytesStore is an in-memory state.Store[[]byte] backed by the supplied
// monoid's Combine. The streaming runtime and the Lambda handler both
// MergeUpdate into it, mimicking how two real drivers fold into the same
// DDB row in production.
type memBytesStore struct {
	mu  sync.Mutex
	mon monoid.Monoid[[]byte]
	m   map[state.Key][]byte
}

func newMemBytesStore(mon monoid.Monoid[[]byte]) *memBytesStore {
	return &memBytesStore{mon: mon, m: map[state.Key][]byte{}}
}

func (s *memBytesStore) Get(_ context.Context, k state.Key) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}
func (s *memBytesStore) GetMany(context.Context, []state.Key) ([][]byte, []bool, error) {
	return nil, nil, nil
}
func (s *memBytesStore) MergeUpdate(_ context.Context, k state.Key, d []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[k]
	if !ok {
		s.m[k] = append([]byte(nil), d...)
		return nil
	}
	s.m[k] = s.mon.Combine(cur, d)
	return nil
}
func (s *memBytesStore) Close() error { return nil }

// kafkaLikeSource satisfies source.Source — used to drive streaming.Run with
// a fixed batch of synthetic interactions, then close.
type kafkaLikeSource struct {
	events []interaction
}

func (s *kafkaLikeSource) Read(_ context.Context, out chan<- source.Record[interaction]) error {
	for i, e := range s.events {
		out <- source.Record[interaction]{
			EventID:   fmt.Sprintf("kafka-%d", i),
			EventTime: time.Now(),
			Value:     e,
			Ack:       func() error { return nil },
		}
	}
	return nil
}
func (*kafkaLikeSource) Name() string { return "test-kafka" }
func (*kafkaLikeSource) Close() error { return nil }

// buildPipeline mirrors examples/recently-interacted-topk.Build but with the
// store and source plugged in by the test. The aggregation shape (TopK
// monoid, daily windowing, "global" key, SingleN value lift) is identical to
// the example.
func buildPipeline(store state.Store[[]byte], src source.Source[interaction]) *pipeline.Pipeline[interaction, []byte] {
	w := windowed.Daily(30 * 24 * time.Hour)
	pipe := pipeline.NewPipeline[interaction, []byte]("recently_interacted").
		Key(func(interaction) string { return "global" }).
		Value(func(e interaction) []byte { return topk.SingleN(k, e.EntityID, 1) }).
		Aggregate(topk.New(k), w).
		StoreIn(store)
	if src != nil {
		pipe = pipe.From(src)
	}
	return pipe
}

func TestMultiSource_KinesisLambdaPlusKafkaWorker_ShareState(t *testing.T) {
	store := newMemBytesStore(topk.New(k))

	// --- Drive Kafka side via streaming.Run ---
	kafkaEvents := []interaction{
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-B", Source: "kafka"},
		{EntityID: "ent-B", Source: "kafka"},
		{EntityID: "ent-C", Source: "kafka"},
	}
	src := &kafkaLikeSource{events: kafkaEvents}
	kafkaPipe := buildPipeline(store, src)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- streaming.Run(ctx, kafkaPipe) }()

	// --- Drive Kinesis side via NewKinesisHandler ---
	// Same pipeline definition, no Source attached.
	lambdaPipe := buildPipeline(store, nil)
	handler, err := mkinesis.NewHandler(lambdaPipe, mkinesis.JSONDecoder[interaction]())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	kinesisEvents := []interaction{
		{EntityID: "ent-A", Source: "kinesis"},
		{EntityID: "ent-A", Source: "kinesis"},
		{EntityID: "ent-B", Source: "kinesis"},
		{EntityID: "ent-D", Source: "kinesis"},
	}
	evt := events.KinesisEvent{Records: make([]events.KinesisEventRecord, len(kinesisEvents))}
	for i, e := range kinesisEvents {
		body, _ := json.Marshal(e)
		evt.Records[i] = events.KinesisEventRecord{
			EventSourceArn: "arn:aws:kinesis:us-east-1:test:stream/interactions",
			Kinesis: events.KinesisRecord{
				Data:           body,
				SequenceNumber: fmt.Sprintf("seq-%d", i),
				PartitionKey:   e.EntityID,
			},
		}
	}
	resp, err := handler(ctx, evt)
	if err != nil {
		t.Fatalf("lambda handler: %v", err)
	}
	if got := len(resp.BatchItemFailures); got != 0 {
		t.Fatalf("BatchItemFailures = %d, want 0", got)
	}

	// Wait for the Kafka driver to drain.
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("streaming.Run did not return in time")
	}

	// --- Verify merged Top-N ---
	// Bucket today gets all 10 events: ent-A=5 (3 kafka + 2 kinesis), ent-B=3 (2k+1k),
	// ent-C=1 (kafka), ent-D=1 (kinesis).
	w := windowed.Daily(30 * 24 * time.Hour)
	bucket := w.BucketID(time.Now())
	raw, ok, err := store.Get(ctx, state.Key{Entity: "global", Bucket: bucket})
	if err != nil {
		t.Fatalf("store Get: %v", err)
	}
	if !ok {
		t.Fatal("store Get: missing aggregation key")
	}
	items, err := topk.Items(raw)
	if err != nil {
		t.Fatalf("topk.Items decode: %v", err)
	}

	// Misra-Gries with K=10 retains every distinct key when the unique-count
	// fits, so we can assert exact counts.
	got := map[string]uint64{}
	for _, it := range items {
		got[it.Key] = it.Count
	}
	want := map[string]uint64{
		"ent-A": 5, // 3 from Kafka + 2 from Kinesis
		"ent-B": 3, // 2 from Kafka + 1 from Kinesis
		"ent-C": 1, // Kafka only
		"ent-D": 1, // Kinesis only
	}
	for k, expected := range want {
		if got[k] != expected {
			t.Errorf("entity %q: merged count = %d, want %d (full got=%v)", k, got[k], expected, got)
		}
	}

	// And the overall ordering matches the merged distribution.
	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, it.Key)
	}
	sort.SliceStable(keys, func(i, j int) bool { return got[keys[i]] > got[keys[j]] })
	if got, want := keys[0], "ent-A"; got != want {
		t.Errorf("top entity: got %q, want %q", got, want)
	}
	if got, want := keys[1], "ent-B"; got != want {
		t.Errorf("second entity: got %q, want %q", got, want)
	}
}
