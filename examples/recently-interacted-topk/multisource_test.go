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

	example "github.com/gallowaysoftware/murmur/examples/recently-interacted-topk"
	mkinesis "github.com/gallowaysoftware/murmur/pkg/exec/lambda/kinesis"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// This test drives the recently-interacted-topk example pipeline from BOTH
// the Lambda Kinesis handler and the streaming runtime simultaneously,
// against an in-memory state store. The point is to prove that the
// multi-source claim — one pipeline definition, two ingest paths, merged
// state — actually holds end-to-end.
//
// It calls the example's real Build(). An earlier version re-stated the
// pipeline inline "to keep it hermetic", and promptly drifted: the copy
// aggregated at K=10 while every deployed binary uses K=32, so the test
// asserted a Top-N shape nothing in production would produce. Build() is
// hermetic enough — it constructs a DDB client but issues no request — so
// the only thing this test substitutes is the Store.

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
	events []example.Interaction
}

func (s *kafkaLikeSource) Read(_ context.Context, out chan<- source.Record[example.Interaction]) error {
	for i, e := range s.events {
		out <- source.Record[example.Interaction]{
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

// exampleConfig is the Config every binary in the example builds from. The
// DDB endpoint is a loopback address that is never dialled — Build creates
// the client, but the test swaps the Store out before a single request is
// issued — and setting it keeps the AWS credential chain from reaching for
// real credentials.
func exampleConfig() example.Config {
	return example.Config{
		DDBTable:        "recently_interacted_test",
		DDBRegion:       "us-east-1",
		DDBEndpoint:     "http://127.0.0.1:8000",
		WindowRetention: 30 * 24 * time.Hour,
	}
}

// buildExamplePipeline returns the example's own pipeline with its DynamoDB
// store swapped for the test's in-memory one and, optionally, a source
// attached. Everything else — the "global" key, the SingleN value lift, the
// TopK monoid and its K, the daily windowing — comes from Build, so a change
// to the example is a change to what this test exercises.
//
// Pass store == nil to build the first pipeline and read back the monoid the
// caller needs in order to construct the shared store.
func buildExamplePipeline(
	t *testing.T,
	store state.Store[[]byte],
	src source.Source[example.Interaction],
) (*pipeline.Pipeline[example.Interaction, []byte], monoid.Monoid[[]byte]) {
	t.Helper()
	pipe, ddbStore, _, err := example.Build(context.Background(), exampleConfig())
	if err != nil {
		t.Fatalf("example.Build: %v", err)
	}
	// The real store is never written to; close it so the test leaks nothing.
	t.Cleanup(func() { _ = ddbStore.Close() })

	if got := pipe.Name(); got != example.PipelineName {
		t.Fatalf("pipeline name: got %q, want %q", got, example.PipelineName)
	}
	// Guard the K contradiction this test used to carry in a local const: the
	// sketch the pipeline actually builds must be the one every binary in the
	// example resolves to. Sketches sized for different K refuse to merge,
	// and the symptom is an empty Top-N rather than an error.
	wantK := exampleConfig().ResolveK()
	if got := sketchK(t, pipe.ValueFn()(example.Interaction{EntityID: "probe"})); got != wantK {
		t.Fatalf("example builds K=%d sketches, but ResolveK says %d", got, wantK)
	}
	if got := sketchK(t, pipe.Monoid().Identity()); got != wantK {
		t.Fatalf("example aggregates at K=%d, but ResolveK says %d", got, wantK)
	}
	if pipe.Window() == nil {
		t.Fatal("example.Build returned an unwindowed pipeline; the test asserts on a daily bucket")
	}

	mon := pipe.Monoid()
	if store != nil {
		pipe = pipe.StoreIn(store)
	}
	if src != nil {
		pipe = pipe.From(src)
	}
	return pipe, mon
}

func TestMultiSource_KinesisLambdaPlusKafkaWorker_ShareState(t *testing.T) {
	// The monoid comes from the example's own Build, so the shared store
	// combines exactly the way the deployed DDB BytesStore does.
	_, mon := buildExamplePipeline(t, nil, nil)
	store := newMemBytesStore(mon)

	// --- Drive Kafka side via streaming.Run ---
	kafkaEvents := []example.Interaction{
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-A", Source: "kafka"},
		{EntityID: "ent-B", Source: "kafka"},
		{EntityID: "ent-B", Source: "kafka"},
		{EntityID: "ent-C", Source: "kafka"},
	}
	src := &kafkaLikeSource{events: kafkaEvents}
	kafkaPipe, _ := buildExamplePipeline(t, store, src)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- streaming.Run(ctx, kafkaPipe) }()

	// --- Drive Kinesis side via NewKinesisHandler ---
	// Same pipeline definition, no Source attached.
	lambdaPipe, _ := buildExamplePipeline(t, store, nil)
	handler, err := mkinesis.NewHandler(lambdaPipe, mkinesis.JSONDecoder[example.Interaction]())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	kinesisEvents := []example.Interaction{
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
	// ent-C=1 (kafka), ent-D=1 (kinesis). The bucket comes from the example's
	// own windowing config, not a restatement of it.
	bucket := lambdaPipe.Window().BucketID(time.Now())
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

	// Misra-Gries retains every distinct key while the unique-count fits
	// under K (4 distinct entities against the example's K), so the counts
	// below are exact rather than approximate.
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
