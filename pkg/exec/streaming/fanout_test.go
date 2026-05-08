package streaming_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// likeEv is the canonical multi-pipeline shape: one event has multiple
// per-attribute counter axes.
type likeEv struct {
	post   string
	user   string
	region string
}

// likeStreamSource emits a fixed batch then closes.
type likeStreamSource struct {
	events []likeEv
}

func (s *likeStreamSource) Read(_ context.Context, out chan<- source.Record[likeEv]) error {
	for i, e := range s.events {
		out <- source.Record[likeEv]{
			EventID: fakeEventID(i),
			Value:   e,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*likeStreamSource) Name() string { return "test-likes" }
func (*likeStreamSource) Close() error { return nil }

// fanoutFlakyStore is a shared int64 store; tests assert on the per-key
// values that result from running multiple pipelines concurrently.
type fanoutStore struct {
	mu sync.Mutex
	m  map[state.Key]int64
}

func newFanoutStore() *fanoutStore { return &fanoutStore{m: map[state.Key]int64{}} }

func (s *fanoutStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fanoutStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fanoutStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (*fanoutStore) Close() error { return nil }

// fanoutBytesStore is the byte-monoid analog used for the TopK pipeline.
type fanoutBytesStore struct {
	mu  sync.Mutex
	m   map[state.Key][]byte
	mon interface {
		Combine([]byte, []byte) []byte
		Identity() []byte
	}
}

func newFanoutBytesStore(mon interface {
	Combine([]byte, []byte) []byte
	Identity() []byte
},
) *fanoutBytesStore {
	return &fanoutBytesStore{m: map[state.Key][]byte{}, mon: mon}
}
func (s *fanoutBytesStore) Get(_ context.Context, k state.Key) ([]byte, bool, error) {
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
func (s *fanoutBytesStore) GetMany(context.Context, []state.Key) ([][]byte, []bool, error) {
	return nil, nil, nil
}
func (s *fanoutBytesStore) MergeUpdate(_ context.Context, k state.Key, d []byte, _ time.Duration) error {
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
func (*fanoutBytesStore) Close() error { return nil }

func TestRunFanout_ThreePipelinesShareOneSource(t *testing.T) {
	// 6 events: 3 posts × 2 users × 2 regions. Three pipelines:
	//   - per-post counter
	//   - per-user counter
	//   - per-region counter
	// All consume the same source; each writes to its own DDB row.
	events := []likeEv{
		{post: "p-A", user: "u-1", region: "us"},
		{post: "p-A", user: "u-1", region: "us"},
		{post: "p-A", user: "u-2", region: "eu"},
		{post: "p-B", user: "u-1", region: "us"},
		{post: "p-B", user: "u-2", region: "eu"},
		{post: "p-C", user: "u-2", region: "eu"},
	}
	src := &likeStreamSource{events: events}

	postStore := newFanoutStore()
	userStore := newFanoutStore()
	regionStore := newFanoutStore()

	postPipe := pipeline.NewPipeline[likeEv, int64]("posts").
		Key(func(e likeEv) string { return e.post }).
		Value(func(likeEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(postStore)
	userPipe := pipeline.NewPipeline[likeEv, int64]("users").
		Key(func(e likeEv) string { return e.user }).
		Value(func(likeEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(userStore)
	regionPipe := pipeline.NewPipeline[likeEv, int64]("regions").
		Key(func(e likeEv) string { return e.region }).
		Value(func(likeEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(regionStore)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.RunFanout[likeEv](ctx, src, []streaming.Bound[likeEv]{
		streaming.Bind[likeEv, int64](postPipe),
		streaming.Bind[likeEv, int64](userPipe),
		streaming.Bind[likeEv, int64](regionPipe),
	}); err != nil {
		t.Fatalf("RunFanout: %v", err)
	}

	// Each pipeline saw all 6 events, keyed differently.
	wantPost := map[string]int64{"p-A": 3, "p-B": 2, "p-C": 1}
	for k, v := range wantPost {
		if got := postStore.m[state.Key{Entity: k}]; got != v {
			t.Errorf("post %q: got %d, want %d", k, got, v)
		}
	}
	wantUser := map[string]int64{"u-1": 3, "u-2": 3}
	for k, v := range wantUser {
		if got := userStore.m[state.Key{Entity: k}]; got != v {
			t.Errorf("user %q: got %d, want %d", k, got, v)
		}
	}
	wantRegion := map[string]int64{"us": 3, "eu": 3}
	for k, v := range wantRegion {
		if got := regionStore.m[state.Key{Entity: k}]; got != v {
			t.Errorf("region %q: got %d, want %d", k, got, v)
		}
	}
}

func TestRunFanout_HeterogeneousAggregationTypes(t *testing.T) {
	// One pipeline does Sum (int64); another does TopK (bytes). The fanout
	// runtime hides V behind Bind[T, V] — both fit in []Bound[T].
	events := []likeEv{
		{post: "p-A"}, {post: "p-A"}, {post: "p-A"},
		{post: "p-B"}, {post: "p-B"}, {post: "p-C"},
	}
	src := &likeStreamSource{events: events}

	sumStore := newFanoutStore()
	topkMon := topk.New(10)
	topkStore := newFanoutBytesStore(topkMon)

	sumPipe := pipeline.NewPipeline[likeEv, int64]("post_sum").
		Key(func(e likeEv) string { return e.post }).
		Value(func(likeEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(sumStore)
	topkPipe := pipeline.NewPipeline[likeEv, []byte]("post_topk").
		Key(func(likeEv) string { return "global" }).
		Value(func(e likeEv) []byte { return topk.SingleN(10, e.post, 1) }).
		Aggregate(topkMon).
		StoreIn(topkStore)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := streaming.RunFanout[likeEv](ctx, src, []streaming.Bound[likeEv]{
		streaming.Bind[likeEv, int64](sumPipe),
		streaming.Bind[likeEv, []byte](topkPipe),
	})
	if err != nil {
		t.Fatalf("RunFanout: %v", err)
	}

	// Sum side: 3+2+1 = 6 events keyed by post.
	if got := sumStore.m[state.Key{Entity: "p-A"}]; got != 3 {
		t.Errorf("sum p-A: got %d, want 3", got)
	}
	// TopK side: 6 contributions to "global" key; merged sketch should
	// have p-A as top.
	raw, ok, _ := topkStore.Get(ctx, state.Key{Entity: "global"})
	if !ok {
		t.Fatal("topk global: missing")
	}
	items, err := topk.Items(raw)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	if len(items) == 0 || items[0].Key != "p-A" {
		t.Errorf("top item: got %+v, want p-A first", items)
	}
}

// stampedEv is the input type for the Ack-counting test.
type stampedEv struct{}

// countingAckSource emits N records; tracks how many times the
// underlying source.Ack fires.
type countingAckSource struct {
	n         int
	totalAcks atomic.Int64
}

func (s *countingAckSource) Read(_ context.Context, out chan<- source.Record[stampedEv]) error {
	for i := 0; i < s.n; i++ {
		out <- source.Record[stampedEv]{
			EventID: fakeEventID(i),
			Value:   stampedEv{},
			Ack:     func() error { s.totalAcks.Add(1); return nil },
		}
	}
	return nil
}
func (*countingAckSource) Name() string { return "ack-counter" }
func (*countingAckSource) Close() error { return nil }

func TestRunFanout_AckFiresAfterAllPipelines(t *testing.T) {
	// Wire a source that exposes a per-record Ack count. The fanout
	// runtime must call the underlying Ack only after BOTH pipelines have
	// processed the record, so each event's count should be exactly 1.
	src := &countingAckSource{n: 5}

	pipeA := pipeline.NewPipeline[stampedEv, int64]("a").
		Key(func(stampedEv) string { return "k" }).
		Value(func(stampedEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFanoutStore())
	pipeB := pipeline.NewPipeline[stampedEv, int64]("b").
		Key(func(stampedEv) string { return "k" }).
		Value(func(stampedEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFanoutStore())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := streaming.RunFanout[stampedEv](ctx, src, []streaming.Bound[stampedEv]{
		streaming.Bind[stampedEv, int64](pipeA),
		streaming.Bind[stampedEv, int64](pipeB),
	}); err != nil {
		t.Fatalf("RunFanout: %v", err)
	}
	got := src.totalAcks.Load()
	// 5 records × 1 underlying-Ack-each = 5 acks total. Without the tee
	// this would be 10 (one per pipeline-per-record).
	if got != 5 {
		t.Errorf("underlying Acks: got %d, want 5 (one per record after both pipelines processed)", got)
	}
}

func TestRunFanout_RejectsEmptySources(t *testing.T) {
	if err := streaming.RunFanout[likeEv](context.Background(), nil,
		[]streaming.Bound[likeEv]{}); err == nil {
		t.Error("expected error on nil source")
	}
}

func TestRunFanout_RejectsEmptyPipelines(t *testing.T) {
	src := &likeStreamSource{}
	if err := streaming.RunFanout[likeEv](context.Background(), src, nil); err == nil {
		t.Error("expected error on empty pipelines")
	}
}

func TestRunFanout_PipelineErrorSurfacedJoined(t *testing.T) {
	src := &likeStreamSource{events: []likeEv{{post: "p-A"}}}

	// One pipeline misses the Aggregate call — Build fails when bound.
	bad := pipeline.NewPipeline[likeEv, int64]("bad").
		Key(func(e likeEv) string { return e.post }).
		Value(func(likeEv) int64 { return 1 }).
		StoreIn(newFanoutStore())

	good := pipeline.NewPipeline[likeEv, int64]("good").
		Key(func(e likeEv) string { return e.post }).
		Value(func(likeEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(newFanoutStore())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := streaming.RunFanout[likeEv](ctx, src, []streaming.Bound[likeEv]{
		streaming.Bind[likeEv, int64](bad),
		streaming.Bind[likeEv, int64](good),
	})
	if err == nil {
		t.Fatal("expected joined error containing the bad pipeline's Build failure")
	}
	if !errors.Is(err, pipeline.ErrMissingMonoid) {
		t.Errorf("expected ErrMissingMonoid in chain; got %v", err)
	}
}
