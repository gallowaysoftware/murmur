package projector_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	projector "github.com/gallowaysoftware/murmur/examples/search-projector"
)

// fakeIndex implements projector.IndexClient with a recording shim. Each
// UpdateDoc call appends an entry; tests assert on what was emitted.
type fakeIndex struct {
	mu   sync.Mutex
	docs []indexedDoc
	err  error // when non-nil, every UpdateDoc returns this
}

type indexedDoc struct {
	index  string
	docID  string
	fields map[string]any
}

func (f *fakeIndex) UpdateDoc(_ context.Context, index, docID string, fields map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	f.docs = append(f.docs, indexedDoc{index: index, docID: docID, fields: cp})
	return nil
}

func (f *fakeIndex) snapshot() []indexedDoc {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]indexedDoc, len(f.docs))
	copy(cp, f.docs)
	return cp
}

// makeRecord constructs a DDB Streams record carrying a counter
// transition for entity `pk` going from `oldV` to `newV`.
func makeRecord(pk string, oldV, newV int64, hasOld bool) events.DynamoDBEventRecord {
	rec := events.DynamoDBEventRecord{
		EventID: "ev-" + pk + "-" + strconv.FormatInt(newV, 10),
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"pk": events.NewStringAttribute(pk),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"v": events.NewNumberAttribute(strconv.FormatInt(newV, 10)),
			},
		},
	}
	if hasOld {
		rec.Change.OldImage = map[string]events.DynamoDBAttributeValue{
			"v": events.NewNumberAttribute(strconv.FormatInt(oldV, 10)),
		}
	}
	return rec
}

func TestProjector_BucketTransitionTriggersIndex(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	// 999 (bucket 2) → 1000 (bucket 3): real transition.
	rec := makeRecord("post-A", 999, 1000, true)
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	docs := idx.snapshot()
	if len(docs) != 1 {
		t.Fatalf("expected 1 indexed doc, got %d", len(docs))
	}
	if docs[0].docID != "post-A" || docs[0].index != "posts" {
		t.Errorf("wrong doc identifiers: %+v", docs[0])
	}
	if docs[0].fields["popularity_bucket"] != 3 {
		t.Errorf("popularity_bucket: got %v, want 3", docs[0].fields["popularity_bucket"])
	}
	stats := p.Stats()
	if stats.Indexed.Load() != 1 || stats.Skipped.Load() != 0 {
		t.Errorf("stats: indexed=%d skipped=%d, want 1/0", stats.Indexed.Load(), stats.Skipped.Load())
	}
}

func TestProjector_NoTransitionSkipsIndex(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	// 100 → 500: both bucket 2 (100–999), no transition.
	rec := makeRecord("post-A", 100, 500, true)
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := len(idx.snapshot()); got != 0 {
		t.Errorf("expected 0 indexed docs (same bucket), got %d", got)
	}
	if p.Stats().Skipped.Load() != 1 {
		t.Errorf("Skipped: got %d, want 1", p.Stats().Skipped.Load())
	}
}

func TestProjector_LogarithmicReindexBudget(t *testing.T) {
	// A document going from 0 to 1M likes: ~1M write events but only
	// 6 bucket transitions (10, 100, 1k, 10k, 100k, 1M).
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	// Simulate a stream of writes 1, 2, 3, ..., 1_000_000.
	const target = int64(1_000_000)
	prev := int64(0)
	for next := int64(1); next <= target; next += step(next) {
		rec := makeRecord("hot-post", prev, next, prev > 0)
		if err := p.Handle(context.Background(), &rec); err != nil {
			t.Fatalf("Handle %d: %v", next, err)
		}
		prev = next
	}
	stats := p.Stats()
	if stats.Indexed.Load() < 5 || stats.Indexed.Load() > 8 {
		t.Errorf("Indexed = %d, want ~6 transitions (10/100/1k/10k/100k/1M)", stats.Indexed.Load())
	}
	t.Logf("0→1M with %d writes triggered %d reindexes (skip=%d)",
		stats.Decoded.Load(), stats.Indexed.Load(), stats.Skipped.Load())
}

// step returns a growth step that sweeps the value space without writing
// every integer. Coarse early, finer near boundaries.
func step(v int64) int64 {
	switch {
	case v < 10:
		return 1
	case v < 100:
		return 5
	case v < 1000:
		return 50
	case v < 10000:
		return 500
	case v < 100000:
		return 5000
	default:
		return 50000
	}
}

func TestProjector_FirstObservation(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	// First observation has no OldImage. The projector treats prev=-1
	// (hysteresis disabled), so the bucket projection is the naive value.
	// Going 0 → 50 lands in bucket 1 — should index.
	rec := makeRecord("new-post", 0, 50, false)
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	docs := idx.snapshot()
	if len(docs) != 1 {
		t.Fatalf("expected 1 indexed doc on first observation past bucket 0, got %d", len(docs))
	}
	if docs[0].fields["popularity_bucket"] != 1 {
		t.Errorf("popularity_bucket: got %v, want 1", docs[0].fields["popularity_bucket"])
	}
}

func TestProjector_TombstoneProjectsToZero(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	// REMOVE: NewImage absent, OldImage present at bucket 5.
	// Projector should treat new value as 0 → bucket 0, transition to 0.
	rec := events.DynamoDBEventRecord{
		EventID: "ev-tombstone",
		Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{
				"pk": events.NewStringAttribute("expiring-post"),
			},
			OldImage: map[string]events.DynamoDBAttributeValue{
				"v": events.NewNumberAttribute("250000"),
			},
		},
	}
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	docs := idx.snapshot()
	if len(docs) != 1 {
		t.Fatalf("expected 1 reindex on tombstone, got %d", len(docs))
	}
	if docs[0].fields["popularity_bucket"] != 0 {
		t.Errorf("popularity_bucket on tombstone: got %v, want 0", docs[0].fields["popularity_bucket"])
	}
}

func TestProjector_OpenSearchFailureReportsToBatchItemFailures(t *testing.T) {
	idx := &fakeIndex{err: errors.New("opensearch 503")}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	evt := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		makeRecord("post-A", 999, 1000, true), // would index → fails
		makeRecord("post-B", 100, 500, true),  // skipped (same bucket)
	}}
	failures := p.HandleEvent(context.Background(), evt)

	// Only the failing record should appear in BatchItemFailures.
	if len(failures) != 1 {
		t.Fatalf("BatchItemFailures: got %d, want 1", len(failures))
	}
	if failures[0].ItemIdentifier != "ev-post-A-1000" {
		t.Errorf("ItemIdentifier: got %q", failures[0].ItemIdentifier)
	}
}

func TestProjector_HysteresisSuppressesOscillation(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{
		Index:      "posts",
		Hysteresis: 0.20, // 20% band around boundaries
	}, idx)

	// Document oscillating 950↔1100 around the 1000 boundary at log10
	// bucket 2/3. Without hysteresis, every flip would reindex; with
	// hysteresis, all flips stay within band → no reindex.
	values := []int64{950, 1100, 950, 1100, 950, 1100}
	prev := int64(900) // start clearly in bucket 2
	for _, v := range values {
		rec := makeRecord("flapper", prev, v, true)
		if err := p.Handle(context.Background(), &rec); err != nil {
			t.Fatalf("Handle %d: %v", v, err)
		}
		prev = v
	}
	if got := p.Stats().Indexed.Load(); got != 0 {
		t.Errorf("hysteresis-protected oscillation: got %d reindexes, want 0", got)
	}
}

func TestProjector_MissingPKDecodesError(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{Index: "posts"}, idx)

	rec := events.DynamoDBEventRecord{
		EventID: "ev-bad",
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"v": events.NewNumberAttribute("100"),
			},
			// no Keys → no pk
		},
	}
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Errorf("Handle should swallow decode errors, got %v", err)
	}
	if p.Stats().DecodeErrs.Load() != 1 {
		t.Errorf("DecodeErrs: got %d, want 1", p.Stats().DecodeErrs.Load())
	}
}

func TestProjector_CustomFieldName(t *testing.T) {
	idx := &fakeIndex{}
	p := projector.New(projector.Config{
		Index: "posts",
		Field: "post_likes_bucket",
	}, idx)
	rec := makeRecord("post-A", 999, 1000, true)
	if err := p.Handle(context.Background(), &rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	docs := idx.snapshot()
	if _, ok := docs[0].fields["post_likes_bucket"]; !ok {
		t.Errorf("expected custom field name; got %+v", docs[0].fields)
	}
}
