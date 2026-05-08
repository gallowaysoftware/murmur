package valkey_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/state"
	mvalkey "github.com/gallowaysoftware/murmur/pkg/state/valkey"
)

func newHLLCache(t *testing.T, prefix string) *mvalkey.HLLCache {
	t.Helper()
	c, err := mvalkey.NewHLLCache(mvalkey.HLLConfig{
		Address:   valkeyAddress(t),
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatalf("new HLL cache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestHLLCache_AddAndCardinality(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "page-1"}

	if err := c.Add(ctx, k, []byte("user-A"), 0); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := c.Add(ctx, k, []byte("user-B"), 0); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, ok, err := c.Cardinality(ctx, k)
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if !ok {
		t.Fatal("Cardinality ok=false; want true")
	}
	if got != 2 {
		t.Errorf("Cardinality: got %d, want 2", got)
	}
}

func TestHLLCache_RepeatedElementCountsOnce(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "page-rep"}

	// 100 inserts of the same element → cardinality = 1.
	for i := 0; i < 100; i++ {
		if err := c.Add(ctx, k, []byte("the-only-user"), 0); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	got, _, _ := c.Cardinality(ctx, k)
	if got != 1 {
		t.Errorf("Cardinality after 100 dup adds: got %d, want 1", got)
	}
}

func TestHLLCache_MissingKey(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()

	got, ok, err := c.Cardinality(ctx, state.Key{Entity: "nonexistent"})
	if err != nil {
		t.Fatalf("Cardinality: %v", err)
	}
	if ok {
		t.Errorf("Cardinality on missing key: ok=true, want false")
	}
	if got != 0 {
		t.Errorf("Cardinality on missing key: got %d, want 0", got)
	}
}

func TestHLLCache_AddManyBatch(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "page-batch"}

	const N = 200
	elems := make([][]byte, N)
	for i := 0; i < N; i++ {
		elems[i] = []byte(fmt.Sprintf("user-%d", i))
	}
	if err := c.AddMany(ctx, k, elems, 0); err != nil {
		t.Fatalf("AddMany: %v", err)
	}

	got, _, _ := c.Cardinality(ctx, k)
	// HLL has ~1.6% standard error; 200 elements should be well within bounds.
	if delta := math.Abs(float64(got) - float64(N)); delta > float64(N)*0.05 {
		t.Errorf("Cardinality after AddMany %d distinct: got %d (err %.2f%%), want within ±5%%",
			N, got, delta/float64(N)*100)
	}
}

func TestHLLCache_AccuracyOnLargeSet(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "page-big"}

	const N = 5000
	for i := 0; i < N; i++ {
		if err := c.Add(ctx, k, []byte(fmt.Sprintf("user-%d", i)), 0); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	got, _, _ := c.Cardinality(ctx, k)
	// Valkey HLL precision is configured for ~0.81% standard error
	// (P14, 16384 registers). Allow ±5% to absorb run-to-run variance.
	if delta := math.Abs(float64(got) - float64(N)); delta > float64(N)*0.05 {
		t.Errorf("Cardinality after %d distinct adds: got %d (err %.2f%%), want within ±5%%",
			N, got, delta/float64(N)*100)
	}
}

func TestHLLCache_MergeInto(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()

	// Three buckets, each with 100 distinct users; partial overlap so
	// the merged cardinality is between max-per-bucket (100) and
	// sum (300).
	bucketA := state.Key{Entity: "page-1", Bucket: 1}
	bucketB := state.Key{Entity: "page-1", Bucket: 2}
	bucketC := state.Key{Entity: "page-1", Bucket: 3}
	for i := 0; i < 100; i++ {
		_ = c.Add(ctx, bucketA, []byte(fmt.Sprintf("user-%d", i)), 0)
	}
	for i := 50; i < 150; i++ { // 50% overlap with A
		_ = c.Add(ctx, bucketB, []byte(fmt.Sprintf("user-%d", i)), 0)
	}
	for i := 100; i < 200; i++ { // 50% overlap with B
		_ = c.Add(ctx, bucketC, []byte(fmt.Sprintf("user-%d", i)), 0)
	}

	rollup := state.Key{Entity: "page-1", Bucket: 999}
	if err := c.MergeInto(ctx, rollup, []state.Key{bucketA, bucketB, bucketC}, 0); err != nil {
		t.Fatalf("MergeInto: %v", err)
	}

	got, ok, _ := c.Cardinality(ctx, rollup)
	if !ok {
		t.Fatal("rollup cardinality ok=false")
	}
	// True union cardinality is 200 (users 0-199). Allow ±5%.
	if delta := math.Abs(float64(got) - 200); delta > 200*0.05 {
		t.Errorf("rollup cardinality: got %d, want within ±5%% of 200", got)
	}
}

func TestHLLCache_CardinalityOver(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()

	// Same setup as MergeInto but query the union via PFCOUNT k1 k2 k3
	// without materializing a rollup.
	bucketA := state.Key{Entity: "page-1", Bucket: 1}
	bucketB := state.Key{Entity: "page-1", Bucket: 2}
	for i := 0; i < 100; i++ {
		_ = c.Add(ctx, bucketA, []byte(fmt.Sprintf("user-%d", i)), 0)
	}
	for i := 75; i < 175; i++ { // 25% overlap with A
		_ = c.Add(ctx, bucketB, []byte(fmt.Sprintf("user-%d", i)), 0)
	}

	got, err := c.CardinalityOver(ctx, []state.Key{bucketA, bucketB})
	if err != nil {
		t.Fatalf("CardinalityOver: %v", err)
	}
	// True union: 175 distinct (users 0-174). Allow ±5%.
	if delta := math.Abs(float64(got) - 175); delta > 175*0.05 {
		t.Errorf("CardinalityOver: got %d, want within ±5%% of 175", got)
	}
}

func TestHLLCache_CardinalityOverAllMissing(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()

	got, err := c.CardinalityOver(ctx, []state.Key{
		{Entity: "nope-1"},
		{Entity: "nope-2"},
	})
	if err != nil {
		t.Fatalf("CardinalityOver missing: %v", err)
	}
	if got != 0 {
		t.Errorf("CardinalityOver all missing: got %d, want 0", got)
	}
}

func TestHLLCache_TTLApplied(t *testing.T) {
	c := newHLLCache(t, fmt.Sprintf("hlltest_%d", time.Now().UnixNano()))
	ctx := context.Background()
	k := state.Key{Entity: "fleeting"}

	// Short-lived entry: 1-second TTL.
	if err := c.Add(ctx, k, []byte("blip"), 1*time.Second); err != nil {
		t.Fatalf("Add with TTL: %v", err)
	}
	// Immediately readable.
	got, ok, _ := c.Cardinality(ctx, k)
	if !ok || got != 1 {
		t.Errorf("immediate read: got %d ok=%v, want 1 true", got, ok)
	}
	// Wait past TTL and re-check. Valkey EXPIRE is honored unlike
	// dynamodb-local TTL (which is best-effort). 2s sleep gives a
	// safe margin past the 1s TTL.
	time.Sleep(2 * time.Second)
	_, ok, err := c.Cardinality(ctx, k)
	if err != nil {
		t.Fatalf("post-TTL Cardinality: %v", err)
	}
	if ok {
		t.Errorf("post-TTL: ok=true (key should be evicted)")
	}
}
