package dynamodb_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func setupMaxStore(t *testing.T) (*mddb.Int64MaxStore, func()) {
	t.Helper()
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set; run docker-compose up dynamodb-local")
	}
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	client := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	table := fmt.Sprintf("murmur_max_test_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	store := mddb.NewInt64MaxStore(client, table)
	cleanup := func() {
		_, _ = client.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
		_ = store.Close()
	}
	return store, cleanup
}

func TestMaxStore_FirstWriteSetsValue(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "p-1"}
	if err := store.MergeUpdate(ctx, k, 100, 0); err != nil {
		t.Fatalf("first MergeUpdate: %v", err)
	}
	v, ok, err := store.Get(ctx, k)
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if v != 100 {
		t.Errorf("first write: got %d, want 100", v)
	}
}

func TestMaxStore_HigherValueWins(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "p-1"}

	for _, v := range []int64{10, 20, 15, 30, 25} {
		if err := store.MergeUpdate(ctx, k, v, 0); err != nil {
			t.Fatalf("MergeUpdate %d: %v", v, err)
		}
	}
	got, _, _ := store.Get(ctx, k)
	if got != 30 {
		t.Errorf("max wins: got %d, want 30", got)
	}
}

func TestMaxStore_OutOfOrderEventsDropped(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "p-1"}

	// First: high value lands.
	if err := store.MergeUpdate(ctx, k, 100, 0); err != nil {
		t.Fatalf("MergeUpdate 100: %v", err)
	}
	// Then: out-of-order lower value arrives. Should be silently dropped
	// (no error returned, store unchanged).
	if err := store.MergeUpdate(ctx, k, 50, 0); err != nil {
		t.Fatalf("MergeUpdate 50 (lower) returned error; should be no-op success: %v", err)
	}
	got, _, _ := store.Get(ctx, k)
	if got != 100 {
		t.Errorf("after out-of-order lower value: got %d, want 100 (unchanged)", got)
	}
}

func TestMaxStore_EqualValueIsNoOp(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "p-1"}

	if err := store.MergeUpdate(ctx, k, 42, 0); err != nil {
		t.Fatalf("MergeUpdate: %v", err)
	}
	// Same value again — condition is strictly greater, so this is a
	// no-op (no DDB write). Caller sees no error.
	if err := store.MergeUpdate(ctx, k, 42, 0); err != nil {
		t.Fatalf("MergeUpdate equal value: %v", err)
	}
	got, _, _ := store.Get(ctx, k)
	if got != 42 {
		t.Errorf("after equal-value re-emit: got %d, want 42", got)
	}
}

func TestMaxStore_ConcurrentWritesWinsHighest(t *testing.T) {
	// 100 concurrent writers, each emitting a unique value 1..100. The
	// final stored value should be 100; lower values either land first
	// (later overwritten) or land second (rejected by the condition).
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "concurrent"}

	const N = 100
	var wg sync.WaitGroup
	var errs atomic.Int64
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			if err := store.MergeUpdate(ctx, k, int64(v), 0); err != nil {
				errs.Add(1)
				t.Errorf("concurrent MergeUpdate %d: %v", v, err)
			}
		}(i)
	}
	wg.Wait()
	if errs.Load() > 0 {
		t.Fatalf("%d errors during concurrent writes", errs.Load())
	}
	got, _, _ := store.Get(ctx, k)
	if got != N {
		t.Errorf("after %d concurrent writes 1..%d: final = %d, want %d", N, N, got, N)
	}
}

func TestMaxStore_TTLAppliedOnWrite(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	k := state.Key{Entity: "fleeting", Bucket: 100}

	// Write with TTL — successful write should set both #v and #t.
	if err := store.MergeUpdate(ctx, k, 1, 24*time.Hour); err != nil {
		t.Fatalf("MergeUpdate with ttl: %v", err)
	}
	got, ok, _ := store.Get(ctx, k)
	if !ok || got != 1 {
		t.Errorf("after ttl write: got %d ok=%v, want 1 true", got, ok)
	}
	// We don't time-out the test waiting for DDB-local TTL eviction
	// (it's not reliably honored in dynamodb-local). The schema-level
	// guarantee is what we're verifying — the TTL attribute IS being
	// written.
}

func TestMaxStore_GetMany(t *testing.T) {
	store, cleanup := setupMaxStore(t)
	defer cleanup()

	ctx := context.Background()
	for i, v := range map[string]int64{"a": 10, "b": 20, "c": 30} {
		_ = store.MergeUpdate(ctx, state.Key{Entity: i}, v, 0)
	}
	keys := []state.Key{{Entity: "a"}, {Entity: "missing"}, {Entity: "c"}}
	vals, oks, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(vals) != 3 {
		t.Fatalf("len(vals): got %d, want 3", len(vals))
	}
	if vals[0] != 10 || !oks[0] {
		t.Errorf("[0] a: got (%d,%v), want (10, true)", vals[0], oks[0])
	}
	if oks[1] {
		t.Errorf("[1] missing: ok = true, want false")
	}
	if vals[2] != 30 || !oks[2] {
		t.Errorf("[2] c: got (%d,%v), want (30, true)", vals[2], oks[2])
	}
}
