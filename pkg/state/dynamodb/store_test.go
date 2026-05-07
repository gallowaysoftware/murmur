package dynamodb_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/gallowaysoftware/murmur/pkg/state"
	"github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// localClient returns a DDB client pointed at dynamodb-local. Tests are skipped if the
// DDB_LOCAL_ENDPOINT environment variable is unset, so unit-test runs without the
// docker-compose stack stay green.
func localClient(t *testing.T) (*awsddb.Client, bool) {
	t.Helper()
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set; skipping (run docker-compose up dynamodb-local and re-run)")
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	c := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return c, true
}

func TestInt64SumStore_AtomicAdd(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)
	key := state.Key{Entity: "page-42", Bucket: 0}

	// Initial Get → missing.
	if v, ok, err := store.Get(ctx, key); err != nil || ok || v != 0 {
		t.Fatalf("initial Get: got (%d,%v,%v), want (0,false,nil)", v, ok, err)
	}

	// MergeUpdate +5, +3, -2 → expect 6.
	for _, d := range []int64{5, 3, -2} {
		if err := store.MergeUpdate(ctx, key, d, 0); err != nil {
			t.Fatalf("MergeUpdate %d: %v", d, err)
		}
	}
	if v, ok, err := store.Get(ctx, key); err != nil || !ok || v != 6 {
		t.Fatalf("after sums Get: got (%d,%v,%v), want (6,true,nil)", v, ok, err)
	}
}

func TestInt64SumStore_Windowed(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_window_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)

	// Same entity, different buckets — verify they don't collide.
	for bucket, count := range map[int64]int64{100: 7, 101: 11, 102: 13} {
		key := state.Key{Entity: "user-1", Bucket: bucket}
		if err := store.MergeUpdate(ctx, key, count, 24*time.Hour); err != nil {
			t.Fatalf("MergeUpdate bucket=%d: %v", bucket, err)
		}
	}
	for bucket, want := range map[int64]int64{100: 7, 101: 11, 102: 13} {
		key := state.Key{Entity: "user-1", Bucket: bucket}
		v, ok, err := store.Get(ctx, key)
		if err != nil || !ok || v != want {
			t.Fatalf("Get bucket=%d: got (%d,%v,%v), want (%d,true,nil)", bucket, v, ok, err, want)
		}
	}
}

func TestInt64SumStore_GetMany(t *testing.T) {
	client, _ := localClient(t)
	ctx := context.Background()
	table := "murmur_int64sum_many_test_" + t.Name()

	if err := dynamodb.CreateInt64Table(ctx, client, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(ctx, &awsddb.DeleteTableInput{TableName: &table})
	})

	store := dynamodb.NewInt64SumStore(client, table)

	// Populate three keys; one stays missing.
	for entity, count := range map[string]int64{"a": 1, "b": 2, "c": 3} {
		key := state.Key{Entity: entity, Bucket: 0}
		if err := store.MergeUpdate(ctx, key, count, 0); err != nil {
			t.Fatalf("seed %s: %v", entity, err)
		}
	}
	keys := []state.Key{
		{Entity: "a", Bucket: 0},
		{Entity: "b", Bucket: 0},
		{Entity: "missing", Bucket: 0},
		{Entity: "c", Bucket: 0},
	}
	want := []int64{1, 2, 0, 3}
	wantOK := []bool{true, true, false, true}
	vals, oks, err := store.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	for i := range keys {
		if vals[i] != want[i] || oks[i] != wantOK[i] {
			t.Fatalf("GetMany[%d]: got (%d,%v), want (%d,%v)", i, vals[i], oks[i], want[i], wantOK[i])
		}
	}
}
