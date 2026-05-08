// End-to-end smoke test for the DDB ParallelScan bootstrap source:
//
//	DDB OLTP table (users) → ddbsnap.Source.Scan → bootstrap.Run → Sum monoid → DDB Int64SumStore
//
// Verifies:
//   - The new pkg/source/snapshot/dynamodb source scans a DDB-local table end-to-end
//   - ParallelScan with multiple segments produces ALL records once (segment fanout works)
//   - bootstrap.Run drives them through the pipeline's monoid Combine
//   - Per-region totals land in the destination DDB table with the right sums
//
// Skipped unless DDB_LOCAL_ENDPOINT is set. The dynamodb-local container is expected
// to be running on the configured endpoint.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	ddbsnap "github.com/gallowaysoftware/murmur/pkg/source/snapshot/dynamodb"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

// userOrder is the OLTP shape we'll bootstrap from.
type userOrder struct {
	ID     string
	Region string
	Amount int64
}

func TestE2E_DDBBootstrap_ParallelScan(t *testing.T) {
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set; run docker-compose up dynamodb-local first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Build a DDB client against dynamodb-local ---
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// --- Create source table (the OLTP table we're bootstrapping from) ---
	sourceTable := fmt.Sprintf("murmur_e2e_orders_%d", time.Now().UnixNano())
	if _, err := ddbClient.CreateTable(ctx, &awsddb.CreateTableInput{
		TableName: aws.String(sourceTable),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &sourceTable})
	})

	// --- Seed source table: 50 orders across 3 regions ---
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	wantTotals := map[string]int64{}
	for i := 0; i < 50; i++ {
		region := regions[i%len(regions)]
		amount := int64(10 + i)
		o := userOrder{
			ID:     fmt.Sprintf("order-%03d", i),
			Region: region,
			Amount: amount,
		}
		wantTotals[region] += amount
		if _, err := ddbClient.PutItem(ctx, &awsddb.PutItemInput{
			TableName: &sourceTable,
			Item: map[string]types.AttributeValue{
				"id":     &types.AttributeValueMemberS{Value: o.ID},
				"region": &types.AttributeValueMemberS{Value: o.Region},
				"amount": &types.AttributeValueMemberN{Value: strconv.FormatInt(o.Amount, 10)},
			},
		}); err != nil {
			t.Fatalf("put item %d: %v", i, err)
		}
	}

	// --- Create destination state table for the bootstrap output ---
	stateTable := fmt.Sprintf("murmur_e2e_region_totals_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, stateTable); err != nil {
		t.Fatalf("create state table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &stateTable})
	})
	store := mddb.NewInt64SumStore(ddbClient, stateTable)

	// --- Wire the DDB ParallelScan bootstrap source ---
	src, err := ddbsnap.NewSource(ddbsnap.Config[userOrder]{
		Client:    ddbClient,
		TableName: sourceTable,
		Segments:  4,
		Decode: func(item map[string]types.AttributeValue) (userOrder, error) {
			id, _ := item["id"].(*types.AttributeValueMemberS)
			region, _ := item["region"].(*types.AttributeValueMemberS)
			amountAttr, _ := item["amount"].(*types.AttributeValueMemberN)
			amount, _ := strconv.ParseInt(amountAttr.Value, 10, 64)
			return userOrder{
				ID:     id.Value,
				Region: region.Value,
				Amount: amount,
			}, nil
		},
		// Use the natural primary key so re-runs fold idempotently.
		EventID: func(o userOrder, _ map[string]types.AttributeValue) string {
			return o.ID
		},
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	defer src.Close()

	// --- Build the pipeline: per-region order totals ---
	pipe := pipeline.NewPipeline[userOrder, int64]("region_totals").
		Key(func(o userOrder) string { return o.Region }).
		Value(func(o userOrder) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	// --- Run the bootstrap ---
	token, err := bootstrap.Run(ctx, pipe, src)
	if err != nil {
		t.Fatalf("bootstrap.Run: %v", err)
	}
	// Token is nil when StreamsClient unset; that's fine for this test.
	_ = token

	// --- Verify per-region totals ---
	for region, want := range wantTotals {
		v, found, err := store.Get(ctx, state.Key{Entity: region})
		if err != nil {
			t.Errorf("Get %s: %v", region, err)
			continue
		}
		if !found {
			t.Errorf("region %q: not found in state store", region)
			continue
		}
		if v != want {
			t.Errorf("region %q: got %d, want %d", region, v, want)
		}
	}
	t.Logf("bootstrap completed: 50 orders across %d regions, all per-region sums match", len(wantTotals))
}

// TestE2E_DDBBootstrap_RerunWithDedupIsIdempotent proves that running
// the bootstrap twice with a Deduper in place produces the same final
// state — re-runs are a real operational concern (operator retry,
// partial-failure recovery).
func TestE2E_DDBBootstrap_RerunWithDedupIsIdempotent(t *testing.T) {
	endpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if endpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	// Tables.
	sourceTable := fmt.Sprintf("murmur_e2e_dedup_src_%d", time.Now().UnixNano())
	stateTable := fmt.Sprintf("murmur_e2e_dedup_state_%d", time.Now().UnixNano())
	dedupTable := fmt.Sprintf("murmur_e2e_dedup_table_%d", time.Now().UnixNano())

	if _, err := ddbClient.CreateTable(ctx, &awsddb.CreateTableInput{
		TableName: aws.String(sourceTable),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("create source: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &sourceTable})
	})
	if err := mddb.CreateInt64Table(ctx, ddbClient, stateTable); err != nil {
		t.Fatalf("create state: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &stateTable})
	})
	if err := mddb.CreateDedupTable(ctx, ddbClient, dedupTable); err != nil {
		t.Fatalf("create dedup: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &dedupTable})
	})

	store := mddb.NewInt64SumStore(ddbClient, stateTable)
	deduper := mddb.NewDeduper(ddbClient, dedupTable, time.Hour)

	// Seed 10 orders.
	for i := 0; i < 10; i++ {
		_, err := ddbClient.PutItem(ctx, &awsddb.PutItemInput{
			TableName: &sourceTable,
			Item: map[string]types.AttributeValue{
				"id":     &types.AttributeValueMemberS{Value: fmt.Sprintf("o-%d", i)},
				"region": &types.AttributeValueMemberS{Value: "us-east-1"},
				"amount": &types.AttributeValueMemberN{Value: "100"},
			},
		})
		if err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	src, err := ddbsnap.NewSource(ddbsnap.Config[userOrder]{
		Client:    ddbClient,
		TableName: sourceTable,
		Segments:  2,
		Decode: func(item map[string]types.AttributeValue) (userOrder, error) {
			id, _ := item["id"].(*types.AttributeValueMemberS)
			region, _ := item["region"].(*types.AttributeValueMemberS)
			amountAttr, _ := item["amount"].(*types.AttributeValueMemberN)
			amount, _ := strconv.ParseInt(amountAttr.Value, 10, 64)
			return userOrder{ID: id.Value, Region: region.Value, Amount: amount}, nil
		},
		EventID: func(o userOrder, _ map[string]types.AttributeValue) string { return o.ID },
	})
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	defer src.Close()

	pipe := pipeline.NewPipeline[userOrder, int64]("region_totals").
		Key(func(o userOrder) string { return o.Region }).
		Value(func(o userOrder) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	// First bootstrap.
	if _, err := bootstrap.Run(ctx, pipe, src, bootstrap.WithDedup(deduper)); err != nil {
		t.Fatalf("bootstrap 1: %v", err)
	}
	got1, _, _ := store.Get(ctx, state.Key{Entity: "us-east-1"})
	if got1 != 1000 {
		t.Fatalf("after bootstrap 1: got %d, want 1000 (10 orders × 100)", got1)
	}

	// Second bootstrap with the SAME source and SAME deduper. Without
	// dedup this would double-count to 2000; with dedup, every record's
	// EventID is already claimed and the second run is a no-op.
	src2, err := ddbsnap.NewSource(ddbsnap.Config[userOrder]{
		Client:    ddbClient,
		TableName: sourceTable,
		Segments:  2,
		Decode: func(item map[string]types.AttributeValue) (userOrder, error) {
			id, _ := item["id"].(*types.AttributeValueMemberS)
			region, _ := item["region"].(*types.AttributeValueMemberS)
			amountAttr, _ := item["amount"].(*types.AttributeValueMemberN)
			amount, _ := strconv.ParseInt(amountAttr.Value, 10, 64)
			return userOrder{ID: id.Value, Region: region.Value, Amount: amount}, nil
		},
		EventID: func(o userOrder, _ map[string]types.AttributeValue) string { return o.ID },
	})
	if err != nil {
		t.Fatalf("NewSource (2): %v", err)
	}
	defer src2.Close()

	if _, err := bootstrap.Run(ctx, pipe, src2, bootstrap.WithDedup(deduper)); err != nil {
		t.Fatalf("bootstrap 2: %v", err)
	}
	got2, _, _ := store.Get(ctx, state.Key{Entity: "us-east-1"})
	if got2 != 1000 {
		t.Errorf("after re-run with dedup: got %d, want 1000 (no double-count)", got2)
	}
	t.Logf("re-run idempotency confirmed: 10 records each marked seen, second pass no-op")
}
