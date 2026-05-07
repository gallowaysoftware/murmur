// End-to-end smoke test for Mongo Bootstrap mode:
//
//	Mongo collection (orders) → Bootstrap runtime → Sum monoid → DynamoDB Int64SumStore
//
// Verifies:
//   - SnapshotSource.CaptureHandoff returns a non-nil resume token
//   - Scan emits one record per document
//   - Bootstrap runtime drives them through the pipeline's monoid Combine
//   - Per-customer totals land in DDB with the right sums
//
// Skipped unless MONGO_URI and DDB_LOCAL_ENDPOINT are set. The Mongo deployment must
// be a replica set (Change Streams require it). docker-compose.yml's mongo service is
// configured for this; rs.initiate must be called once on first start.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mmongo "github.com/gallowaysoftware/murmur/pkg/source/snapshot/mongo"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type order struct {
	ID         string `bson:"_id"`
	CustomerID string `bson:"customer_id"`
	Amount     int64  `bson:"amount"`
}

func TestE2E_MongoBootstrap(t *testing.T) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
	}
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	if ddbEndpoint == "" {
		t.Skip("DDB_LOCAL_ENDPOINT must be set; run docker-compose up dynamodb-local first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Pre-flight: confirm Mongo is reachable as a replica set ---
	mc, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Skipf("Mongo unavailable (%v); skipping. Run docker compose up -d mongo and rs.initiate", err)
	}
	if err := mc.Ping(ctx, nil); err != nil {
		_ = mc.Disconnect(ctx)
		t.Skipf("Mongo ping failed (%v); skipping", err)
	}
	defer mc.Disconnect(context.Background())

	// --- DDB setup ---
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(awsCfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ddbEndpoint)
	})
	table := fmt.Sprintf("murmur_e2e_bootstrap_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// --- Seed Mongo with a known distribution ---
	dbName := fmt.Sprintf("murmur_e2e_%d", time.Now().UnixNano())
	collName := "orders"
	coll := mc.Database(dbName).Collection(collName)
	t.Cleanup(func() {
		_ = mc.Database(dbName).Drop(context.Background())
	})

	// customer-A: 4 orders, 10 + 20 + 30 + 40 = 100
	// customer-B: 3 orders, 5 + 5 + 5      = 15
	// customer-C: 1 order,  77             = 77
	want := map[string]int64{"customer-A": 100, "customer-B": 15, "customer-C": 77}
	docs := []order{
		{ID: "o1", CustomerID: "customer-A", Amount: 10},
		{ID: "o2", CustomerID: "customer-A", Amount: 20},
		{ID: "o3", CustomerID: "customer-A", Amount: 30},
		{ID: "o4", CustomerID: "customer-A", Amount: 40},
		{ID: "o5", CustomerID: "customer-B", Amount: 5},
		{ID: "o6", CustomerID: "customer-B", Amount: 5},
		{ID: "o7", CustomerID: "customer-B", Amount: 5},
		{ID: "o8", CustomerID: "customer-C", Amount: 77},
	}
	for _, d := range docs {
		if _, err := coll.InsertOne(ctx, d); err != nil {
			t.Fatalf("insert %s: %v", d.ID, err)
		}
	}

	// --- Bootstrap source ---
	src, err := mmongo.NewSource(ctx, mmongo.Config[order]{
		URI:        mongoURI,
		Database:   dbName,
		Collection: collName,
		Decode:     mmongo.BSONDecoder[order](),
	})
	if err != nil {
		t.Fatalf("mongo source: %v", err)
	}
	defer src.Close()

	pipe := pipeline.NewPipeline[order, int64]("e2e_bootstrap_orders").
		Key(func(o order) string { return o.CustomerID }).
		Value(func(o order) int64 { return o.Amount }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	// --- Run bootstrap ---
	token, err := bootstrap.Run(ctx, pipe, src)
	if err != nil {
		t.Fatalf("bootstrap.Run: %v", err)
	}
	if len(token) == 0 {
		t.Errorf("bootstrap token empty; expected a Mongo Change Stream resume token")
	}

	// --- Verify per-customer totals in DDB ---
	for customer, expected := range want {
		v, ok, err := store.Get(ctx, state.Key{Entity: customer})
		if err != nil {
			t.Fatalf("Get %s: %v", customer, err)
		}
		if !ok {
			t.Errorf("entity %q: missing", customer)
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", customer, v, expected)
		}
	}

	// Verify the resume token is well-formed BSON (a Change Stream resume token is a
	// document with a single _data field).
	var doc bson.M
	if err := bson.Unmarshal(token, &doc); err != nil {
		t.Errorf("token is not valid BSON: %v", err)
	} else if _, has := doc["_data"]; !has {
		t.Errorf("token missing _data field; got keys %v", doc)
	}

	t.Logf("bootstrap captured handoff token (%d bytes), processed %d documents", len(token), len(docs))
}
