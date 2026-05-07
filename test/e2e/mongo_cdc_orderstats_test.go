// End-to-end smoke test for the mongo-cdc-orderstats example: Bootstrap
// (Mongo collection scan) followed by Live (Kafka CDC) feeding the same per-
// customer Sum monoid into the same DDB table. Verifies the merged totals
// equal bootstrapTotals + liveDeltas across two customers.
//
// Skipped unless DDB_LOCAL_ENDPOINT, KAFKA_BROKERS, and a usable Mongo replset
// are present. The replset is auto-detected via Ping; the test t.Skips
// gracefully if it's not initiated.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	orderstats "github.com/gallowaysoftware/murmur/examples/mongo-cdc-orderstats"
	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestE2E_MongoCDCOrderstats(t *testing.T) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
	}
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	brokers := os.Getenv("KAFKA_BROKERS")
	if ddbEndpoint == "" || brokers == "" {
		t.Skip("DDB_LOCAL_ENDPOINT and KAFKA_BROKERS must both be set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Mongo: pre-flight + seed ---
	mc, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Skipf("Mongo unavailable (%v); skipping", err)
	}
	if err := mc.Ping(ctx, nil); err != nil {
		_ = mc.Disconnect(ctx)
		t.Skipf("Mongo ping failed (%v)", err)
	}
	defer mc.Disconnect(context.Background())

	dbName := fmt.Sprintf("orderstats_e2e_%d", time.Now().UnixNano())
	collName := "orders"
	t.Cleanup(func() { _ = mc.Database(dbName).Drop(context.Background()) })

	historical := []orderstats.Order{
		{ID: "o-1", CustomerID: "cust-a", Amount: 100},
		{ID: "o-2", CustomerID: "cust-a", Amount: 50},
		{ID: "o-3", CustomerID: "cust-b", Amount: 25},
	}
	coll := mc.Database(dbName).Collection(collName)
	for _, o := range historical {
		if _, err := coll.InsertOne(ctx, o); err != nil {
			t.Fatalf("seed mongo %s: %v", o.ID, err)
		}
	}

	// --- DDB: state + dedup tables ---
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
	stateTable := fmt.Sprintf("orderstats_state_%d", time.Now().UnixNano())
	dedupTable := fmt.Sprintf("orderstats_dedup_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, stateTable); err != nil {
		t.Fatalf("create state table: %v", err)
	}
	if err := mddb.CreateDedupTable(ctx, ddbClient, dedupTable); err != nil {
		t.Fatalf("create dedup table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &stateTable})
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &dedupTable})
	})

	cfg := orderstats.Config{
		MongoURI:        mongoURI,
		MongoDB:         dbName,
		MongoCollection: collName,
		KafkaBrokers:    brokers,
		KafkaTopic:      fmt.Sprintf("orderstats-cdc-%d", time.Now().UnixNano()),
		ConsumerGroup:   fmt.Sprintf("orderstats-e2e-%d", time.Now().UnixNano()),
		DDBEndpoint:     ddbEndpoint,
		DDBTable:        stateTable,
		DDBRegion:       "us-east-1",
		DDBDedupTable:   dedupTable,
	}

	// --- Bootstrap ---
	bootPipe, snapSrc, bootStore, err := orderstats.BuildBootstrap(ctx, cfg)
	if err != nil {
		t.Fatalf("BuildBootstrap: %v", err)
	}
	token, err := bootstrap.Run(ctx, bootPipe, snapSrc)
	_ = snapSrc.Close()
	_ = bootStore.Close()
	if err != nil {
		t.Fatalf("bootstrap.Run: %v", err)
	}
	if len(token) == 0 {
		t.Errorf("bootstrap should capture a non-empty handoff token")
	}
	t.Logf("bootstrap captured handoff token (%d bytes)", len(token))

	// --- Verify post-bootstrap totals ---
	verifyState := mddb.NewInt64SumStore(ddbClient, stateTable)
	if v, ok, _ := verifyState.Get(ctx, state.Key{Entity: "cust-a"}); !ok || v != 150 {
		t.Errorf("post-bootstrap cust-a: got (%d, %v), want (150, true)", v, ok)
	}
	if v, ok, _ := verifyState.Get(ctx, state.Key{Entity: "cust-b"}); !ok || v != 25 {
		t.Errorf("post-bootstrap cust-b: got (%d, %v), want (25, true)", v, ok)
	}

	// --- Live: produce CDC events to Kafka, run streaming worker ---
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	liveOrders := []orderstats.Order{
		{ID: "o-4", CustomerID: "cust-a", Amount: 75},  // cust-a → 225
		{ID: "o-5", CustomerID: "cust-b", Amount: 200}, // cust-b → 225
		{ID: "o-6", CustomerID: "cust-c", Amount: 999}, // cust-c → 999 (new customer post-bootstrap)
	}
	// Produce each live record twice — simulates an at-least-once redelivery
	// after a hypothetical worker crash. With WithDedup wired up the runtime
	// should fold each EventID exactly once.
	for round := 0; round < 2; round++ {
		for _, o := range liveOrders {
			body, _ := json.Marshal(o)
			producer.ProduceSync(ctx, &kgo.Record{Topic: cfg.KafkaTopic, Value: body})
		}
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("producer flush: %v", err)
	}

	livePipe, liveStore, deduper, err := orderstats.BuildLive(ctx, cfg)
	if err != nil {
		t.Fatalf("BuildLive: %v", err)
	}
	defer liveStore.Close()
	if deduper != nil {
		defer deduper.Close()
	}

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- streaming.Run(runCtx, livePipe, streaming.WithDedup(deduper))
	}()

	// Wait for convergence: cust-c should land at 999 once the worker sees
	// either of the two delivered "o-6" records.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, _ := verifyState.Get(ctx, state.Key{Entity: "cust-c"})
		if ok && v == 999 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	cases := map[string]int64{"cust-a": 225, "cust-b": 225, "cust-c": 999}
	for entity, want := range cases {
		v, ok, err := verifyState.Get(ctx, state.Key{Entity: entity})
		if err != nil || !ok {
			t.Errorf("merged Get %s: ok=%v err=%v", entity, ok, err)
			continue
		}
		if v != want {
			t.Errorf("entity %q: got %d, want %d (bootstrap + live, deduped)", entity, v, want)
		}
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("streaming runtime did not shut down in 5s")
	}
}
