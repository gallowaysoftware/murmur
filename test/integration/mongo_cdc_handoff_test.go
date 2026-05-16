// Deployed-shape end-to-end for the mongo-cdc-orderstats example.
//
// Two tests, scoped by what the containerised setup proves:
//
//   - TestDeployed_MongoCDC_BootstrapOnly — Mongo replset + DDB-local
//     + the built bootstrap binary running as a container against
//     both. Verifies the bootstrap leg of the snapshot-then-stream
//     pattern in its real deployed shape. Runs by default.
//
//   - TestDeployed_MongoCDC_FullHandoff — adds Kafka and the live
//     worker, asserts merged totals == bootstrap + delta. Same kafka
//     sibling-network blocker as TestDeployed_PageViewCounters_RoundTrip
//     (worker joins kafka:9092 but doesn't observe records produced
//     from the test process via the host listener; hostname override
//     is confirmed correct via the kafka_diag investigation, so the
//     issue is deeper). Skipped by default; opt-in via
//     MURMUR_RUN_DEPLOYED_ROUNDTRIP=1.

//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type seedOrder struct {
	ID         string `bson:"_id" json:"_id"`
	CustomerID string `bson:"customer_id" json:"customer_id"`
	Amount     int64  `bson:"amount" json:"amount"`
}

func TestDeployed_MongoCDC_BootstrapOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	net := newTestNetwork(ctx, t)

	mongoInside, mongoHost := startMongoReplset(ctx, t, net, "mongo")
	ddbInside, ddbClient := startDynamoDBLocal(ctx, t, net, "ddb")
	createInt64Table(ctx, t, ddbClient, "order_totals")
	store := mddb.NewInt64SumStore(ddbClient, "order_totals")

	// --- Seed Mongo from the test process via the host-mapped URI ---
	mongoClient, err := mongo.Connect(options.Client().ApplyURI(mongoHost))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	t.Cleanup(func() { _ = mongoClient.Disconnect(context.Background()) })

	coll := mongoClient.Database("shop").Collection("orders")
	docs := []seedOrder{
		{ID: "o1", CustomerID: "cust-a", Amount: 100},
		{ID: "o2", CustomerID: "cust-a", Amount: 50},
		{ID: "o3", CustomerID: "cust-b", Amount: 25},
		{ID: "o4", CustomerID: "cust-b", Amount: 75},
	}
	docsAny := make([]any, len(docs))
	for i, d := range docs {
		docsAny[i] = d
	}
	if _, err := coll.InsertMany(ctx, docsAny); err != nil {
		t.Fatalf("seed mongo: %v", err)
	}
	want := map[string]int64{"cust-a": 150, "cust-b": 100}

	// --- Build the example image and run /murmur-bootstrap as a one-shot container ---
	dockerfile := dockerfileBuild(t, "examples/mongo-cdc-orderstats/Dockerfile")
	bootReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-bootstrap"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"MONGO_URI":             mongoInside,
			"MONGO_DB":              "shop",
			"MONGO_COLLECTION":      "orders",
			"DDB_LOCAL_ENDPOINT":    ddbInside,
			"DDB_TABLE":             "order_totals",
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
		},
		// Bootstrap is one-shot: scan completes, prints handoff token,
		// exits 0. Wait for the exit.
		WaitingFor: wait.ForExit().WithExitTimeout(120 * time.Second),
	}
	boot, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: bootReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("bootstrap start: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "bootstrap", boot)
		_ = boot.Terminate(context.Background())
	})

	// --- Verify per-customer totals landed in DDB ---
	for customer, expected := range want {
		v, ok, err := store.Get(ctx, state.Key{Entity: customer})
		if err != nil {
			t.Fatalf("store Get %s: %v", customer, err)
		}
		if !ok {
			t.Errorf("entity %q: missing", customer)
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", customer, v, expected)
		}
	}
	t.Logf("bootstrap container scanned %d Mongo docs into DDB shadow table; totals match", len(docs))
}

func TestDeployed_MongoCDC_FullHandoff(t *testing.T) {
	if os.Getenv("MURMUR_RUN_DEPLOYED_ROUNDTRIP") == "" {
		t.Skip("CDC handoff round-trip skipped pending kafka sibling-network debugging; see TODO in test source. Bootstrap-only variant of this test runs by default and passes.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	net := newTestNetwork(ctx, t)

	mongoInside, mongoHost := startMongoReplset(ctx, t, net, "mongo")
	_, kafkaHost := startKafka(ctx, t, net, "kafka")
	ddbInside, ddbClient := startDynamoDBLocal(ctx, t, net, "ddb")
	createInt64Table(ctx, t, ddbClient, "order_totals")
	createInt64Table(ctx, t, ddbClient, "order_totals_dedup")
	store := mddb.NewInt64SumStore(ddbClient, "order_totals")

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(mongoHost))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}
	t.Cleanup(func() { _ = mongoClient.Disconnect(context.Background()) })
	coll := mongoClient.Database("shop").Collection("orders")
	bootstrapDocs := []seedOrder{
		{ID: "o1", CustomerID: "cust-a", Amount: 100},
		{ID: "o2", CustomerID: "cust-a", Amount: 50},
		{ID: "o3", CustomerID: "cust-b", Amount: 25},
	}
	docsAny := make([]any, len(bootstrapDocs))
	for i, d := range bootstrapDocs {
		docsAny[i] = d
	}
	if _, err := coll.InsertMany(ctx, docsAny); err != nil {
		t.Fatalf("seed mongo: %v", err)
	}

	dockerfile := dockerfileBuild(t, "examples/mongo-cdc-orderstats/Dockerfile")
	bootReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-bootstrap"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"MONGO_URI":             mongoInside,
			"MONGO_DB":              "shop",
			"MONGO_COLLECTION":      "orders",
			"DDB_LOCAL_ENDPOINT":    ddbInside,
			"DDB_TABLE":             "order_totals",
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
		},
		WaitingFor: wait.ForExit().WithExitTimeout(120 * time.Second),
	}
	boot, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: bootReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "bootstrap", boot)
		_ = boot.Terminate(context.Background())
	})

	workerReq := testcontainers.ContainerRequest{
		FromDockerfile: dockerfile,
		Entrypoint:     []string{"/murmur-worker"},
		Networks:       []string{net.Name},
		Env: map[string]string{
			"KAFKA_BROKERS":         "kafka:9092",
			"KAFKA_TOPIC":           "orders.cdc",
			"CONSUMER_GROUP":        "order_totals_cdc",
			"DDB_LOCAL_ENDPOINT":    ddbInside,
			"DDB_TABLE":             "order_totals",
			"DDB_DEDUP_TABLE":       "order_totals_dedup",
			"AWS_REGION":            "us-east-1",
			"AWS_ACCESS_KEY_ID":     "test",
			"AWS_SECRET_ACCESS_KEY": "test",
		},
		WaitingFor: wait.ForLog("starting").WithStartupTimeout(60 * time.Second),
	}
	worker, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: workerReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, "worker", worker)
		_ = worker.Terminate(context.Background())
	})

	// Produce CDC deltas via the host bootstrap (the kafka issue's
	// blast radius is the worker's read path; the producer side works).
	// Sleep first to give the worker time to join the consumer group.
	time.Sleep(5 * time.Second)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaHost, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	deltas := []seedOrder{
		{ID: "o4", CustomerID: "cust-a", Amount: 200},
		{ID: "o5", CustomerID: "cust-b", Amount: 50},
	}
	for _, d := range deltas {
		body, _ := json.Marshal(d)
		producer.ProduceSync(ctx, &kgo.Record{
			Topic: "orders.cdc",
			Value: body,
			Key:   []byte(d.CustomerID),
		})
	}
	_ = producer.Flush(ctx)

	// Expected = bootstrap totals + CDC deltas.
	want := map[string]int64{"cust-a": 350, "cust-b": 75}

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for customer, expected := range want {
			v, found, err := store.Get(ctx, state.Key{Entity: customer})
			if err != nil {
				t.Fatalf("store Get: %v", err)
			}
			if !found || v != expected {
				ok = false
				break
			}
		}
		if ok {
			t.Logf("merged totals match: %v", want)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	got := map[string]int64{}
	for customer := range want {
		v, _, _ := store.Get(ctx, state.Key{Entity: customer})
		got[customer] = v
	}
	t.Errorf("after 90s, got %v, want %v", got, want)
}

