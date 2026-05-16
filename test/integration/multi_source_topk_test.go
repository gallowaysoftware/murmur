// Deployed-shape end-to-end for the recently-interacted-topk example.
//
// One pipeline, two source containers — two long-running
// /murmur-worker instances consuming DIFFERENT Kafka topics but
// writing to the SAME DDB row. Demonstrates the multi-source
// aggregation pattern from the worked example: a single TopK sketch
// fed by independent input streams, with the DDB store's CAS-retry
// semantics keeping concurrent writers from clobbering each other.

//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type interaction struct {
	EntityID string `json:"entity_id"`
	UserID   string `json:"user_id"`
	Source   string `json:"source"`
}

func TestDeployed_RecentlyInteractedTopK_TwoSources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	net := newTestNetwork(ctx, t)

	_, kafkaHost := startKafka(ctx, t, net, "kafka")
	ddbInside, ddbClient := startDynamoDBLocal(ctx, t, net, "ddb")

	// recently-interacted-topk uses a BytesStore (Misra-Gries sketch
	// state), not the Int64SumStore the helpers' createInt64Table is
	// built for. Create the DDB table inline with the right schema.
	tableName := "recently_interacted"
	if err := mddb.CreateBytesTable(ctx, ddbClient, tableName); err != nil {
		t.Fatalf("create bytes table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &tableName})
	})
	store := mddb.NewBytesStore(ddbClient, tableName, topk.New(10))

	// --- Build the image, run two worker containers ---
	dockerfile := dockerfileBuild(t, "examples/recently-interacted-topk/Dockerfile")

	commonEnv := map[string]string{
		"DDB_ENDPOINT":          ddbInside,
		"DDB_TABLE":             tableName,
		"AWS_REGION":            "us-east-1",
		"AWS_ACCESS_KEY_ID":     "test",
		"AWS_SECRET_ACCESS_KEY": "test",
		"TOPK_K":                "10",
	}

	// Worker A: consumes "interactions.kafka" topic, simulating the
	// long-running Kafka ECS source.
	workerA := startTopKWorker(ctx, t, net, dockerfile, mergeMap(commonEnv, map[string]string{
		"KAFKA_BROKERS":  "kafka:9092",
		"KAFKA_TOPIC":    "interactions.kafka",
		"CONSUMER_GROUP": "topk_worker_kafka",
	}), "worker-kafka")
	_ = workerA

	// Worker B: consumes "interactions.kinesis" topic, simulating the
	// Kinesis Lambda source (we don't actually run Lambda; the same
	// worker binary reading a different Kafka topic is functionally
	// equivalent for testing the multi-writer DDB merge).
	workerB := startTopKWorker(ctx, t, net, dockerfile, mergeMap(commonEnv, map[string]string{
		"KAFKA_BROKERS":  "kafka:9092",
		"KAFKA_TOPIC":    "interactions.kinesis",
		"CONSUMER_GROUP": "topk_worker_kinesis",
	}), "worker-kinesis")
	_ = workerB

	// Sleep so both consumer groups join before producing.
	time.Sleep(5 * time.Second)

	// --- Produce interactions to both topics ---
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaHost, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// Topic kafka: items A (heavy), B (light)
	produceInteractions(t, ctx, producer, "interactions.kafka", []interaction{
		{UserID: "u1", EntityID: "A", Source: "kafka"},
		{UserID: "u2", EntityID: "A", Source: "kafka"},
		{UserID: "u3", EntityID: "A", Source: "kafka"},
		{UserID: "u4", EntityID: "A", Source: "kafka"},
		{UserID: "u5", EntityID: "B", Source: "kafka"},
	})
	// Topic kinesis: items A (more), C (only)
	produceInteractions(t, ctx, producer, "interactions.kinesis", []interaction{
		{UserID: "u6", EntityID: "A", Source: "kinesis"},
		{UserID: "u7", EntityID: "A", Source: "kinesis"},
		{UserID: "u8", EntityID: "C", Source: "kinesis"},
	})
	_ = producer.Flush(ctx)

	// Expected merged top: A=6 (4 kafka + 2 kinesis), B=1, C=1.
	// The pipeline is Daily-windowed (per the example's pipeline.go),
	// so the workers write to today's bucket — not bucket 0. Compute
	// today's bucket ID with the same windowed.Daily config the
	// workers use and read that key explicitly.
	w := windowed.Daily(30 * 24 * time.Hour)
	todayBucket := w.BucketID(time.Now())

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		raw, ok, err := store.Get(ctx, state.Key{Entity: "global", Bucket: todayBucket})
		if err != nil {
			t.Fatalf("store Get: %v", err)
		}
		if !ok {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		items, _ := topk.Items(raw)
		// Find A's count
		var aCount uint64
		for _, it := range items {
			if it.Key == "A" {
				aCount = it.Count
			}
		}
		if aCount >= 6 {
			t.Logf("merged TopK: %+v (item A reached expected merged count)", items)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Errorf("after 120s, merged TopK didn't reach the expected counts (item A < 6)")
}

func startTopKWorker(ctx context.Context, t *testing.T, net *testcontainers.DockerNetwork, df testcontainers.FromDockerfile, env map[string]string, name string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		FromDockerfile: df,
		Entrypoint:     []string{"/murmur-worker"},
		Networks:       []string{net.Name},
		Env:            env,
		WaitingFor:     wait.ForLog("starting").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("%s start: %v", name, err)
	}
	t.Cleanup(func() {
		dumpContainerLogs(t, name, c)
		_ = c.Terminate(context.Background())
	})
	return c
}

func produceInteractions(t *testing.T, ctx context.Context, p *kgo.Client, topic string, events []interaction) {
	t.Helper()
	for _, e := range events {
		body, _ := json.Marshal(e)
		p.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(e.EntityID),
			Value: body,
		})
	}
}

func mergeMap(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
