// End-to-end smoke test for pkg/source/kafka.Config.Concurrency:
//
//	multi-partition Kafka topic →
//	  Source(Concurrency=4) [partition mod 4 -> worker] →
//	  Sum monoid →
//	  DynamoDB Int64SumStore
//
// Verifies that the per-partition-fanout decode path (introduced in the
// v1-prep work) doesn't lose, duplicate, or misroute records. Skipped
// unless KAFKA_BROKERS and DDB_LOCAL_ENDPOINT are set.
//
// Run via:
//
//	docker compose up -d kafka dynamodb-local
//	KAFKA_BROKERS=localhost:9092 DDB_LOCAL_ENDPOINT=http://localhost:8000 \
//	  go test ./test/e2e/... -v -run KafkaConcurrency
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type clickEvent struct {
	PageID string `json:"page_id"`
	UserID string `json:"user_id"`
	Seq    int64  `json:"seq"`
}

func TestE2E_KafkaConcurrency_FanoutPreservesTotals(t *testing.T) {
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	brokers := os.Getenv("KAFKA_BROKERS")
	if ddbEndpoint == "" || brokers == "" {
		t.Skip("DDB_LOCAL_ENDPOINT and KAFKA_BROKERS must both be set; run docker-compose first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

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
	table := fmt.Sprintf("murmur_e2e_kafkaconc_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// --- Kafka topic with 12 explicit partitions so the per-partition
	// concurrency fanout has something to do. The default-auto-created
	// topic only has 1 partition, which would degenerate the test to the
	// serial path.
	topic := fmt.Sprintf("murmur-e2e-kafkaconc-%d", time.Now().UnixNano())
	const partitions = 12
	if err := createTopic(ctx, brokers, topic, partitions); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_ = deleteTopic(context.Background(), brokers, topic)
	})

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// --- Produce a known distribution across 24 keys, 30 events each
	// (720 total). With 12 partitions and 4 workers, each worker handles
	// 3 partitions; with 24 keys we get reasonable cross-partition
	// fanout (Kafka's default partitioner hashes keys uniformly).
	const keys = 24
	const perKey = 30
	want := map[string]int64{}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("page-%02d", k)
		want[key] = int64(perKey)
	}
	for key := range want {
		for seq := int64(0); seq < perKey; seq++ {
			body, _ := json.Marshal(clickEvent{
				PageID: key,
				UserID: fmt.Sprintf("u-%d", seq),
				Seq:    seq,
			})
			producer.ProduceSync(ctx, &kgo.Record{
				Topic: topic,
				Key:   []byte(key),
				Value: body,
			})
		}
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("producer flush: %v", err)
	}
	t.Logf("produced %d records (%d keys × %d each) to %s [%d partitions]",
		keys*perKey, keys, perKey, topic, partitions)

	// --- Source with Concurrency=4 — exercises the new fanout path.
	src, err := mkafka.NewSource(mkafka.Config[clickEvent]{
		Brokers:       strings.Split(brokers, ","),
		Topic:         topic,
		ConsumerGroup: topic + "-cg",
		Decode:        mkafka.JSONDecoder[clickEvent](),
		Concurrency:   4,
	})
	if err != nil {
		t.Fatalf("kafka source: %v", err)
	}
	defer src.Close()

	pipe := pipeline.NewPipeline[clickEvent, int64]("e2e_kafkaconc").
		From(src).
		Key(func(e clickEvent) string { return e.PageID }).
		Value(func(clickEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- streaming.Run(runCtx, pipe)
	}()

	// --- Poll DDB until expected counts land or timeout.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		complete := true
		for key, expected := range want {
			v, found, err := store.Get(ctx, state.Key{Entity: key})
			if err != nil {
				t.Fatalf("store Get %s: %v", key, err)
			}
			if !found || v != expected {
				complete = false
				break
			}
		}
		if complete {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// --- Verify every key reaches its expected total exactly.
	// A bug in the fanout would surface as either (a) missing records
	// (some key < perKey) or (b) double-counting (some key > perKey).
	var fails int
	for key, expected := range want {
		v, found, err := store.Get(ctx, state.Key{Entity: key})
		if err != nil {
			t.Fatalf("store Get %s: %v", key, err)
		}
		if !found {
			t.Errorf("entity %q: missing", key)
			fails++
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", key, v, expected)
			fails++
		}
	}
	if fails == 0 {
		t.Logf("all %d keys at expected counts after fanout consumption", keys)
	}

	runCancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Logf("streaming.Run returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Errorf("streaming runtime did not shut down within 10s")
	}
}

// createTopic pre-creates the topic with N partitions. Default
// auto-creation gives 1 partition, which would degenerate the
// concurrency test to the serial path.
func createTopic(ctx context.Context, brokers, topic string, partitions int32) error {
	admClient, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer admClient.Close()
	adm := kadm.NewClient(admClient)
	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
	if err != nil {
		return fmt.Errorf("CreateTopic: %w", err)
	}
	if resp.Err != nil && !strings.Contains(resp.Err.Error(), "TopicAlreadyExists") {
		return fmt.Errorf("CreateTopic response: %w", resp.Err)
	}
	return nil
}

func deleteTopic(ctx context.Context, brokers, topic string) error {
	admClient, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	defer admClient.Close()
	adm := kadm.NewClient(admClient)
	_, err = adm.DeleteTopics(ctx, topic)
	return err
}
