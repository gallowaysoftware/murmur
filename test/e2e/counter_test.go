// End-to-end smoke test for a counter pipeline:
//
//	Kafka topic → Sum monoid → DynamoDB Int64SumStore
//
// Skipped unless both KAFKA_BROKERS and DDB_LOCAL_ENDPOINT are set. Run via:
//
//	docker compose up -d kafka dynamodb-local
//	KAFKA_BROKERS=localhost:9092 DDB_LOCAL_ENDPOINT=http://localhost:8000 \
//	  go test ./test/e2e/... -v -run Counter
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

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type pageView struct {
	PageID string `json:"page_id"`
	UserID string `json:"user_id"`
}

func TestE2E_CounterPipeline(t *testing.T) {
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	brokers := os.Getenv("KAFKA_BROKERS")
	if ddbEndpoint == "" || brokers == "" {
		t.Skip("DDB_LOCAL_ENDPOINT and KAFKA_BROKERS must both be set; run docker-compose first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- DynamoDB setup ---
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	ddbClient := awsddb.NewFromConfig(cfg, func(o *awsddb.Options) {
		o.BaseEndpoint = aws.String(ddbEndpoint)
	})
	table := fmt.Sprintf("murmur_e2e_counter_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	// --- Kafka topic + producer ---
	topic := fmt.Sprintf("murmur-e2e-counter-%d", time.Now().UnixNano())
	consumerGroup := topic + "-cg"
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// Produce a known distribution: page-A gets 5 events, page-B gets 3, page-C gets 1.
	want := map[string]int64{"page-A": 5, "page-B": 3, "page-C": 1}
	var produced int
	for page, n := range want {
		for i := int64(0); i < n; i++ {
			body, _ := json.Marshal(pageView{PageID: page, UserID: fmt.Sprintf("u-%d", i)})
			producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: body})
			produced++
		}
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("producer flush: %v", err)
	}
	t.Logf("produced %d records to %s", produced, topic)

	// --- Pipeline ---
	src, err := mkafka.NewSource(mkafka.Config[pageView]{
		Brokers:       strings.Split(brokers, ","),
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		Decode:        mkafka.JSONDecoder[pageView](),
	})
	if err != nil {
		t.Fatalf("kafka source: %v", err)
	}
	defer src.Close()

	pipe := pipeline.NewPipeline[pageView, int64]("e2e_counter").
		From(src).
		Key(func(e pageView) string { return e.PageID }).
		Value(func(pageView) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	// --- Run streaming runtime in background ---
	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- streaming.Run(runCtx, pipe)
	}()

	// --- Poll DDB until expected counts land or timeout ---
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for page, expected := range want {
			v, found, err := store.Get(ctx, state.Key{Entity: page})
			if err != nil {
				t.Fatalf("store Get %s: %v", page, err)
			}
			if !found || v != expected {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// --- Verify ---
	for page, expected := range want {
		v, found, err := store.Get(ctx, state.Key{Entity: page})
		if err != nil {
			t.Fatalf("store Get %s: %v", page, err)
		}
		if !found {
			t.Errorf("entity %q: missing", page)
			continue
		}
		if v != expected {
			t.Errorf("entity %q: got %d, want %d", page, v, expected)
		}
	}

	// --- Clean shutdown ---
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
