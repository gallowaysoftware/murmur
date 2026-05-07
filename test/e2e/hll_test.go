// End-to-end smoke test for an HLL unique-cardinality pipeline:
//
//	Kafka topic → HLL monoid → DynamoDB BytesStore (CAS path)
//
// Skipped unless both KAFKA_BROKERS and DDB_LOCAL_ENDPOINT are set.
package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestE2E_HLLUniqueVisitorsPipeline(t *testing.T) {
	ddbEndpoint := os.Getenv("DDB_LOCAL_ENDPOINT")
	brokers := os.Getenv("KAFKA_BROKERS")
	if ddbEndpoint == "" || brokers == "" {
		t.Skip("DDB_LOCAL_ENDPOINT and KAFKA_BROKERS must both be set; run docker-compose first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

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
	table := fmt.Sprintf("murmur_e2e_hll_%d", time.Now().UnixNano())
	if err := mddb.CreateBytesTable(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewBytesStore(ddbClient, table, hll.HLL())

	topic := fmt.Sprintf("murmur-e2e-hll-%d", time.Now().UnixNano())
	consumerGroup := topic + "-cg"
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	// Distribution: page-A gets 500 unique visitors with each visitor making 2 visits;
	// page-B gets 100 unique visitors with each visitor making 5 visits. Total 1500
	// records but only 600 unique users across the two pages (no overlap).
	uniques := map[string]int{"page-A": 500, "page-B": 100}
	visitsEach := map[string]int{"page-A": 2, "page-B": 5}
	for page, n := range uniques {
		for u := 0; u < n; u++ {
			user := fmt.Sprintf("%s-user-%d", page, u)
			for v := 0; v < visitsEach[page]; v++ {
				body, _ := json.Marshal(pageView{PageID: page, UserID: user})
				producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: body})
			}
		}
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("producer flush: %v", err)
	}

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

	pipe := pipeline.NewPipeline[pageView, []byte]("e2e_hll_uniques").
		From(src).
		Key(func(e pageView) string { return e.PageID }).
		Value(func(e pageView) []byte { return hll.Single([]byte(e.UserID)) }).
		Aggregate(hll.HLL()).
		StoreIn(store)

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- streaming.Run(runCtx, pipe)
	}()

	// Wait for processing to converge, then verify within HLL error bounds.
	deadline := time.Now().Add(60 * time.Second)
	tol := 0.05 // 5% — comfortably wide for HLL++ p=14 (~1.6% std error)
	for {
		if time.Now().After(deadline) {
			break
		}
		ok := true
		for page, expected := range uniques {
			b, found, err := store.Get(ctx, state.Key{Entity: page})
			if err != nil {
				t.Fatalf("Get %s: %v", page, err)
			}
			if !found {
				ok = false
				break
			}
			est, err := hll.Estimate(b)
			if err != nil {
				t.Fatalf("estimate %s: %v", page, err)
			}
			if math.Abs(float64(est)-float64(expected))/float64(expected) > tol {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	for page, expected := range uniques {
		b, found, err := store.Get(ctx, state.Key{Entity: page})
		if err != nil || !found {
			t.Fatalf("final Get %s: ok=%v err=%v", page, found, err)
		}
		est, _ := hll.Estimate(b)
		errPct := math.Abs(float64(est)-float64(expected)) / float64(expected) * 100
		t.Logf("%s: estimated %d unique, expected %d (error %.2f%%)", page, est, expected, errPct)
		if math.Abs(float64(est)-float64(expected))/float64(expected) > tol {
			t.Errorf("%s: estimate %d outside %g tolerance of %d", page, est, tol, expected)
		}
	}

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Errorf("streaming runtime did not shut down within 10s")
	}
}
