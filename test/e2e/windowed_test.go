// End-to-end smoke test for a windowed counter pipeline:
//
//	Kafka topic (with explicit event-time per record) →
//	  Sum monoid wrapped in WindowDaily →
//	  DynamoDB Int64SumStore (per-bucket rows) →
//	  query.GetWindow / query.GetRange merging
//
// Skipped unless KAFKA_BROKERS and DDB_LOCAL_ENDPOINT are set.
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
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/query"
	mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
	mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

func TestE2E_WindowedCounterPipeline(t *testing.T) {
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
	table := fmt.Sprintf("murmur_e2e_window_%d", time.Now().UnixNano())
	if err := mddb.CreateInt64Table(ctx, ddbClient, table); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &awsddb.DeleteTableInput{TableName: &table})
	})
	store := mddb.NewInt64SumStore(ddbClient, table)

	topic := fmt.Sprintf("murmur-e2e-window-%d", time.Now().UnixNano())
	consumerGroup := topic + "-cg"

	// Anchor "today" at a fixed time for deterministic bucket assignment.
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	// Produce events with explicit event-times spanning the last 10 days for "page-A".
	// Day 0 (today) gets 10 events, day 1 (yesterday) gets 9, ..., day 9 gets 1.
	// Total events: 10 + 9 + ... + 1 = 55.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	dailyCounts := map[int]int64{} // days-ago → count
	for daysAgo := 0; daysAgo < 10; daysAgo++ {
		count := int64(10 - daysAgo)
		dailyCounts[daysAgo] = count
		eventTime := now.Add(-time.Duration(daysAgo) * 24 * time.Hour)
		for i := int64(0); i < count; i++ {
			body, _ := json.Marshal(pageView{PageID: "page-A", UserID: fmt.Sprintf("u-%d-%d", daysAgo, i)})
			producer.ProduceSync(ctx, &kgo.Record{
				Topic:     topic,
				Value:     body,
				Timestamp: eventTime,
			})
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

	w := windowed.Daily(90 * 24 * time.Hour)
	pipe := pipeline.NewPipeline[pageView, int64]("e2e_windowed_counter").
		From(src).
		Key(func(e pageView) string { return e.PageID }).
		Value(func(pageView) int64 { return 1 }).
		Aggregate(core.Sum[int64](), w).
		StoreIn(store)

	runCtx, runCancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- streaming.Run(runCtx, pipe)
	}()

	// Wait for processing to converge — for windowed pipelines, "converged" means the
	// total of all 10 daily buckets equals 55.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if time.Now().After(deadline) {
			break
		}
		got, err := query.GetWindow(ctx, store, core.Sum[int64](), w, "page-A", 30*24*time.Hour, now)
		if err == nil && got == 55 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Verify per-window sums.
	cases := []struct {
		name     string
		duration time.Duration
		want     int64
	}{
		{"Last1Day", 1 * 24 * time.Hour, 10},                  // today only
		{"Last2Days", 2 * 24 * time.Hour, 10 + 9},             // 19
		{"Last3Days", 3 * 24 * time.Hour, 10 + 9 + 8},         // 27
		{"Last7Days", 7 * 24 * time.Hour, 10 + 9 + 8 + 7 + 6 + 5 + 4}, // 49
		{"Last10Days", 10 * 24 * time.Hour, 55},               // all
		{"Last30Days", 30 * 24 * time.Hour, 55},               // all (still 55, no older data)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := query.GetWindow(ctx, store, core.Sum[int64](), w, "page-A", tc.duration, now)
			if err != nil {
				t.Fatalf("GetWindow %s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Errorf("GetWindow %s: got %d, want %d", tc.name, got, tc.want)
			}
		})
	}

	// Bounded range query: days 3-5 ago = 7 + 6 + 5 = 18.
	t.Run("RangeDays3To5", func(t *testing.T) {
		start := now.Add(-5 * 24 * time.Hour)
		end := now.Add(-3 * 24 * time.Hour)
		got, err := query.GetRange(ctx, store, core.Sum[int64](), w, "page-A", start, end)
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		if got != 18 {
			t.Errorf("GetRange days 3-5: got %d, want 18", got)
		}
	})

	runCancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Errorf("streaming runtime did not shut down within 10s")
	}
}
